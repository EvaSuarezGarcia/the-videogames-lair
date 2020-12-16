from typing import Dict, List

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from django.views.generic import ListView
from elasticsearch_dsl import connections
from elasticsearch_dsl.query import MultiMatch

from vgl.documents import Game
from vgl.forms import SearchForm
from vgl.models import AgeRating, Platform

from videogames_lair_site import settings


connections.create_connection(hosts=settings.ES_HOST)


class GameListView(ListView):
    context_object_name = "game_list"
    paginate_by = 10
    max_results = 100
    only_filters = False

    def setup(self, request, *args, **kwargs):
        super().setup(request, *args, **kwargs)
        self.form = SearchForm(self.request.GET)

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super().get_context_data(object_list=object_list, **kwargs)
        context["form"] = self.form
        context["only_filters"] = self.only_filters

        # Set up pagination
        paginator = context["paginator"]
        try:
            num_page = int(self.request.GET.get("page", "1"))
        except ValueError:
            num_page = 1

        index = num_page - 1
        max_index = paginator.num_pages - 1
        start_index = max(0, index - 1 if index != max_index else index - 2)
        end_index = min(max_index + 1, index + 2 if index != 0 else index + 3)
        page_range = list(paginator.page_range)[start_index:end_index]

        if 1 not in page_range:
            page_range.insert(0, "...")
            page_range.insert(0, 1)

        if paginator.num_pages not in page_range:
            page_range.extend(["...", paginator.num_pages])

        context["page_range"] = page_range

        # Set filters
        context["open_filters"] = self.request.GET.get("open", False)

        # Set age ratings filter
        age_ratings = {}
        for age_rating in AgeRating.objects.all().order_by("name"):
            system, rating = age_rating.name.split(": ")
            age_ratings.setdefault(system, set())
            age_ratings[system].add(rating)

        sorted_age_ratings = {}
        for system, age_rating_set in age_ratings.items():
            sorted_age_ratings[system] = sorted(age_rating_set, key=lambda x: (len(x), x.lower()))

        context["age_ratings"] = sorted_age_ratings

        if self.form.is_valid():
            # Set platform filters (special case b/c shown value != value used to filter)
            platforms = self.form.cleaned_data.get("platforms")
            context["platform_filters"] = list(Platform.objects.filter(name__in=platforms).values("name", "abbreviation"))

            # Set year filters
            years = self.form.cleaned_data.get("years")
            if years:
                context["year_from"] = years.split(";")[0]
                context["year_to"] = years.split(";")[1]

        return context


class SearchResultsView(GameListView):
    template_name = "vgl/search.html"
    normal_filters_names = ["genres", "themes", "franchises", "platforms", "developers", "publishers", "age_ratings"]
    only_filters = False

    def _fill_filter(self, field: str, filters: Dict[str, List[str]]):
        values_to_filter = self.form.cleaned_data.get(field)

        if values_to_filter:
            filters[field] = values_to_filter

    def _fill_filters(self, fields: List[str], filters: Dict[str, List[str]]):
        for field in fields:
            self._fill_filter(field, filters)

    def get_queryset(self):
        query_text = None
        filters = {}
        year_range = []
        search = Game.search()

        if self.form.is_valid():
            query_text = self.form.cleaned_data.get("q")
            self._fill_filters(self.normal_filters_names, filters)
            form_year_range = self.form.cleaned_data.get("years")
            if form_year_range:
                year_range = form_year_range.split(";")

        for filter_name, filter_values in filters.items():
            for filter_value in filter_values:
                search = search.filter("term", **{filter_name: filter_value})

        if year_range:
            search = search.filter("range",
                                   release_date={"gte": f"{year_range[0]}-01-01", "lte": f"{year_range[1]}-12-31"})

        if not query_text:
            return []
        query = MultiMatch(query=query_text,
                           fields=["name^3", "aliases^3", "deck^2", "description", "concepts", "genres", "themes"],
                           type="cross_fields", tie_breaker=0.3)

        search = search.query(query)[:self.max_results]

        return search.execute()


class RecommendationsView(GameListView):
    template_name = "vgl/recommendations.html"
    only_filters = True

    def get_queryset(self):
        auth_provider = PlainTextAuthProvider(
            username=settings.CASSANDRA_USER,
            password=settings.CASSANDRA_PASSWORD
        )

        with Cluster(settings.CASSANDRA_HOST, auth_provider=auth_provider) as cluster:
            with cluster.connect(keyspace=settings.CASSANDRA_KEYSPACE) as session:
                recommendations = session.execute(f"SELECT * FROM recommendations "
                                                  f"WHERE user_id = {self.request.user.als_user_id} "
                                                  f"ORDER BY rank")

        # Get recommended games data from ES
        game_ids = [recommendation.game_id for recommendation in recommendations]
        search = Game.search().filter("terms", vgl_id=game_ids)[:self.max_results]
        games = list(search.execute())

        # Sort games so that they match the ranking from Cassandra
        games_dict = {game_id: position for position, game_id in enumerate(game_ids)}

        results = [None] * len(games)
        for game in games:
            results[games_dict[game.vgl_id]] = game
        # Remove empty results, if any
        results = [game for game in results if game is not None]

        return results
