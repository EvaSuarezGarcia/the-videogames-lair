from typing import Dict, List

from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import ListView
from copy import deepcopy
from elasticsearch_dsl import connections, Search
from elasticsearch_dsl.query import MultiMatch

from vgl import utils
from vgl.cassandra import CassandraConnectionManager
from vgl.documents import Game
from vgl.forms import SearchForm
from vgl.models import AgeRating, Platform, GameStats

from videogames_lair_site import settings


connections.create_connection(hosts=settings.ES_HOST)


class GameListView(ListView):
    context_object_name = "game_list"
    paginate_by = 10
    max_results = 100
    normal_filters_names = ["genres", "themes", "franchises", "platforms", "developers", "publishers", "age_ratings"]
    only_filters = False
    form = None

    def setup(self, request, *args, **kwargs):
        super().setup(request, *args, **kwargs)
        GameListView.form = SearchForm(self.request.GET)

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

    def _get_filters(self) -> Dict[str, List[str]]:
        filters = {}
        for field in self.normal_filters_names:
            values_to_filter = self.form.cleaned_data.get(field)
            if values_to_filter:
                filters[field] = values_to_filter

        return filters

    def apply_filters_to_search(self, search: Search) -> Search:
        filters = {}
        year_range = []
        new_search = deepcopy(search)

        if self.form.is_valid():
            filters = self._get_filters()
            form_year_range = self.form.cleaned_data.get("years")
            if form_year_range:
                year_range = form_year_range.split(";")

        for filter_name, filter_values in filters.items():
            for filter_value in filter_values:
                new_search = new_search.filter("term", **{filter_name: filter_value})

        if year_range:
            new_search = new_search.filter("range", release_date={"gte": f"{year_range[0]}-01-01",
                                                                  "lte": f"{year_range[1]}-12-31"})

        return new_search

    def search_by_game_ids(self, game_ids: List[int]) -> List[Game]:
        search = Game.search()
        search = self.apply_filters_to_search(search)
        search = search.filter("terms", vgl_id=game_ids)[:self.max_results]
        return list(search.execute())

    @staticmethod
    def _add_rating_to_games(ratings: List, games: List[Game]) -> None:
        ratings_dict = {rating.game_id: rating for rating in ratings}
        for game in games:
            if game.vgl_id in ratings_dict:
                game.user_rating = ratings_dict[game.vgl_id].rating
                game.estimated_rating = ratings_dict[game.vgl_id].estimated

    def get_ratings_and_add_to_games(self, games: List[Game]) -> None:
        with CassandraConnectionManager() as cassandra:
            ratings = cassandra.get_user_ratings(self.request.user.als_user_id)

        self._add_rating_to_games(ratings, games)


class SearchResultsView(GameListView):
    template_name = "vgl/search.html"
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
        search = Game.search()

        if self.form.is_valid():
            query_text = self.form.cleaned_data.get("q")

        search = self.apply_filters_to_search(search)

        if not query_text:
            return []
        query = MultiMatch(query=query_text,
                           fields=["name^3", "aliases^3", "deck^2", "description", "concepts", "genres", "themes"],
                           type="cross_fields", tie_breaker=0.3)

        search = search.query(query)[:self.max_results]

        games = list(search.execute())
        if self.request.user.is_authenticated:
            self.get_ratings_and_add_to_games(games)

        # Add stats to the games
        utils.add_stats_to_games(games)

        return games


class RecommendationsView(LoginRequiredMixin, GameListView):
    template_name = "vgl/recommendations.html"
    only_filters = True
    has_custom_recommendations = True

    def get_queryset(self):
        with CassandraConnectionManager() as cassandra:
            recommendations = cassandra.get_recommendations_for_user(self.request.user.als_user_id)

        # If there are no recommendations in Cassandra, suggest most popular games
        if not recommendations:
            self.has_custom_recommendations = False
            recommendations = list(GameStats.objects.order_by("-popularity")[:self.max_results])

        # Get recommended games data from ES
        game_ids = [recommendation.game_id for recommendation in recommendations]
        games = list(self.search_by_game_ids(game_ids))
        self.get_ratings_and_add_to_games(games)

        # Add stats to the games
        utils.add_stats_to_games(games)

        # Sort games so that they match the ranking from Cassandra
        games_dict = {game_id: position for position, game_id in enumerate(game_ids)}

        results = [None] * len(games_dict)
        for game in games:
            results[games_dict[game.vgl_id]] = game
        # Remove empty results, if any
        results = [game for game in results if game is not None]

        return results

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super().get_context_data(object_list=object_list, **kwargs)
        context["has_custom_recommendations"] = self.has_custom_recommendations

        return context


class RatingsView(LoginRequiredMixin, GameListView):
    template_name = "vgl/ratings.html"
    only_filters = True
    user_has_ratings = True

    def get_queryset(self):
        # Get this user's ratings from Cassandra
        with CassandraConnectionManager() as cassandra:
            ratings = cassandra.get_user_ratings(self.request.user.als_user_id)

        if ratings:
            # Search games in ES
            game_ids = [rating.game_id for rating in ratings]
            games = self.search_by_game_ids(game_ids)

            # Add stats to the games
            utils.add_stats_to_games(games)

            # Add user rating to the games
            self._add_rating_to_games(ratings, games)

        else:
            self.user_has_ratings = False
            games = []

        return games

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super().get_context_data(object_list=object_list, **kwargs)
        context['user_has_ratings'] = self.user_has_ratings
        return context
