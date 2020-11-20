from django.views.generic import ListView

from elasticsearch_dsl import connections
from elasticsearch_dsl.query import MultiMatch

from videogames_lair_site import settings
from vgl.documents import Game
from vgl.forms import SearchForm

from typing import Dict, List


connections.create_connection(hosts=settings.ES_HOST)


class SearchResultsView(ListView):
    template_name = "vgl/search.html"
    context_object_name = "results_list"
    paginate_by = 10
    total_results = 100
    normal_filters_names = ["genres", "themes", "franchises", "platforms", "developers", "publishers", "age_ratings"]

    def setup(self, request, *args, **kwargs):
        super(SearchResultsView, self).setup(request, *args, **kwargs)
        self.form = SearchForm(self.request.GET)

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

        search = search.query(query)[:self.total_results]

        return search.execute()

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super().get_context_data()
        context["form"] = self.form

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

        return context
