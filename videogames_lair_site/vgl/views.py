from django.shortcuts import render, get_object_or_404, get_list_or_404
from django.http import HttpResponse
from django.utils import timezone
from django.views.generic import ListView
from elasticsearch_dsl import connections
from elasticsearch_dsl.query import MultiMatch
from vgl.documents import Game
from videogames_lair_site import settings


connections.create_connection(hosts=settings.ES_HOST)


class SearchResultsView(ListView):
    template_name = "vgl/search.html"
    context_object_name = "results_list"
    paginate_by = 10
    total_results = 100

    def get_queryset(self):
        query_text = self.request.GET.get("q", "").strip()
        if not query_text:
            return []
        query = MultiMatch(query=query_text,
                           fields=["name^3", "aliases^3", "deck^2", "description", "concepts", "genres", "themes"],
                           type="cross_fields", tie_breaker=0.3)
        return Game.search().query(query)[:self.total_results].execute()

    def get_context_data(self, *, object_list=None, **kwargs):
        context = {
            "query": self.request.GET.get("q", "").strip()
        }

        context.update(super().get_context_data(**context))

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
