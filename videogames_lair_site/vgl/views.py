from django.shortcuts import render, get_object_or_404, get_list_or_404
from django.http import HttpResponse
from django.utils import timezone
from django.views.generic import ListView
from elasticsearch_dsl import connections
from elasticsearch_dsl.query import MultiMatch
from vgl.documents import Game
from videogames_lair_site import settings


connections.create_connection(hosts=settings.ES_HOST)


def index(request):
    return render(request, "vgl/index.html")


def search(request):
    return render(request, "vgl/search.html")


class SearchResultsView(ListView):
    template_name = "vgl/search.html"
    context_object_name = "results_list"
    paginate_by = 10
    total_results = 100

    def get_queryset(self):
        query_text = self.request.GET.get("q")
        if not query_text:
            return []
        query = MultiMatch(query=query_text,
                           fields=["name^3", "aliases^3", "deck^2", "description", "concepts", "genres", "themes"],
                           type="cross_fields", tie_breaker=0.3)
        return Game.search().query(query)[:self.total_results].execute()

    def get_context_data(self, *, object_list=None, **kwargs):
        context = {
            "query": self.request.GET.get("q")
        }
        return super().get_context_data(**context)
