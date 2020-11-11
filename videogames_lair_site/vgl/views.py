from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_GET
from django.views.generic import ListView

from elasticsearch_dsl import connections
from elasticsearch_dsl.query import MultiMatch

from videogames_lair_site import settings
from vgl.documents import Game
from vgl.forms import SearchForm
from vgl.models import GiantBombEntity, Platform
from vgl.utils import get_model

from typing import Type


connections.create_connection(hosts=settings.ES_HOST)


@require_GET
def autocomplete_genre(request):
    return _autocomplete_by_name(request, get_model("genre"))


@require_GET
def autocomplete_company(request):
    return _autocomplete_by_name(request, get_model("company"))


@require_GET
def autocomplete_franchise(request):
    return _autocomplete_by_name(request, get_model("franchise"))


@require_GET
def autocomplete_theme(request):
    return _autocomplete_by_name(request, get_model("theme"))


def _autocomplete_by_name(request, model: Type[GiantBombEntity]):
    q = request.GET.get("q")
    data = model.objects.filter(name__istartswith=q).values_list("name", flat=True)
    json = list(data)
    return JsonResponse(json, safe=False)


def autocomplete_platform(request):
    q = request.GET.get("q")
    data = Platform.objects.filter(Q(name__istartswith=q) | Q(abbreviation__istartswith=q))\
        .values_list("abbreviation", "name")
    json = list(map(lambda x: "{} ({})".format(x[0], x[1]), data))
    return JsonResponse(json, safe=False)


class SearchResultsView(ListView):
    template_name = "vgl/search.html"
    context_object_name = "results_list"
    paginate_by = 10
    total_results = 100

    def setup(self, request, *args, **kwargs):
        super(SearchResultsView, self).setup(request, *args, **kwargs)
        self.form = SearchForm(self.request.GET)

    def get_queryset(self):
        query_text = None
        if self.form.is_valid():
            query_text = self.form.cleaned_data.get("q")

        if not query_text:
            return []
        query = MultiMatch(query=query_text,
                           fields=["name^3", "aliases^3", "deck^2", "description", "concepts", "genres", "themes"],
                           type="cross_fields", tie_breaker=0.3)
        return Game.search().query(query)[:self.total_results].execute()

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
