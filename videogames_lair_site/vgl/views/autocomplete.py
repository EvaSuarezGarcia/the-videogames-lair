from django.db.models import Q
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from vgl.models import GiantBombEntity, Platform
from vgl.utils import get_model

from typing import Type


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
