from django.apps import apps
from django.db.models import Model
from django.urls import reverse
from django.utils.http import urlencode

from typing import Type, List

from vgl.documents import Game
from vgl.models import GameStats


def reverse_querystring(view, urlconf=None, args=None, kwargs=None, current_app=None, query_kwargs=None) -> str:
    """
    Custom reverse to handle query strings.

    :param view: Name of the view.
    :param urlconf: reverse param.
    :param args: reverse param.
    :param kwargs: reverse param.
    :param current_app: reverse param.
    :param query_kwargs: Dict of query string arguments.
    :return:
    """
    base_url = reverse(view, urlconf=urlconf, args=args, kwargs=kwargs, current_app=current_app)
    if query_kwargs:
        return '{}?{}'.format(base_url, urlencode(query_kwargs, doseq=True))
    return base_url


def get_model(model_name: str) -> Type[Model]:
    return apps.get_model(app_label="vgl", model_name=model_name)


def add_stats_to_games(games: List[Game]) -> None:
    for game in games:
        stats = {}
        try:
            stats = GameStats.objects.get(game_id=game.vgl_id)
        except GameStats.DoesNotExist:
            pass
        game.stats = stats
