from django.apps import apps
from django.urls import reverse
from django.utils.http import urlencode


def reverse_querystring(view, urlconf=None, args=None, kwargs=None, current_app=None, query_kwargs=None):
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


def get_model(model_name: str):
    return apps.get_model(app_label="vgl", model_name=model_name)