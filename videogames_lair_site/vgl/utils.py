from django.utils.http import urlencode
from django.urls import reverse


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
        return '{}?{}'.format(base_url, urlencode(query_kwargs))
    return base_url
