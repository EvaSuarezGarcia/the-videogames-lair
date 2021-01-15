from django import template
from django.urls import resolve

register = template.Library()


@register.simple_tag(takes_context=True)
def page_querystring(context, page: int, page_param: str = "page"):
    new_request_params = context.request.GET.copy()
    new_request_params[page_param] = page

    return new_request_params.urlencode()


@register.inclusion_tag("vgl/advanced_filter.html")
def advanced_filter(name: str, visible_value: str, hidden_value: str = None):
    if not hidden_value:
        hidden_value = visible_value

    return {
        "name": name,
        "visible_value": visible_value,
        "hidden_value": hidden_value
    }


@register.simple_tag()
def resolve_url_name(url: str) -> str:
    return "vgl:" + resolve(url).url_name
