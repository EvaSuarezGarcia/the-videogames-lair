from django import template

register = template.Library()


@register.simple_tag(takes_context=True)
def page_querystring(context, page: int, page_param: str = "page"):
    new_request_params = context.request.GET.copy()
    new_request_params[page_param] = page

    return new_request_params.urlencode()
