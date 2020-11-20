from django.template import Context, Template
from django.test import TestCase
from django.test.client import RequestFactory
from django.urls import reverse


class PageQuerystringTests(TestCase):

    def setUp(self):
        self.factory = RequestFactory()
        self.url = reverse("vgl:search")
        self.expected_html = "q=fantasy&page=7"

    def render_tag(self, page_number, **request_kwargs):
        request = self.factory.get(self.url, {"q": "fantasy", **request_kwargs})
        context = Context()
        context.request = request
        template = Template("{% load vgl_tags %}"
                            "{% page_querystring " + str(page_number) + " %}")
        return template.render(context)

    def test_no_page_arg_in_request(self):
        """
        If there is no page arg in the request, tag adds one.
        """
        rendered_template = self.render_tag(7)
        self.assertHTMLEqual(self.expected_html, rendered_template)

    def test_existing_page_arg_in_request(self):
        """
        If there is a page arg in the request, tag replaces it.
        """
        rendered_template = self.render_tag(7, page=1)
        self.assertHTMLEqual(self.expected_html, rendered_template)

    def test_several_page_args_in_request(self):
        """
        If there are several page args, tag replace them by a single value.
        """
        rendered_template = self.render_tag(7, page=[1, 2])
        self.assertHTMLEqual(self.expected_html, rendered_template)
