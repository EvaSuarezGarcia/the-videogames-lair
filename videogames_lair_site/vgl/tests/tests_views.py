from django.test import SimpleTestCase

from vgl.utils import reverse_querystring


class SearchViewTests(SimpleTestCase):

    SEARCH_URL_NAME = 'vgl:search'

    def test_search(self):
        """
        A normal search returns a page with results, and the original query in the context.
        """
        query_text = "final fantasy"
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME, query_kwargs={"q": query_text}))
        self.assertEqual(len(response.context["results_list"]), 10)
        self.assertEqual(response.context["query"], query_text)

    def test_search_no_results(self):
        """
        A search with no results returns a page with an appropriate message.
        """
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME, query_kwargs={"q": "testest"}))
        self.assertContains(response, "No results")

    def test_search_no_query(self):
        """
        A search with no query returns the same as a search with no results.
        """
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME))
        self.assertContains(response, "No results")
