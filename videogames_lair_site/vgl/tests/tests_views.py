from django.test import SimpleTestCase

from vgl.utils import reverse_querystring


class SearchViewTests(SimpleTestCase):

    SEARCH_URL_NAME = 'vgl:search'

    def test_search(self):
        """
        A normal search returns a page with results, and the original query in the context. Results are rendered
        in an element with ID "results-list".
        """
        query_text = "final fantasy"
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME, query_kwargs={"q": query_text}))
        self.assertEqual(len(response.context["results_list"]), 10)
        self.assertEqual(response.context["query"], query_text)
        self.assertContains(response, "id=\"results-list\"")

    def test_search_no_results(self):
        """
        A search with no results returns a page with an appropriate message. Message is within element with
        ID "no-results".
        """
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME, query_kwargs={"q": "testest"}))
        self.assertContains(response, "id=\"no-results\"")

    def test_search_no_query(self):
        """
        A search with no query returns a page with just a search form.
        """
        response = self.client.get(reverse_querystring(self.SEARCH_URL_NAME))
        self.assertNotContains(response, "id=\"results-list\"")
        self.assertNotContains(response, "id=\"no-results\"")
