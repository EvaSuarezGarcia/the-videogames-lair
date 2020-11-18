from django.test import SimpleTestCase

from vgl.views.SearchResultsView import SearchResultsView
from vgl.utils import reverse_querystring

from typing import List


class SearchViewTests(SimpleTestCase):

    SEARCH_URL_NAME = 'vgl:search'
    NUM_PAGES = int(SearchResultsView.total_results / SearchResultsView.paginate_by)

    def do_normal_search(self, **additional_query_kwargs):
        return self.client.get(reverse_querystring(self.SEARCH_URL_NAME,
                                                   query_kwargs={"q": "final fantasy", **additional_query_kwargs}))

    def test_search(self):
        """
        A normal search returns a page with results. Results are rendered in an element with ID "results-list".
        """
        response = self.do_normal_search()
        self.assertGreaterEqual(len(response.context["results_list"]), 1)
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

    # --- Pagination tests ---
    def test_first_page(self):
        response = self.do_normal_search()
        pages = response.context["page_range"]
        expected_pages = [1, 2, 3, "...", self.NUM_PAGES]
        self.assertEqual(pages, expected_pages)

    def test_second_page(self):
        response = self.do_normal_search(page=2)
        pages = response.context["page_range"]
        expected_pages = [1, 2, 3, "...", self.NUM_PAGES]
        self.assertEqual(pages, expected_pages)

    def test_last_page(self):
        response = self.do_normal_search(page=self.NUM_PAGES)
        pages = response.context["page_range"]
        expected_pages = [1, "...", self.NUM_PAGES-2, self.NUM_PAGES-1, self.NUM_PAGES]
        self.assertEqual(pages, expected_pages)

    def test_second_to_last_page(self):
        response = self.do_normal_search(page=self.NUM_PAGES-1)
        pages = response.context["page_range"]
        expected_pages = [1, "...", self.NUM_PAGES-2, self.NUM_PAGES-1, self.NUM_PAGES]
        self.assertEqual(pages, expected_pages)

    def test_middle_page(self):
        page = int(self.NUM_PAGES/2)
        response = self.do_normal_search(page=page)
        pages = response.context["page_range"]
        expected_pages = [1, "...", page-1, page, page+1, "...", self.NUM_PAGES]
        self.assertEqual(pages, expected_pages)


class SearchViewFiltersTests(SimpleTestCase):

    SEARCH_URL_NAME = 'vgl:search'

    def do_normal_search(self, **additional_query_kwargs):
        return self.client.get(reverse_querystring(self.SEARCH_URL_NAME,
                                                   query_kwargs={"q": "final fantasy", **additional_query_kwargs}))

    def check_filter(self, filter_name: str, filter_values: List[str], doc_field: str = ""):
        if not doc_field:
            doc_field = filter_name + "s"
        response = self.do_normal_search(**{filter_name: filter_values})
        results = response.context["results_list"]
        self.assertGreater(len(results), 0)

        for i, result in enumerate(results):
            values_in_result = getattr(result, doc_field)
            values_in_both = set(filter_values).intersection(values_in_result)
            self.assertEqual(len(values_in_both), len(filter_values),
                             msg=f"Failed for result {i} (filters: {filter_values}; {doc_field}: {values_in_result}")

    def test_genres_filter(self):
        self.check_filter("genre", ["Role-Playing", "Fighting"])

    def test_themes_filter(self):
        self.check_filter("theme", ["Sci-Fi"])

    def test_franchises_filter(self):
        self.check_filter("fran", ["Final Fantasy"], doc_field="franchises")

    def test_platforms_filter(self):
        self.check_filter("plat", ["Game Boy Advance", "PlayStation"], doc_field="platforms")

    def test_developers_filter(self):
        self.check_filter("dev", ["Square Enix", "Squaresoft"], doc_field="developers")

    def test_publishers_filter(self):
        self.check_filter("pub", ["Nintendo"], doc_field="publishers")
