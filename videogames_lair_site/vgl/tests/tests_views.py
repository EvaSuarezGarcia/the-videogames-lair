from collections import namedtuple
from unittest import mock

from django.db.models import Model
from django.test import TestCase
from django.template.defaultfilters import date
from django.urls import reverse

from model_bakery import baker
from model_bakery.recipe import seq

from vgl import models
from vgl.cassandra import CassandraConnectionManager
from vgl.utils import reverse_querystring
from vgl.views.game_lists import SearchResultsView, RecommendationsView

from typing import List, Type


class SearchViewTests(TestCase):

    SEARCH_URL_NAME = 'vgl:search'
    NUM_PAGES = int(SearchResultsView.max_results / SearchResultsView.paginate_by)

    def do_normal_search(self, **additional_query_kwargs):
        return self.client.get(reverse_querystring(self.SEARCH_URL_NAME,
                                                   query_kwargs={"q": "final fantasy", **additional_query_kwargs}))

    def test_search(self):
        """
        A normal search returns a page with results. Results are rendered in an element with ID "results-list".
        """
        response = self.do_normal_search()
        self.assertGreaterEqual(len(response.context["game_list"]), 1)
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


class SearchViewFiltersTests(TestCase):

    SEARCH_URL_NAME = 'vgl:search'

    def do_normal_search(self, **additional_query_kwargs):
        return self.client.get(reverse_querystring(self.SEARCH_URL_NAME,
                                                   query_kwargs={"q": "final fantasy", **additional_query_kwargs}))

    def check_filter(self, filter_name: str, filter_values: List[str], doc_field: str = ""):
        if not doc_field:
            doc_field = filter_name + "s"
        response = self.do_normal_search(**{filter_name: filter_values})
        results = response.context["game_list"]
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

    def test_age_ratings_filter(self):
        self.check_filter("ar", ["PEGI: 3+"], doc_field="age_ratings")

    def test_release_year_filter(self):
        year_from = 1990
        year_to = 2000
        response = self.do_normal_search(years=f"{year_from};{year_to}")
        results = response.context["game_list"]
        self.assertGreater(len(results), 0)

        for i, result in enumerate(results):
            release_year = int(date(result["release_date"], "Y"))
            self.assertGreaterEqual(release_year, year_from, msg=f"Failed for result {i}: Game year ({release_year}) "
                                                                 f"is less than filter from-year ({year_from})")
            self.assertLessEqual(release_year, year_to, msg=f"Failed for result {i}: Game year ({release_year}) "
                                                            f"is greater than filter to-year ({year_to})")


class AutocompleteTests(TestCase):

    query = "foo"

    def check_autocomplete(self, model: Type[Model], url_name: str):
        baker.make(model, name=seq(self.query), _quantity=3)
        baker.make(model, name=seq("bar"), _quantity=2)

        response = self.client.get(reverse_querystring(url_name, query_kwargs={"q": self.query})).json()
        self.assertEqual(len(response), 3)
        self.assertTrue(all(map(lambda x: x.startswith(self.query), response)))

    def test_autocomplete_genre(self):
        self.check_autocomplete(models.Genre, "vgl:autocomplete_genre")

    def test_autocomplete_company(self):
        self.check_autocomplete(models.Company, "vgl:autocomplete_company")

    def test_autocomplete_franchise(self):
        self.check_autocomplete(models.Franchise, "vgl:autocomplete_franchise")

    def test_autocomplete_theme(self):
        self.check_autocomplete(models.Theme, "vgl:autocomplete_theme")

    def test_autocomplete_platform(self):
        baker.make(models.Platform, name=seq(self.query), abbreviation=seq(f"not {self.query}"), _quantity=3)
        baker.make(models.Platform, name=seq(f"not {self.query}"), abbreviation=seq(self.query), _quantity=2)
        baker.make(models.Platform, name=seq("bar"), abbreviation=seq("bar"))

        response = self.client.get(reverse_querystring("vgl:autocomplete_platform", query_kwargs={"q": self.query}))\
            .json()

        self.assertEqual(len(response), 5)
        self.assertTrue(all(map(lambda x: len(x.split(" (")) == 2, response)))
        self.assertTrue(all(map(lambda x: x.split(" ")[0].startswith(self.query)
                                          or x.split(" ")[1].startswith(self.query), response)))


class RecommendationsViewTests(TestCase):

    recommendation = namedtuple("recommendation", "game_id")
    RECOMMENDATIONS_URL = reverse("vgl:recommendations")
    user = None
    sample_game_ids = [6, 18, 23, 34, 37, 52]  # These games exist in ES
    sample_recommendations = [recommendation(game_id=61), recommendation(game_id=65),
                              recommendation(game_id=68), recommendation(game_id=82)]  # These games also exist

    @classmethod
    def setUpTestData(cls):
        cls.user = baker.make(models.User)
        baker.make(models.GameStats, game_id=iter(cls.sample_game_ids), _quantity=len(cls.sample_game_ids))
        
    @mock.patch.object(CassandraConnectionManager, "get_recommendations_for_user", return_value=sample_recommendations)
    def test_custom_recommendations(self, mock_cassandra_recommendations):
        self.client.force_login(self.user)
        response = self.client.get(self.RECOMMENDATIONS_URL)

        most_popular_games = models.GameStats.objects.order_by("-popularity")[:RecommendationsView.max_results]
        most_popular_games = [game.game_id for game in most_popular_games]
        recommended_games = [game.vgl_id for game in response.context["game_list"]]
        self.assertGreater(len(recommended_games), 0)
        self.assertNotEqual(most_popular_games, recommended_games)

    def test_popular_recommendations(self):
        self.client.force_login(self.user)
        response = self.client.get(self.RECOMMENDATIONS_URL)

        self.assertFalse(response.context["has_custom_recommendations"])

        most_popular_games = models.GameStats.objects.order_by("-popularity")[:RecommendationsView.max_results]
        most_popular_games = [game.game_id for game in most_popular_games]
        recommended_games = [game.vgl_id for game in response.context["game_list"]]
        self.assertGreater(len(recommended_games), 0)
        self.assertEqual(most_popular_games, recommended_games)
