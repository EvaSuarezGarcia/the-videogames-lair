from django.http import QueryDict
from django.test import TestCase
from vgl.forms import SearchForm


class SearchFormYearsValidationTests(TestCase):

    def test_correct_data(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "1990;2000"})
        form = SearchForm(data)
        self.assertTrue(form.is_valid())

    def test_no_data(self):
        data = QueryDict("")
        form = SearchForm(data)
        self.assertTrue(form.is_valid())

    def test_same_two_years(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "2000;2000"})
        form = SearchForm(data)
        self.assertTrue(form.is_valid())

    def test_less_than_two_years(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "1990"})
        form = SearchForm(data)
        self.assertFalse(form.is_valid())

    def test_more_than_two_years(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "1990;1991;1993"})
        form = SearchForm(data)
        self.assertFalse(form.is_valid())

    def test_year_from_gt_year_to(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "2000;1999"})
        form = SearchForm(data)
        self.assertFalse(form.is_valid())

    def test_not_ints(self):
        data = QueryDict("", mutable=True)
        data.update({"years": "foo;bar"})
        form = SearchForm(data)
        self.assertFalse(form.is_valid())
