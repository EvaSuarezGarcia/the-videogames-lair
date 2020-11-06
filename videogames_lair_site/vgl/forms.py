from django import forms
from multivaluefield import MultiValueField


class SearchForm(forms.Form):
    q = forms.CharField(required=False, label="Your keywords")
    genres = MultiValueField(forms.CharField(), "genre", required=False)
    themes = MultiValueField(forms.CharField(), "theme", required=False)
    franchises = MultiValueField(forms.CharField(), "fran", required=False)
    platforms = MultiValueField(forms.CharField(), "plat", required=False)
    developers = MultiValueField(forms.CharField(), "dev", required=False)
    publishers = MultiValueField(forms.CharField(), "pub", required=False)
    age_rating = MultiValueField(forms.CharField(), "ar", required=False)
    years = forms.CharField(required=False)
