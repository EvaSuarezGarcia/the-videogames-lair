from django import forms
from django.core.exceptions import ValidationError
from multivaluefield import MultiValueField


class SearchForm(forms.Form):
    q = forms.CharField(required=False, label="Your keywords")
    genres = MultiValueField(forms.CharField(), "genre", required=False)
    themes = MultiValueField(forms.CharField(), "theme", required=False)
    franchises = MultiValueField(forms.CharField(), "fran", required=False)
    platforms = MultiValueField(forms.CharField(), "plat", required=False)
    developers = MultiValueField(forms.CharField(), "dev", required=False)
    publishers = MultiValueField(forms.CharField(), "pub", required=False)
    age_ratings = MultiValueField(forms.CharField(), "ar", required=False)
    years = forms.CharField(required=False)

    def clean_years(self):
        data = self.cleaned_data["years"]
        if data:
            years = data.split(";")
            if len(years) != 2:
                raise ValidationError("There has to be two years separated by ;")

            try:
                year_from = int(years[0])
                year_to = int(years[1])

                if not year_from <= year_to:
                    raise ValidationError("First year must be less or equal than second year.")
            except ValueError:
                raise ValidationError("Years must be integers.")

        return data
