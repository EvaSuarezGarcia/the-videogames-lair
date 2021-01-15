from django.urls import path, reverse_lazy
from django.views.generic import TemplateView

from vgl.views import autocomplete as ac
from vgl.views.game_lists import SearchResultsView, RecommendationsView

app_name = "vgl"
urlpatterns = [
    path("", TemplateView.as_view(template_name="vgl/index.html"), name="index"),
    path("search/", SearchResultsView.as_view(), name="search"),
    path("recommendations/", RecommendationsView.as_view(), name="recommendations"),
    path("autocomplete-genre/", ac.autocomplete_genre, name="autocomplete_genre"),
    path("autocomplete-company/", ac.autocomplete_company, name="autocomplete_company"),
    path("autocomplete-franchise/", ac.autocomplete_franchise, name="autocomplete_franchise"),
    path("autocomplete-theme/", ac.autocomplete_theme, name="autocomplete_theme"),
    path("autocomplete-platform/", ac.autocomplete_platform, name="autocomplete_platform")
]
