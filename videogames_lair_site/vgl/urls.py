from django.urls import path
from django.views.generic import TemplateView

from vgl.views import autocomplete as ac
from vgl.views.game_lists import SearchResultsView, RecommendationsView, RatingsView, rate_game, GameDetail

app_name = "vgl"
urlpatterns = [
    path("", TemplateView.as_view(template_name="vgl/index.html"), name="index"),
    path("search/", SearchResultsView.as_view(), name="search"),
    path("recommendations/", RecommendationsView.as_view(), name="recommendations"),
    path("ratings/", RatingsView.as_view(), name="ratings"),
    path("game/<int:game_id>/", GameDetail.as_view(), name="game_detail"),
    path("rate-game/", rate_game, name="rate_game"),
    path("autocomplete-genre/", ac.autocomplete_genre, name="autocomplete_genre"),
    path("autocomplete-company/", ac.autocomplete_company, name="autocomplete_company"),
    path("autocomplete-franchise/", ac.autocomplete_franchise, name="autocomplete_franchise"),
    path("autocomplete-theme/", ac.autocomplete_theme, name="autocomplete_theme"),
    path("autocomplete-platform/", ac.autocomplete_platform, name="autocomplete_platform")
]
