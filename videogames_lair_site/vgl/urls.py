from django.urls import path, reverse_lazy
from django.views.generic import TemplateView

from . import views

app_name = "vgl"
urlpatterns = [
    path("", TemplateView.as_view(template_name="vgl/index.html"), name="index"),
    path("search/", views.SearchResultsView.as_view(), name="search"),
    path("autocomplete-genre/", views.autocomplete_genre, name="autocomplete_genre"),
    path("autocomplete-company/", views.autocomplete_company, name="autocomplete_company"),
    path("autocomplete-franchise/", views.autocomplete_franchise, name="autocomplete_franchise"),
    path("autocomplete-theme/", views.autocomplete_theme, name="autocomplete_theme"),
    path("autocomplete-platform/", views.autocomplete_platform, name="autocomplete_platform")
]
