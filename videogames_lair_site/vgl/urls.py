from django.urls import path, reverse_lazy
from django.views.generic import TemplateView

from . import views

app_name = 'vgl'
urlpatterns = [
    path('', TemplateView.as_view(template_name="vgl/index.html"), name='index'),
    path('search', views.SearchResultsView.as_view(), name='search')
]
