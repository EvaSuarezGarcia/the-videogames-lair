from django.urls import path, reverse_lazy

from . import views

app_name = 'vgl'
urlpatterns = [
    path('', views.index, name='index'),
    path('search', views.SearchResultsView.as_view(), name='search'),
    path('test', views.test)
]
