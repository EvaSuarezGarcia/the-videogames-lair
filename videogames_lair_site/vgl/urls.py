from django.urls import path, reverse_lazy

from . import views

app_name = 'vgl'
urlpatterns = [
    path('', views.index, name='index')
]
