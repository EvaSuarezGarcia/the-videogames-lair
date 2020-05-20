from django.urls import path, reverse_lazy
from django.views.generic import CreateView
from django.contrib.auth import views as auth_views

from . import views
from .forms import MyUserCreationForm

app_name = 'vgl'
urlpatterns = [
    path('', views.index, name='index'),
    path('login', auth_views.LoginView.as_view(template_name='vgl/login.html'), name='login'),
    path('logout', auth_views.LogoutView.as_view(), name='logout'),
    path('signup', CreateView.as_view(template_name='vgl/signup.html', form_class=MyUserCreationForm,
                                      success_url=reverse_lazy('vgl:login')), name='signup')
]
