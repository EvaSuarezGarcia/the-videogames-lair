from django.contrib.auth.forms import UserCreationForm as DjangoUserCreationForm, UsernameField
from django.forms import EmailField

from .models import User


class MyUserCreationForm(DjangoUserCreationForm):

    class Meta(DjangoUserCreationForm):
        model = User
        fields = ("username", "email")
        field_classes = {"username": UsernameField, "email": EmailField}
