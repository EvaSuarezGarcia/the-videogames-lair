from django.db import models
from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
    # Remove unneeded fields
    first_name = None
    last_name = None

    # Make email field unique
    email = models.EmailField(unique=True)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']

    def __str__(self):
        return "{} ({})".format(self.username, self.email)
