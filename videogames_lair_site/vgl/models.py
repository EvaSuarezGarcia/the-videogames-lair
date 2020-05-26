from django.contrib.auth.models import AbstractUser


class User(AbstractUser):
    # Remove unneeded fields
    first_name = None
    last_name = None

    def __str__(self):
        if self.email:
            return "{} ({})".format(self.username, self.email)
        return "{}".format(self.username)
