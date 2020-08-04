from django.contrib.auth.models import AbstractUser
from django.db import models, transaction
from sequences import Sequence


class User(AbstractUser):
    # Remove unneeded fields
    first_name = None
    last_name = None

    def __str__(self):
        if self.email:
            return "{} ({})".format(self.username, self.email)
        return "{}".format(self.username)


# Initial value is the first unused value from the original sequence that was not managed by Django
user_ids_seq = Sequence("user_ids", initial_value=13577732)


class ALSUser(models.Model):
    """
    Abstract model that has a ``user_id`` drawn from a sequence shared by all ALSUsers regardless
    of their concrete type. This ``user_id`` is used in the ALS model.
    """

    user_id = models.PositiveIntegerField(unique=True)

    def save(self, *args, **kwargs):
        with transaction.atomic():
            if not self.user_id:
                self.user_id = user_ids_seq.get_next_value()
            super().save(*args, **kwargs)

    class Meta:
        abstract = True


class GiantBombEntity(ALSUser):
    """
    Abstract model for entities that have a GB ID, name and GB URL.
    """

    gb_id = models.PositiveIntegerField('Giant Bomb ID', unique=True)
    name = models.CharField(max_length=100)
    site_url = models.CharField(max_length=2000, null=True, blank=True)

    class Meta:
        abstract = True


class AgeRating(GiantBombEntity):
    site_url = None


class Concept(GiantBombEntity):
    deck = models.CharField(max_length=1000, null=True, blank=True)


class Company(GiantBombEntity):
    pass


class Franchise(GiantBombEntity):
    deck = models.CharField(max_length=1000, null=True, blank=True)


class Genre(GiantBombEntity):
    deck = models.CharField(max_length=1000, null=True, blank=True)


class Platform(GiantBombEntity):
    abbreviation = models.CharField(max_length=20)
    deck = models.CharField(max_length=1000, null=True, blank=True)


class Theme(GiantBombEntity):
    pass
