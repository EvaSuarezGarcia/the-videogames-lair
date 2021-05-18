from typing import List, Tuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection, columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import DoesNotExist, BatchQuery
from django.utils import timezone

from videogames_lair_site import settings

connection.setup(settings.CASSANDRA_HOST, default_keyspace="videogames_lair",
                 auth_provider=PlainTextAuthProvider(username=settings.CASSANDRA_USER,
                                                     password=settings.CASSANDRA_PASSWORD))


class Recommendation(Model):
    __table_name__ = "recommendations"
    user_id = columns.Integer(primary_key=True)
    game_id = columns.Integer()
    rank = columns.Integer(primary_key=True)


class Rating(Model):
    __table_name__ = "ratings"
    user_id = columns.Integer(primary_key=True)
    game_id = columns.Integer(primary_key=True)
    rating = columns.TinyInt()
    estimated = columns.Boolean()
    created_at = columns.DateTime()
    updated_at = columns.DateTime()


class SteamGame(Model):
    __table_name__ = "steam_games"
    steam_id = columns.Integer(primary_key=True)
    p20 = columns.Float()
    p40 = columns.Float()
    p60 = columns.Float()
    p80 = columns.Float()
    players = columns.Integer()
    owners = columns.Integer()


class SteamPlaytime(Model):
    __table_name__ = "steam_playtimes"
    user_id = columns.Integer(primary_key=True)
    steam_id = columns.Integer(primary_key=True)
    playtime = columns.Integer()


def get_user_ratings(user_id: int) -> List[Rating]:
    ratings = list(Rating.filter(user_id=user_id))
    return sorted(ratings, key=lambda rating: (rating.updated_at, rating.game_id), reverse=True)


def get_recommendations_for_user(user_id: int) -> List[Recommendation]:
    recommendations = Recommendation.filter(user_id=user_id).order_by("rank")
    rated_game_ids = [rating.game_id for rating in get_user_ratings(user_id)]
    return [recommendation for recommendation in recommendations if recommendation.game_id not in rated_game_ids]


def get_user_rating_for_game(user_id: int, game_id: int) -> Rating:
    try:
        rating = Rating.get(user_id=user_id, game_id=game_id)
    except DoesNotExist:
        rating = None

    return rating


def rate_game(user_id: int, game_id: int, rating: int) -> None:
    previous_rating = get_user_rating_for_game(user_id, game_id)
    now = timezone.now()
    rating_object = Rating(user_id=user_id, game_id=game_id, rating=rating, estimated=False,
                           updated_at=now)
    if not previous_rating:
        rating_object.created_at = now

    rating_object.save()


def get_steam_games(game_steam_ids: List[int]) -> List[SteamGame]:
    return list(SteamGame.filter(steam_id__in=game_steam_ids))


def insert_steam_playtimes(user_id: int, playtimes: List[Tuple[int, int]]):
    with BatchQuery() as b:
        for playtime in playtimes:
            SteamPlaytime.batch(b).create(user_id=user_id, steam_id=playtime[0], playtime=playtime[1])


def insert_estimated_ratings(user_id: int, ratings: List[Tuple[int, int]]):
    now = timezone.now()
    with BatchQuery() as b:
        for rating in ratings:
            Rating.batch(b).create(user_id=user_id, game_id=rating[0], rating=rating[1], estimated=True,
                                   created_at=now, updated_at=now)
