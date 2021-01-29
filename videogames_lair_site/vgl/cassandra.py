from typing import Optional, List, NamedTuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

from videogames_lair_site import settings


class CassandraConnectionManager:

    host: str = settings.CASSANDRA_HOST
    user: str = settings.CASSANDRA_USER
    password: str = settings.CASSANDRA_PASSWORD
    keyspace: str = "videogames_lair"

    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

    def __enter__(self):
        auth_provider = PlainTextAuthProvider(
            username=self.user,
            password=self.password
        )
        self.cluster = Cluster(self.host, auth_provider=auth_provider)
        self.session = self.cluster.connect(keyspace=self.keyspace)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session is not None:
            self.session.shutdown()
        if self.cluster is not None:
            self.cluster.shutdown()

    def get_recommendations_for_user(self, user_id: int) -> List:
        recommendations = list(self.session.execute(f"SELECT * FROM recommendations "
                                                    f"WHERE user_id = {user_id} "
                                                    f"ORDER BY rank"))
        rated_games = [rating.game_id for rating in self.get_user_ratings(user_id)]
        return [recommendation for recommendation in recommendations
                if recommendation.game_id not in rated_games]

    def get_user_ratings(self, user_id: int) -> List:
        ratings = list(self.session.execute(f"SELECT * FROM ratings "
                                            f"WHERE user_id = {user_id}"))
        return sorted(ratings, key=lambda rating: (rating.created_at, rating.game_id))

    def rate_game(self, user_id: int, game_id: int, rating: int) -> bool:
        previous_rating = self.session.execute(f"SELECT * FROM ratings WHERE user_id={user_id} AND game_id={game_id}")

        if previous_rating:
            self.session.execute(f"INSERT INTO ratings (user_id, game_id, rating, estimated, updated_at) "
                                 f"VALUES ({user_id}, {game_id}, {rating}, false, toTimestamp(now()))")
        else:
            self.session.execute(f"INSERT INTO ratings (user_id, game_id, rating, estimated, created_at, updated_at) "
                                 f"VALUES ({user_id}, {game_id}, {rating}, false, "
                                 f"toTimestamp(now()), toTimestamp(now()))")
        success = len(list(self.session.execute(f"SELECT * FROM ratings "
                                                f"WHERE user_id={user_id} AND game_id={game_id}"))) == 1
        return success
