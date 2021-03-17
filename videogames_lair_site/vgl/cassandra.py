from typing import Optional, List, Tuple

from cassandra import query, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement

from videogames_lair_site import settings


class CassandraConnectionManager:

    host: str = settings.CASSANDRA_HOST
    user: str = settings.CASSANDRA_USER
    password: str = settings.CASSANDRA_PASSWORD
    keyspace: str = "videogames_lair"
    select_rating_statement: str = "SELECT * FROM ratings WHERE user_id=%s AND game_id=%s"

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
        recommendations = list(self.session.execute("SELECT * FROM recommendations "
                                                    "WHERE user_id = %s ORDER BY rank", [user_id]))
        rated_games = [rating.game_id for rating in self.get_user_ratings(user_id)]
        return [recommendation for recommendation in recommendations
                if recommendation.game_id not in rated_games]

    def get_user_ratings(self, user_id: int) -> List:
        ratings = list(self.session.execute("SELECT * FROM ratings WHERE user_id = %s", [user_id]))
        return sorted(ratings, key=lambda rating: (rating.created_at, rating.game_id), reverse=True)

    def get_user_rating_for_game(self, user_id: int, game_id: int):
        rating = None
        result = list(self.session.execute(self.select_rating_statement, [user_id, game_id]))
        if result:
            rating = result[0]
        return rating

    def rate_game(self, user_id: int, game_id: int, rating: int) -> bool:
        previous_rating = self.session.execute(self.select_rating_statement, [user_id, game_id])

        if previous_rating:
            self.session.execute("INSERT INTO ratings (user_id, game_id, rating, estimated, updated_at) "
                                 "VALUES (%s, %s, %s, false, toTimestamp(now()))", [user_id, game_id, rating])
        else:
            self.session.execute("INSERT INTO ratings (user_id, game_id, rating, estimated, created_at, updated_at) "
                                 "VALUES (%s, %s, %s, false, toTimestamp(now()), toTimestamp(now()))",
                                 [user_id, game_id, rating])
        success = len(list(self.session.execute(self.select_rating_statement, [user_id, game_id]))) == 1
        return success

    def get_steam_games(self, game_steam_ids: List[int]):
        result = self.session.execute("SELECT * FROM steam_games WHERE steam_id IN %s",
                                      [query.ValueSequence(game_steam_ids)])
        return list(result)

    def insert_steam_playtimes(self, user_id: int, playtimes: List[Tuple[int, int]]):
        insert_cql = "INSERT INTO steam_playtimes (user_id, steam_id, playtime) " \
                     "VALUES (?, ?, ?)"
        insert_stmt = self.session.prepare(insert_cql)
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for playtime in playtimes:
            batch.add(insert_stmt, (user_id, playtime[0], playtime[1]))
        self.session.execute(batch)

    def insert_estimated_ratings(self, user_id: int, ratings: List[Tuple[int, int]]):
        insert_cql = "INSERT INTO ratings (user_id, game_id, rating, estimated, created_at, updated_at) " \
                     "VALUES (?, ?, ?, true, toTimestamp(now()), toTimestamp(now()))"
        insert_stmt = self.session.prepare(insert_cql)
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for rating in ratings:
            batch.add(insert_stmt, (user_id, rating[0], rating[1]))
        self.session.execute(batch)
