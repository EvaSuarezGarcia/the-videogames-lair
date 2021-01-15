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

    def get_recommendations_for_user(self, user_id: int) -> List[NamedTuple]:
        return list(self.session.execute(f"SELECT * FROM recommendations "
                                         f"WHERE user_id = {user_id} "
                                         f"ORDER BY rank"))
