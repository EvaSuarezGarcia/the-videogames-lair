import pymysql

from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from absl.app import run, flags

FLAGS = flags.FLAGS

# DB settings
flags.DEFINE_string("host", "localhost", "DB host")
flags.DEFINE_string("user", None, "DB user")
flags.mark_flag_as_required("user")
flags.DEFINE_string("passwd", None, "DB password")
flags.mark_flag_as_required("passwd")
flags.DEFINE_string("db", None, "Source DB name")
flags.mark_flag_as_required("db")
flags.DEFINE_string("charset", "utf8mb4", "DB charset")

# Cassandra settings
flags.DEFINE_list("nodes", None, "Cassandra nodes")
flags.mark_flag_as_required("nodes")
flags.DEFINE_string("cassandraUser", None, "Cassandra user")
flags.mark_flag_as_required("cassandraUser")
flags.DEFINE_string("cassandraPasswd", None, "Cassandra password")
flags.DEFINE_string("keyspace", "videogames_lair", "Cassandra keyspace")


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def move_steam_games_data(cursor, session):
    # Retrieve steam games from DB
    select_games_sql = "SELECT * FROM all_steam_games_2"
    cursor.execute(select_games_sql)
    games = cursor.fetchall()

    # Prepare and insert batches into Cassandra
    insert_cql = "INSERT INTO steam_games (steam_id, game_name, p20, p40, p60, p80, players, owners) " \
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    insert_stmt = session.prepare(insert_cql)

    for chunk in chunks(games, 25):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for game in chunk:
            batch.add(insert_stmt, (game["steam_id"], game["game_name"], game["p20"], game["p40"], game["p60"],
                                    game["p80"], game["players"], game["owners"]))
        session.execute(batch, timeout=60.0)


def get_als_user_ids(cursor):
    select_sql = "SELECT user_id, foreign_user_id FROM dataset_users WHERE steam_user"
    cursor.execute(select_sql)
    users = cursor.fetchall()
    users_dict = {}
    for user in users:
        users_dict[int(user["foreign_user_id"])] = user["user_id"]

    return users_dict


def move_playtimes(cursor, session):
    # Get ALS id for Steam users (create a dict)
    users_dict = get_als_user_ids(cursor)

    # Get playtimes from DB (possibly in batches)
    select_sql = "SELECT * FROM filtered_games_2 " \
                 "WHERE ((user_id = %s AND steam_id >= %s) OR user_id > %s) " \
                 "ORDER BY user_id, steam_id LIMIT 26"
    cursor.execute(select_sql, (76561197961346418, 40, 76561197961346418))
    rows = cursor.fetchall()

    insert_cql = "INSERT INTO steam_playtimes (user_id, steam_id, playtime) " \
                 "VALUES (?, ?, ?)"
    insert_stmt = session.prepare(insert_cql)
    count = 0

    while len(rows) > 25:
        # For each row, get ALS user ID and add insert statement to batch
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for row in rows[0:-1]:
            if row["user_id"] in users_dict:
                user_id = users_dict[row["user_id"]]
                batch.add(insert_stmt, (user_id, row["steam_id"], row["playtime_forever"]))
        session.execute(batch, timeout=60.0)
        last_row = rows[-1]
        cursor.execute(select_sql, (last_row["user_id"], last_row["steam_id"], last_row["user_id"]))
        rows = cursor.fetchall()
        count += 25
        print("Progress: {} rows".format(count))
        print("Now starting at user_id {} and steam_id {}".format(last_row["user_id"], last_row["steam_id"]))

    if rows:
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for row in rows:
            user_id = users_dict[row["user_id"]]
            batch.add(insert_stmt, (user_id, row["steam_id"], row["playtime_forever"]))
        session.execute(batch, timeout=60.0)
        print("Finished")


def main(argv):

    connection = pymysql.connect(host=FLAGS.host, user=FLAGS.user,
                                 password=FLAGS.passwd,
                                 db=FLAGS.db, charset=FLAGS.charset,
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        auth_provider = PlainTextAuthProvider(
            username=FLAGS.cassandraUser,
            password=FLAGS.cassandraPasswd
        )

        with Cluster(FLAGS.nodes, auth_provider=auth_provider) as cluster:
            with cluster.connect(FLAGS.keyspace) as session:
                with connection.cursor() as cursor:
                    move_steam_games_data(cursor, session)
                    move_playtimes(cursor, session)

    finally:
        connection.close()


if __name__ == '__main__':
    run(main)
