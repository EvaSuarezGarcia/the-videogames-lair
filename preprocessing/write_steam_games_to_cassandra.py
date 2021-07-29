import pymysql
import sys

from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement

from config import mysql, cassandra_conf
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from batch_operation_utils import read_next_batch_data, write_info


def write_batch_to_cassandra(rows):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for row in rows:
        batch.add(insert_stmt, (row["steam_id"], row["p20"], row["p40"], row["p60"], row["p80"],
                                row["players"], row["owners"]))
    session.execute(batch, timeout=60.0)


BATCH_SIZE = 100
PROGRESS_FILE = 'logs/write_steam_to_cassandra_progress.log'
NEXT_BATCH_FILE = 'logs/next_steam_cassandra_batch.log'


if __name__ == "__main__":

    # Read last batch data
    batch_number = 0
    steam_id = -1
    next_batch = read_next_batch_data(NEXT_BATCH_FILE)
    if next_batch is None:
        print("No more batches to process")
        sys.exit(0)
    elif next_batch:
        batch_number = int(next_batch[0])
        steam_id = next_batch[1]

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"],
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        # Connect to Cassandra
        auth_provider = PlainTextAuthProvider(
            username=cassandra_conf["user"],
            password=cassandra_conf["password"]
        )

        with Cluster(cassandra_conf["nodes"], auth_provider=auth_provider) as cluster:
            with cluster.connect(cassandra_conf["keyspace"]) as session:
                with connection.cursor() as cursor:
                    # Retrieve first batch of game ratings
                    sql = "SELECT steam_id, p20, p40, p60, p80, players, owners FROM all_steam_games_2 " \
                        "WHERE steam_id >= %s " \
                        "ORDER BY steam_id LIMIT %s"
                    cursor.execute(sql, (steam_id, BATCH_SIZE+1))
                    rows = cursor.fetchall()

                    # Prepare insert statement
                    insert_cql = "INSERT INTO steam_games (steam_id, p20, p40, p60, p80, players, owners) " \
                        "VALUES (?, ?, ?, ?, ?, ?, ?)"
                    insert_stmt = session.prepare(insert_cql)

                    while len(rows) > BATCH_SIZE:
                        write_batch_to_cassandra(rows[0:-1])

                        # Log progress
                        write_info(PROGRESS_FILE, "a+", [[batch_number, steam_id]])
                        batch_number += 1
                        steam_id = rows[-1]["steam_id"]
                        write_info(NEXT_BATCH_FILE, "w+", [[batch_number, steam_id]])

                        # Fetch next batch
                        cursor.execute(sql, (steam_id, BATCH_SIZE+1))
                        rows = cursor.fetchall()

                    # Write last batch
                    if rows:
                        write_batch_to_cassandra(rows)
                        write_info(PROGRESS_FILE, "a+", [[batch_number, steam_id]])
                        write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()
