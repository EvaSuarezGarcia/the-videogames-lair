import pymysql
import sys
from config import mysql, cassandra_conf
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from cassandra.auth import PlainTextAuthProvider
from batch_operation_utils import read_next_batch_data, write_info

def write_batch_to_cassandra(rows):
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for row in rows:
        #batch.add(insert_stmt, (row["game_id"], row["user_id"], row["rating"], bool(row["estimated"])))
        session.execute(insert_cql, (row["game_id"], row["user_id"], row["rating"], bool(row["estimated"])))
    #session.execute(batch)

BATCH_SIZE = 1000
PROGRESS_FILE = 'write_to_cassandra_progress.log'
NEXT_BATCH_FILE = 'next_cassandra_batch.log'

if __name__ == "__main__":
    
    # Read last batch data
    batch_number = 0
    user_id = -1
    game_id = -1
    next_batch = read_next_batch_data(NEXT_BATCH_FILE)
    if next_batch is None:
        print("No more batches to process")
        sys.exit(0)
    elif next_batch:
        batch_number = int(next_batch[0])
        user_id = next_batch[1]
        game_id = next_batch[2]

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

        with Cluster(auth_provider=auth_provider, default_retry_policy=RetryPolicy.RETRY) as cluster:
            with cluster.connect(cassandra_conf["keyspace"]) as session:
                with connection.cursor() as cursor:
                    # Retrieve first batch of game ratings
                    sql = "SELECT user_id, game_id, rating, estimated FROM game_ratings " \
                        "WHERE (user_id = %s AND game_id >= %s) OR user_id > %s " \
                        "ORDER BY user_id, game_id LIMIT %s"
                    cursor.execute(sql, (user_id, game_id, user_id, BATCH_SIZE+1))
                    rows = cursor.fetchall()

                    # Prepare insert statement
                    insert_cql = "INSERT INTO ratings (game_id, user_id, rating, estimated) " \
                        "VALUES (%s, %s, %s, %s)"

                    while len(rows) > BATCH_SIZE:
                        write_batch_to_cassandra(rows[0:-1])
                        
                        # Log progress 
                        write_info(PROGRESS_FILE, "a+", [[batch_number, user_id, game_id]])
                        batch_number += 1
                        user_id = rows[-1]["user_id"]
                        game_id = rows[-1]["game_id"]
                        write_info(NEXT_BATCH_FILE, "w+", [[batch_number, user_id, game_id]])
                        
                        # Fetch next batch
                        cursor.execute(sql, (user_id, game_id, user_id, BATCH_SIZE+1))
                        rows = cursor.fetchall()

                    # Write last batch
                    if rows:
                        write_batch_to_cassandra(rows)
                        write_info(PROGRESS_FILE, "a+", [[batch_number, user_id, game_id]])
                        write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()