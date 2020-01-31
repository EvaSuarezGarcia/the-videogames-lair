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
        batch.add(insert_stmt, (row[0], row[1]))
    session.execute(batch, timeout=60.0)


BATCH_SIZE = 100
PROGRESS_FILE = 'logs/write_metadata_to_cassandra_progress.log'
NEXT_BATCH_FILE = 'logs/gti next_cassandra_metadata_batch.log'
METADATA_PREFIX = "game_"
METADATA_ELEMENTS = [("age_rating",), ("concept",),
                     ("developer", "game_companies"), ("franchise",),
                     ("genre",), ("platform",), ("publisher", "game_companies"),
                     ("theme",)]

if __name__ == "__main__":

    # Read last batch data
    batch_number = 0
    user_id = -1
    game_id = -1
    table_index = 0
    next_batch = read_next_batch_data(NEXT_BATCH_FILE)
    if next_batch is None:
        print("No more batches to process")
        sys.exit(0)
    elif next_batch:
        batch_number = int(next_batch[0])
        user_id = next_batch[1]
        game_id = next_batch[2]
        table_index = int(next_batch[3])

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])
    
    try:
        # Connect to Cassandra
        auth_provider = PlainTextAuthProvider(
            username=cassandra_conf["user"],
            password=cassandra_conf["password"]
        )

        with Cluster(cassandra_conf["nodes"], auth_provider=auth_provider) as cluster:
            with cluster.connect(cassandra_conf["keyspace"]) as session:
                with connection.cursor() as cursor:
                    for i in range(table_index, len(METADATA_ELEMENTS)):
                        element = METADATA_ELEMENTS[table_index][0]
                        table = METADATA_PREFIX + element + "s"
                        cassandra_table = METADATA_ELEMENTS[table_index][1] if len(METADATA_ELEMENTS[table_index]) == 2 else table
                        element_id = element + "_id"
                        # Retrieve first batch of game ratings
                        sql = "SELECT " + element_id + ", game_id FROM " + table + " " \
                            "WHERE (" + element_id + " = %s AND game_id >= %s) OR " + element_id + " > %s " \
                            "ORDER BY " + element_id + ", game_id LIMIT %s"
                        cursor.execute(sql, (user_id, game_id, user_id, BATCH_SIZE+1))
                        rows = cursor.fetchall()

                        # Prepare insert statement
                        insert_cql = "INSERT INTO " + cassandra_table + " (user_id, game_id, rating) " \
                            "VALUES (?, ?, 1)"
                        insert_stmt = session.prepare(insert_cql)

                        while len(rows) > BATCH_SIZE:
                            write_batch_to_cassandra(rows[0:-1])
                            
                            # Log progress 
                            write_info(PROGRESS_FILE, "a+", [[batch_number, user_id, game_id, table_index]])
                            batch_number += 1
                            user_id = rows[-1][0]
                            game_id = rows[-1][1]
                            write_info(NEXT_BATCH_FILE, "w+", [[batch_number, user_id, game_id, table_index]])
                            
                            # Fetch next batch
                            cursor.execute(sql, (user_id, game_id, user_id, BATCH_SIZE+1))
                            rows = cursor.fetchall()

                        # Write last batch
                        if rows:
                            write_batch_to_cassandra(rows)
                            write_info(PROGRESS_FILE, "a+", [[batch_number, user_id, game_id, table_index]])

                        # Reset user_id and game_id
                        user_id = -1
                        game_id = -1
                        
                        # Next table
                        table_index += 1
                        batch_number += 1
                        write_info(NEXT_BATCH_FILE, "w+", [[batch_number, user_id, game_id, table_index]])

                    write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()