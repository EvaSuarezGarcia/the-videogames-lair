import pymysql.cursors
import sys
from batch_operation_utils import read_next_batch_data, write_info
from config import mysql

BATCH_SIZE = 1000
PROGRESS_FILE = 'valid_progress.log'
NEXT_BATCH_FILE = 'next_valid_batch.log'


if __name__ == "__main__":
    
    # Read last batch data
    batch_number = 0
    user_id = -1
    next_batch = read_next_batch_data(NEXT_BATCH_FILE)
    if next_batch is None:
        print("No more batches to process")
        sys.exit(0)
    elif next_batch:
        batch_number = int(next_batch[0])
        user_id = next_batch[1]

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])

    try:
        with connection.cursor() as cursor:
            # Read first batch of users from dataset_users
            select_users_sql = "SELECT user_id FROM game_ratings " \
                "WHERE user_id >= %s GROUP BY user_id HAVING COUNT(*) < 5 LIMIT %s"
            cursor.execute(select_users_sql, (user_id, BATCH_SIZE+1))
            users = cursor.fetchall()

            update_rows_sql = "UPDATE game_ratings SET is_valid=FALSE WHERE user_id=%s"

            while len(users) > BATCH_SIZE:
                # Update rows
                cursor.executemany(update_rows_sql, users[0:-1])
                connection.commit()

                # Write this batch's number and ID of its first row
                write_info(PROGRESS_FILE, "a+", [[batch_number, user_id]])

                # Write next batch's number and ID of its first row
                batch_number += 1
                user_id = users[-1][0]
                write_info(NEXT_BATCH_FILE, "w+", [[batch_number, user_id]])

                # Read next batch
                cursor.execute(select_users_sql, (user_id, BATCH_SIZE+1))
                users = cursor.fetchall()

            # Insert last batch's data
            cursor.executemany(update_rows_sql, users)
            connection.commit()

            # Write this batch's number and ID of its first row
            write_info(PROGRESS_FILE, "a+", [[batch_number, user_id]])

            # Write finished marker
            write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])
                
    finally:
        connection.close()