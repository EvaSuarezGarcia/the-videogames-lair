import pymysql.cursors
import sys
from config import mysql
from batch_operation_utils import read_next_batch_data, write_info

BATCH_SIZE = 100
PROGRESS_FILE = 'insert_users_progress.log'
NEXT_BATCH_FILE = 'next_insert_users_batch.log'
ROWS_FOLDER = '/home/eva/tfm/insert_users'

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
            # Read first batch of users
            # We only select users that have played at least 5 games
            select_sql = "SELECT user_id FROM filtered_games_2 " \
                "WHERE user_id >= %s AND playtime_forever > 0 " \
                "GROUP BY user_id HAVING COUNT(*) >= 5 ORDER BY user_id LIMIT %s"
            cursor.execute(select_sql, (user_id, BATCH_SIZE+1))
            users = cursor.fetchall()

            # Insert those users into dataset_users
            insert_sql = "INSERT INTO dataset_users (foreign_user_id, steam_user) VALUES (%s, true)"
            
            while len(users) > BATCH_SIZE:
                # Insert this batch  
                cursor.executemany(insert_sql, users[0:-1])
                connection.commit()

                # Write rows 
                write_info(ROWS_FOLDER + "/" + str(batch_number).zfill(5) + ".csv", "w+", users[0:-1])

                # Write this batch's number and ID of its first row
                write_info(PROGRESS_FILE, "a+", [[batch_number, user_id]])

                # Write next batch's number and ID of its first row
                batch_number += 1
                user_id = users[-1][0]
                write_info(NEXT_BATCH_FILE, "w+", [[batch_number, user_id]])

                # Read next batch
                cursor.execute(select_sql, (user_id, BATCH_SIZE+1))
                users = cursor.fetchall()

            # Insert last batch
            cursor.executemany(insert_sql, users)
            connection.commit()

            # Write rows 
            write_info(ROWS_FOLDER + "/" + str(batch_number).zfill(5) + ".csv", "w+", users)

            # Write this batch's number and ID of its first row
            write_info(PROGRESS_FILE, "a+", [[batch_number, user_id]])

            # Write finished mark
            write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()