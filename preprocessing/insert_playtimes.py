import pymysql.cursors
import sys
from config import mysql
from batch_operation_utils import read_next_batch_data, write_info

BATCH_SIZE = 10000
PROGRESS_FILE = 'insert_progress.log'
NEXT_BATCH_FILE = 'next_batch.log'
ROWS_FOLDER = '/datos/eva/filtered_games_2'

if __name__ == "__main__":

    # Read last data
    batch_number = 0
    steamid = -1
    appid = -1
    next_batch = read_next_batch_data(NEXT_BATCH_FILE)
    if next_batch is None:
        print("No more batches to process")
        sys.exit(0)
    elif next_batch:
        batch_number = int(next_batch[0])
        steamid = next_batch[1]
        appid = next_batch[2]

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])
    
    try:
        with connection.cursor() as cursor:
            # Read first batch from Games_2
            select_sql = "SELECT steamid, appid, playtime_forever FROM Games_2 " \
                "WHERE ((steamid = %s AND appid >= %s) OR (steamid > %s)) AND appid IN (SELECT steam_id FROM all_steam_games_2) " \
                "ORDER BY steamid, appid LIMIT %s"
            insert_sql = "INSERT IGNORE INTO filtered_games_2 (user_id, steam_id, playtime_forever) " \
                "VALUES (%s, %s, %s)"
            cursor.execute(select_sql, (steamid, appid, steamid, BATCH_SIZE+1))
            rows = cursor.fetchall()

            while len(rows) > BATCH_SIZE:
                # Insert row batch into filtered_games_2
                with connection.cursor() as insert_cursor:
                    insert_cursor.executemany(insert_sql, rows[0:-1])
                    connection.commit()         

                # Write rows to csv
                write_info(ROWS_FOLDER + "/" + str(batch_number).zfill(5) + ".csv", "w+", rows[0:-1])

                # Write number and ID of first row from this batch
                write_info(PROGRESS_FILE, "a+", [[batch_number, steamid, appid]])

                # Write number and ID of first row from next batch
                batch_number += 1
                steamid = rows[-1][0]
                appid = rows[-1][1]
                write_info(NEXT_BATCH_FILE, "w+", [[batch_number, steamid, appid]])

                # Read next batch
                cursor.execute(select_sql, (steamid, appid, steamid, BATCH_SIZE+1))
                rows = cursor.fetchall()

            # Write last batch
            with connection.cursor() as insert_cursor:
                insert_cursor.executemany(insert_sql, rows)
                connection.commit()

            # Write rows to csv
            write_info(ROWS_FOLDER + "/" + str(batch_number) + ".csv", "w+", rows)

            # Write number and ID of first row from this batch
            write_info(PROGRESS_FILE, "a+", [[batch_number, steamid, appid]])

            # Write in NEXT_BATCH_FILE finished marker
            write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()