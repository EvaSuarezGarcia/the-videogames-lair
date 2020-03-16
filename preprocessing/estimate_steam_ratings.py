import pymysql.cursors
import sys
from batch_operation_utils import read_next_batch_data, write_info
from config import mysql

BATCH_SIZE = 10
PROGRESS_FILE = 'estimate_progress.log'
NEXT_BATCH_FILE = 'next_estimate_batch.log'
ROWS_FOLDER = '/datos/eva/steam_ratings'


def prepare_batch(users, cursor):
    batch = []
    for user in users:
        user_id = user[0]
        user_steam_id = user[1]
        
        # Get playtimes for this user
        cursor.execute(select_playtime_sql, user_steam_id)
        playtimes = cursor.fetchall()
        
        # Prepare rows to insert
        batch.extend(prepare_partial_batch(playtimes, user_id, cursor))

    return batch


def prepare_partial_batch(rows, user_id, cursor):
    rows_to_insert = []
    for row in rows:
        steam_id = row[0]
        playtime = row[1]

        # Get GB ID(s) for this game
        cursor.execute(select_game_id_sql, steam_id)
        gb_ids = cursor.fetchall()
        
        # If no GB ID is found, skip this game
        if not gb_ids:
            continue

        # Get quintiles for this Steam game
        cursor.execute(select_game_data_sql, steam_id)
        quintiles = cursor.fetchone()
        p20 = quintiles[0]
        p40 = quintiles[1]
        p60 = quintiles[2]
        p80 = quintiles[3]

        # Estimate rating
        if playtime > p80:
            rating = 5
        elif playtime > p60:
            rating = 4
        elif playtime > p40:
            rating = 3
        elif playtime > p20:
            rating = 2
        else:
            rating = 1

        # Add a row for each gb_id
        for gb_id in gb_ids:
            rows_to_insert.append([user_id, gb_id[0], rating])

    return rows_to_insert


if __name__ == "__main__":
    
    # Read last batch data
    batch_number = 0
    user_id = 1073 # First non-GB user
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
            select_users_sql = "SELECT user_id, foreign_user_id FROM dataset_users " \
                "WHERE user_id >= %s ORDER BY user_id LIMIT %s"
            cursor.execute(select_users_sql, (user_id, BATCH_SIZE+1))
            users = cursor.fetchall()

            select_playtime_sql = "SELECT steam_id, playtime_forever FROM filtered_games_2 " \
                "WHERE user_id=%s AND playtime_forever > 0"
            select_game_id_sql = "SELECT gb_id FROM game_steam_ids WHERE steam_id=%s"
            select_game_data_sql = "SELECT p20, p40, p60, p80 FROM all_steam_games_2 WHERE steam_id = %s"
            insert_sql = "INSERT IGNORE INTO game_ratings (user_id, game_id, rating, estimated) " \
                "VALUES (%s, %s, %s, true)"

            while len(users) > BATCH_SIZE:
                # Insert batch
                batch = prepare_batch(users[0:-1], cursor)
                cursor.executemany(insert_sql, batch)
                connection.commit()

                # Write inserted rows to csv
                write_info(ROWS_FOLDER + "/" + str(batch_number).zfill(5) + ".csv", "w+", batch)

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
            batch = prepare_batch(users, cursor)
            cursor.executemany(insert_sql, batch)
            connection.commit()

            # Write inserted rows to csv
            write_info(ROWS_FOLDER + "/" + str(batch_number).zfill(5) + ".csv", "w+", batch)

            # Write this batch's number and ID of its first row
            write_info(PROGRESS_FILE, "a+", [[batch_number, user_id]])

            # Write finished marker
            write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])
                
    finally:
        connection.close()