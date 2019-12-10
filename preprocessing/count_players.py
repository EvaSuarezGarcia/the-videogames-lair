import pymysql.cursors
import sys
from config import mysql
from batch_operation_utils import read_next_batch_data, write_info

BATCH_SIZE = 1
PROGRESS_FILE = 'players_progress.log'
NEXT_BATCH_FILE = 'next_players_batch.log'

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
                                 charset=mysql["charset"])
    
    try:
        with connection.cursor() as cursor:
            # Read first batch from all_steam_games
            select_sql = "SELECT DISTINCT steam_id FROM all_steam_games_2 " \
                "WHERE steam_id >= %s ORDER BY steam_id LIMIT %s"
            cursor.execute(select_sql, (steam_id, BATCH_SIZE+1))
            rows = cursor.fetchall()

            update_sql = "UPDATE all_steam_games_2 s JOIN (" \
                "SELECT steam_id, COUNT(*) AS players FROM filtered_games_2 " \
                "WHERE steam_id IN (%s) AND playtime_forever > 0 GROUP BY steam_id) AS g " \
                "ON s.steam_id = g.steam_id " \
                "SET s.players = g.players"

            while len(rows) > BATCH_SIZE:
                # Count players for these games using filtered_games_2
                format_strings = ','.join(['%s'] * BATCH_SIZE)
                cursor.execute(update_sql % format_strings, rows[0:-1])
                connection.commit()

                # Write this batch's number and ID of its first row
                write_info(PROGRESS_FILE, "a+", [[batch_number, steam_id]])

                # Write next batch's number and ID of its first row
                batch_number += 1
                steam_id = rows[-1][0]
                write_info(NEXT_BATCH_FILE, "w+", [[batch_number, steam_id]])
                
                # Read next batch
                cursor.execute(select_sql, (steam_id, BATCH_SIZE+1))
                rows = cursor.fetchall()

            # Write last batch
            format_strings = ','.join(['%s'] * (len(rows)))
            cursor.execute(update_sql % format_strings, rows)
            connection.commit()

            # Write this batch's number and ID of its first row
            write_info(PROGRESS_FILE, "a+", [[batch_number, steam_id]])

            # Write in NEXT_BATCH_FILE finished marker
            write_info(NEXT_BATCH_FILE, "w+", [["Finished"]])

    finally:
        connection.close()