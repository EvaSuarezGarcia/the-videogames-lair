import pymysql
import log_config, logging
from config import mysql

if __name__ == "__main__":

    # Setup logging conf
    log_config.setup_logging('steam-common-games.log')
    logger = logging.getLogger('steam-common-games')
    logger.setLevel(logging.DEBUG)

    # Set up database conf
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])
                    
    try:
        with connection.cursor() as cursor:
            # Get steam games that are in gb dataset too
            # Collate specified in order to not take capitalization and accents into account when comparing
            # Precisely because of that, a distinct is also necessary
            sql = "WITH g AS ( \
                      SELECT id, main_name FROM games \
                      UNION \
                      SELECT game_id, alias FROM game_aliases) \
                   SELECT DISTINCT id AS gb_id, steam_id \
                   FROM all_steam_games_2 JOIN g ON game_name = main_name \
                   COLLATE utf8mb4_general_ci"
            cursor.execute(sql)
            common_games = cursor.fetchall()

            # Store steam ID - GB ID relationship
            sql = "INSERT IGNORE INTO game_steam_ids (gb_id, steam_id) VALUES (%s, %s)"
            cursor.executemany(sql, common_games)
        
        connection.commit()

        with connection.cursor() as cursor:
            sql = "SELECT DISTINCT steam_id FROM game_steam_ids"
            cursor.execute(sql)
            common_games = cursor.fetchall()
            logger.debug("Found " + str(len(common_games)) + " Steam games in common with GB")
    
    finally:
        connection.close()