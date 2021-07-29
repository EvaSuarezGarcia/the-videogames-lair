import logging, log_config
import pymysql
import gb_api
import db_utils
from config import mysql

if __name__ == "__main__":

    # Set up logging conf
    log_config.setup_logging('gb-games-metadata.log')
    logger = logging.getLogger('gb-games-metadata')

    # Set up database conf
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])

    db = db_utils.DbUtils()
    gb = gb_api.GbApi()

    # Get GB games from database
    try:
        games = []
        with connection.cursor() as cursor:
            get_games_sql = "SELECT game_id FROM gb_staff_games"
            cursor.execute(get_games_sql)
            games = cursor.fetchall()
            # Convert list of tuples to list of strings
            games = list(map(lambda x: x[0], games))

        # Real ID of game that had an erroneous ID in the original GB dataset (found during a failed run)
        games.extend(['3030-8161'])

        field_list = ["aliases", "api_detail_url", "concepts", "developers",
            "expected_release_day", "expected_release_month", "expected_release_year",
            "franchises", "genres", "id", "name", "original_game_rating", "original_release_date",
            "platforms", "publishers", "site_detail_url", "themes"
        ]

        for game_id in games:
            result = gb.get_game_and_related_elements(game_id.split('-')[1], field_list=field_list)

            if result:
                
                game = result['game']

                ### Store metadata in database
                with connection.cursor() as cursor:
                    # Store elements related to the game
                    db.store_concepts(cursor, result['concepts'])
                    db.store_companies(cursor, result['companies'])
                    db.store_franchises(cursor, result['franchises'])
                    db.store_genres(cursor, result['genres'])
                    db.store_age_ratings(cursor, result['age_ratings'])
                    db.store_platforms(cursor, result['platforms'])
                    db.store_themes(cursor, result['themes'])

                    # Store game and its relationship with elements above
                    db.store_game(cursor, game)

                    connection.commit()

    finally:
        connection.close()