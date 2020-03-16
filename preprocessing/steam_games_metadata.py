import logging, log_config
import pymysql
import gb_api
import db_utils
import difflib
import re
from config import mysql

def clean_name(name):
    name = name.lower()
    name = re.sub("( :|\.|'|,)", "", name)
    name = re.sub("(: | - )", " ", name)
    name = name.replace("&", "and")
    return name.strip()

def longest_common_substring(s1, s2):
    matcher = difflib.SequenceMatcher(None, s1, s2)
    match = matcher.find_longest_match(0, len(s1), 0, len(s2))

    if match.size != 0:
        return s1[match.a : match.a + match.size]
    else:
        return ''

def store_game_list(matching_games, gb, db):
    for game_id in matching_games:
        # Retrieve data for all matching games
        result = gb.get_game_and_related_elements(game_id)

        if result:
            with connection.cursor() as insert_cursor:
                # Store elements related to the game
                db.store_concepts(insert_cursor, result['concepts'])
                db.store_companies(insert_cursor, result['companies'])
                db.store_franchises(insert_cursor, result['franchises'])
                db.store_genres(insert_cursor, result['genres'])
                db.store_age_ratings(insert_cursor, result['age_ratings'])
                db.store_platforms(insert_cursor, result['platforms'])
                db.store_themes(insert_cursor, result['themes'])

                # Store game and its relationship with elements above
                db.store_game(insert_cursor, result['game'])

                # Store game and Steam ID
                sql = "INSERT IGNORE INTO game_steam_ids (gb_id, steam_id) VALUES (%s, %s)"
                insert_cursor.execute(sql, (game_id, steam_id))

if __name__ == "__main__":

    # Setup logging conf
    log_config.setup_logging('steam-games-metadata.log')
    logger = logging.getLogger('steam-games-metadata')
    logger.setLevel(logging.DEBUG)

    # Set up database conf
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])
                    
    try:
        db = db_utils.DbUtils()
        gb = gb_api.GbApi()

        not_found_games = 0

        with connection.cursor() as cursor:
            # Get all steam games not found in GB
            sql = "SELECT steam_id, game_name FROM all_steam_games_2 WHERE steam_id NOT IN "\
                "(SELECT steam_id FROM game_steam_ids)"
            cursor.execute(sql)
            rows = cursor.fetchall()
            logger.debug("Searching " + str(len(rows)) + " Steam games")

            for row in rows:
                steam_id = row[0]
                #game_name = row[1].lower().replace(' :', '').replace(':', '').replace('&', 'and').strip()
                game_name = clean_name(row[1])
                # Search this game's name in GB
                results = gb.search(game_name, field_list=["id", "aliases", "name"])
                matching_games = []
                matching_games_lcs = []
                exact_match = False

                # Iterate over results
                for result in results:
                    #result_name = result['name'].lower().replace(' :', '').replace(':', '').replace('&', 'and').strip() if result['name'] else ''
                    result_name = clean_name(result['name'])
                    aliases_tmp = gb.split_aliases(result['aliases']) if result['aliases'] else []
                    aliases = []
                    for alias in aliases_tmp:
                        aliases.append(alias.lower().replace(':', '').strip())

                    # Record games having a name or alias that matches (lowercase)
                    if result_name and game_name == result_name:
                        matching_games.append(result['id'])
                        logger.debug('MATCHING NAME: Found matching game (' + str(result['id']) + ',' + result['name'] + \
                            ') for Steam game (' + str(steam_id) + ',' + game_name + ')')
                        exact_match = True
                    elif aliases:
                        for alias in aliases:
                            if alias == game_name:
                                matching_games.append(result['id'])
                                logger.debug('MATCHING ALIAS: Found matching game (' + str(result['id']) + ',' + result['name'] + \
                                    ') for Steam game (' + str(steam_id) + ',' + game_name + ')')
                                exact_match = True
                                break
                        
                    # If no exact match has been found, try name and aliases with LCS
                    if not exact_match:
                        all_names = [result_name] + aliases
                        for name in all_names:
                            lcs = longest_common_substring(game_name, name)
                            if lcs == name:
                                matching_games_lcs.append(result['id'])
                                logger.debug('MATCHING LCS: Found matching game (' + str(result['id']) + ',' + result['name'] + \
                                    ') for Steam game (' + str(steam_id) + ',' + game_name + ') by LCS (' + lcs + ')')
                    # If an exact match has been found, skip other results
                    else:
                        break

                # If found retrieve all of this game's data
                if matching_games:
                    if len(matching_games) > 1:
                        logger.warning('MULTIPLE MATCHING GAMES: For Steam game (' \
                            + str(steam_id) + ',' + game_name +'): ' + str(matching_games))
                    
                    store_game_list(matching_games, gb, db)
                    connection.commit()
                elif matching_games_lcs:
                    if len(matching_games_lcs) > 1:
                        logger.warning('MULTIPLE MATCHING GAMES (LCS): For Steam game (' \
                            + str(steam_id) + ',' + game_name +'): ' + str(matching_games_lcs))
                    store_game_list(matching_games_lcs, gb, db)
                    connection.commit()
                else:
                    # If not found, log this game's name
                    logger.warning('NOT FOUND: No matching games found for ' \
                        + 'Steam game (' + str(steam_id) + ',' + game_name + ')')
                    not_found_games += 1

        if not_found_games > 0:
            logger.warning('NUMBER OF NOT FOUND GAMES: ' + str(not_found_games))

    finally:
        connection.close()