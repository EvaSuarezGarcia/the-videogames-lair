import logging
import pymysql

class DbUtils(object):

    logger = None

    def __init__(self):
        self.logger = logging.getLogger('db-utils')

    def store_game(self, cursor, game):
        try:
            sql = "INSERT IGNORE INTO games (id, main_name, api_detail_url, site_detail_url, " \
                + "release_date_year, release_date_month, release_date_day, estimated_date) " \
                + "VALUES (%(id)s, %(name)s, %(api_url)s, %(site_url)s, " \
                    "%(release_year)s, %(release_month)s, %(release_day)s, %(estimated_date)s)"
            cursor.execute(sql, game)

            # Store this game's relationships with other elements
            self.store_aliases(cursor, game['aliases'])
            self.store_game_concepts(cursor, game['concepts'])
            self.store_game_developers(cursor, game['developers'])
            self.store_game_publishers(cursor, game['publishers'])
            self.store_game_franchises(cursor, game['franchises'])
            self.store_game_genres(cursor, game['genres'])
            self.store_game_age_ratings(cursor, game['age_ratings'])
            self.store_game_platforms(cursor, game['platforms'])
            self.store_game_themes(cursor, game['themes'])

        except pymysql.err.MySQLError as e:
            msg = e.args[1] + ' in table games. Data: ' + str(game)
            self.logger.exception(msg)
            raise(e)  

    def store_relationship(self, cursor, table, data):
        try:
            sql = "INSERT IGNORE INTO " + table + " VALUES (%s, %s)"
            cursor.executemany(sql, data)
        except pymysql.err.MySQLError as e:
            msg = e.args[1] + ' in table ' + table + '. Data: ' + str(data)
            self.logger.exception(msg)
            raise(e)

    def store_general_element(self, cursor, table, data):
        try:
            sql = "INSERT IGNORE INTO " + table + " VALUES (%(id)s, %(name)s, %(api_detail_url)s, %(site_detail_url)s)"
            cursor.executemany(sql, data)
        except pymysql.err.MySQLError as e:
            msg = e.args[1] + ' in table ' + table + '. Data: ' + str(data)
            self.logger.exception(msg)
            raise(e)

    def store_aliases(self, cursor, aliases):
        self.store_relationship(cursor, 'game_aliases', aliases)

    def store_concepts(self, cursor, concepts):
        self.store_general_element(cursor, 'concepts', concepts)

    def store_game_concepts(self, cursor, game_concepts):
        self.store_relationship(cursor, 'game_concepts', game_concepts)

    def store_companies(self, cursor, companies):
        self.store_general_element(cursor, 'companies', companies)

    def store_game_developers(self, cursor, developers):
        self.store_relationship(cursor, 'game_developers', developers)

    def store_game_publishers(self, cursor, publishers):
        self.store_relationship(cursor, 'game_publishers', publishers)

    def store_franchises(self, cursor, franchises):
        self.store_general_element(cursor, 'franchises', franchises)

    def store_game_franchises(self, cursor, game_franchises):
        self.store_relationship(cursor, 'game_franchises', game_franchises)

    def store_genres(self, cursor, genres):
        self.store_general_element(cursor, 'genres', genres)
    
    def store_game_genres(self, cursor, game_genres):
        self.store_relationship(cursor, 'game_genres', game_genres)

    def store_age_ratings(self, cursor, age_ratings):
        self.store_general_element(cursor, 'age_ratings', age_ratings)

    def store_game_age_ratings(self, cursor, game_age_ratings):
        self.store_relationship(cursor, 'game_age_ratings', game_age_ratings)

    def store_platforms(self, cursor, platforms):
        try:
            sql = "INSERT IGNORE INTO platforms VALUES (%(id)s, %(name)s, %(abbreviation)s, %(api_detail_url)s, %(site_detail_url)s)"
            cursor.executemany(sql, platforms)
        except pymysql.err.MySQLError as e:
            msg = e.args[1] + ' in table platforms. Data: ' + str(platforms)
            self.logger.exception(msg)
            raise(e)
    
    def store_game_platforms(self, cursor, game_platforms):
        self.store_relationship(cursor, 'game_platforms', game_platforms)

    def store_themes(self, cursor, themes):
        self.store_general_element(cursor, 'themes', themes)

    def store_game_themes(self, cursor, game_themes):
        self.store_relationship(cursor, 'game_themes', game_themes)