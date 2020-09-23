import pymysql.cursors
import requests
import shutil
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from absl.app import run, flags
from os.path import isfile, join
from preprocessing.gb_api import GbApi

FLAGS = flags.FLAGS

# DB settings
flags.DEFINE_string("host", "localhost", "DB host")
flags.DEFINE_string("user", None, "DB user")
flags.mark_flag_as_required("user")
flags.DEFINE_string("passwd", None, "DB password")
flags.mark_flag_as_required("passwd")
flags.DEFINE_string("db", None, "DB name")
flags.mark_flag_as_required("db")
flags.DEFINE_string("charset", "utf8mb4", "DB charset")

# ES settings
flags.DEFINE_string("esURL", "localhost:9200", "ES URL")
flags.DEFINE_integer("batchSize", 10, "Size of index batches")

# Other settings
flags.DEFINE_string("imagesFolder", None, "Folder to save the game images to")
flags.mark_flag_as_required("imagesFolder")
flags.DEFINE_string("imagesSubfolder", None, "Subfolder to save the game images to. "
                                             "This will be used for the URL of the image field.")


def get_related_metadata(game_id, table, connection):
    select_sql = "SELECT * FROM {} WHERE game_id = {}".format(table, game_id)
    with connection.cursor() as cursor:
        cursor.execute(select_sql)
        rows = [str(x[1]) for x in cursor.fetchall()]
    return rows


def get_game_aliases(game_id, connection):
    return get_related_metadata(game_id, "game_aliases", connection)


def get_game_steam_ids(game_id, connection):
    select_sql = "SELECT * FROM {} WHERE gb_id = {}".format("game_steam_ids", game_id)
    with connection.cursor() as cursor:
        cursor.execute(select_sql)
        rows = [str(x[1]) for x in cursor.fetchall()]
    return rows


def get_game_metadata(game_id, relation_table, connection, metadata_table=None):
    metadata_table = metadata_table or relation_table
    metadata_ids = get_related_metadata(game_id, "game_" + relation_table, connection)
    metadata = []
    metadata_strings = []

    if metadata_ids:
        select_sql = "SELECT name, gb_id FROM {} WHERE user_id IN ({})"\
            .format(metadata_table, ",".join(["%s"] * len(metadata_ids)))
        with connection.cursor() as cursor:
            cursor.execute(select_sql, metadata_ids)
            rows = cursor.fetchall()
            metadata = [x[0] for x in rows]
            metadata_strings = [x[0] + "##" + str(x[1]) for x in rows]
    return metadata, metadata_strings


def get_game_age_ratings(game_id, connection):
    return get_game_metadata(game_id, "age_ratings", connection)


def get_game_concepts(game_id, connection):
    return get_game_metadata(game_id, "concepts", connection)


def get_game_developers(game_id, connection):
    return get_game_metadata(game_id, "developers", connection, metadata_table="companies")


def get_game_publishers(game_id, connection):
    return get_game_metadata(game_id, "publishers", connection, metadata_table="companies")


def get_game_franchises(game_id, connection):
    return get_game_metadata(game_id, "franchises", connection)


def get_game_genres(game_id, connection):
    return get_game_metadata(game_id, "genres", connection)


def get_game_platforms(game_id, connection):
    return get_game_metadata(game_id, "platforms", connection)


def get_game_themes(game_id, connection):
    return get_game_metadata(game_id, "themes", connection)


def add_to_game(game_dict, key, value):
    if value:
        game_dict[key] = value


def add_tuple_to_game(game_dict, key, tuple):
    data, data_strings = tuple
    add_to_game(game_dict, key, data)
    add_to_game(game_dict, key + "_strings", data_strings)


def parse_db_game(game_db, connection):
    game_id = game_db[0]
    game_name = game_db[1]
    game = {
        "gb_id": game_id,
        "vgl_id": game_id,  # Use GB ID as VGL ID to avoid changing the ratings dataset
        "name": game_name
    }
    # Some dates may be missing entirely, or they may be missing the month and/or day
    release_date = "-".join(str(x) for x in filter(lambda x: x is not None, game_db[2:]))
    add_to_game(game, "release_date", release_date)

    # Get other game metadata
    add_to_game(game, "aliases", get_game_aliases(game_id, connection))
    add_to_game(game, "steam_ids", get_game_steam_ids(game_id, connection))
    add_tuple_to_game(game, "age_ratings", get_game_age_ratings(game_id, connection))
    add_tuple_to_game(game, "concepts", get_game_concepts(game_id, connection))
    add_tuple_to_game(game, "developers", get_game_developers(game_id, connection))
    add_tuple_to_game(game, "publishers", get_game_publishers(game_id, connection))
    add_tuple_to_game(game, "franchises", get_game_franchises(game_id, connection))
    add_tuple_to_game(game, "genres", get_game_genres(game_id, connection))
    add_tuple_to_game(game, "platforms", get_game_platforms(game_id, connection))
    add_tuple_to_game(game, "themes", get_game_themes(game_id, connection))

    return game


def add_gb_data(game, gb: GbApi):
    gb_data = gb.get_game(game['gb_id'], field_list=["deck", "description", "image", "site_detail_url"])
    add_to_game(game, "deck", gb_data.get("deck"))
    add_to_game(game, "description", gb_data.get("description"))
    add_to_game(game, "site_detail_url", gb_data.get("site_detail_url"))

    # Download image
    image = gb_data.get("image", {}).get("small_url", "")
    if image:
        filename = image.split("/")[-1]
        file_path = join(FLAGS.imagesFolder, FLAGS.imagesSubfolder, filename)
        if not isfile(file_path):
            response = requests.get(image, stream=True)
            if response.ok:
                response.raw.decode_content = True
                with open(file_path, "wb") as f:
                    shutil.copyfileobj(response.raw, f)
                add_to_game(game, "image", filename)
            else:
                print("Image of game {} could not be downloaded".format(game['gb_id']))
        else:
            add_to_game(game, "image", filename)


def index_games_main(argv):
    client = Elasticsearch(FLAGS.esURL)
    gb = GbApi()
    games_batch = []
    count = 0

    connection = pymysql.connect(host=FLAGS.host, user=FLAGS.user, password=FLAGS.passwd,
                                 db=FLAGS.db, charset=FLAGS.charset)
    try:
        with connection.cursor() as cursor:
            select_games_sql = "SELECT id, main_name, release_date_year, release_date_month, " \
                               "release_date_day FROM games"

            cursor.execute(select_games_sql)
            games_db = cursor.fetchall()

            for game_db in games_db:
                game = parse_db_game(game_db, connection)

                # Skip this game if it already exists in the index
                response = client.search(index="games", body={"query": {"term": {"gb_id": game['gb_id']}}})
                if not response.get("hits", {}).get("total", {}).get("value", 0):
                    add_gb_data(game, gb)
                    action = {
                        "_op_type": "index",
                        "_index": "games",
                        "_source": game
                    }
                    games_batch.append(action)
                    count += 1
                    print("Progress: Prepared {} games".format(count))

                    if len(games_batch) == FLAGS.batchSize:
                        bulk(client, games_batch)
                        games_batch = []
                        print("Progress: Indexed {} games".format(count))

    finally:
        connection.close()

    # Possible last batch
    if games_batch:
        bulk(client, games_batch)
        print("Progress: Indexed {} games".format(count))
        print("Finished!")


if __name__ == '__main__':
    run(index_games_main)