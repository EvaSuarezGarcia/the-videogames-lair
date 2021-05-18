import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "videogames_lair_site.settings")
django.setup()

import pymysql.cursors
from absl.app import run, flags
from vgl.models import AgeRating, Concept, Company, Franchise, Genre, Platform, Theme
from preprocessing.gb_api import GbApi

FLAGS = flags.FLAGS

# DB settings
flags.DEFINE_string("host", "localhost", "DB host")
flags.DEFINE_string("user", None, "DB user")
flags.mark_flag_as_required("user")
flags.DEFINE_string("passwd", None, "DB password")
flags.mark_flag_as_required("passwd")
flags.DEFINE_string("db", None, "Source DB name")
flags.mark_flag_as_required("db")
flags.DEFINE_string("charset", "utf8mb4", "DB charset")

gb = GbApi()


def move_data(cursor, Model, table, gb_fields=None, has_url=True):
    if gb_fields is None:
        gb_fields = []
    select_sql = "SELECT * FROM {}".format(table)
    cursor.execute(select_sql)
    source_items = cursor.fetchall()
    count = 0

    for source_item in source_items:
        item = Model(gb_id=source_item["gb_id"], user_id=source_item["user_id"], name=source_item["name"])

        request_fields = ["site_detail_url"] if has_url else []
        request_fields.extend(gb_fields)

        # Retrieve GB data
        if request_fields:
            gb_data = gb.get(source_item["api_detail_url"], request_fields)
            for gb_field in gb_fields:
                if gb_field in gb_data:
                    setattr(item, gb_field, gb_data[gb_field])
            if has_url and "site_detail_url" in gb_data:
                item.site_url = gb_data["site_detail_url"]

        item.save()
        count += 1

        if count % 100 == 0:
            print("Progress: {}/{}".format(count, len(source_items)))

    print("Finished moving table {}".format(table))


def main(argv):
    connection = pymysql.connect(host=FLAGS.host, user=FLAGS.user, password=FLAGS.passwd,
                                 db=FLAGS.db, charset=FLAGS.charset, cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            move_data(cursor, AgeRating, "age_ratings", has_url=False)
            move_data(cursor, Concept, "concepts", ["deck"])
            move_data(cursor, Company, "companies")
            move_data(cursor, Franchise, "franchises", ["deck"])
            move_data(cursor, Genre, "genres", ["deck"])
            move_data(cursor, Platform, "platforms", ["abbreviation", "deck"])
            move_data(cursor, Theme, "themes")

    finally:
        connection.close()


if __name__ == '__main__':
    run(main)
