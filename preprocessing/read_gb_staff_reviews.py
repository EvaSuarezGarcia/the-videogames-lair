#!/usr/bin/env python

import sys, json
import pymysql.cursors
from config import mysql
from os import listdir
from os.path import join, isfile

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_gb_staff_reviews.py <json files directory>")
        sys.exit(0)

    # Get files in dir
    dir = sys.argv[1]
    files = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])

    all_games = []
    all_releases = []

    # Read files' results and insert into db tables
    try:
        for reviews_file in files:
            games = []
            releases = []
            reviews = []

            json_file = open(reviews_file)
            data = json.load(json_file)
            results = data["results"]
            for result in results:
                review = {
                    "reviewer": result["reviewer"],
                    "score": result["score"],
                }

                review["dlc_name"] = result["dlc_name"]

                # Avoid trying to insert the same data twice
                game = result["game"]
                review["game_id"] = game["id"] if game else None
                if game and game not in all_games:
                    games.append(game)
                    all_games.append(game)
                
                release = result["release"]
                review["release_id"] = release["id"] if release else None
                if release and release not in all_releases:
                    releases.append(release)
                    all_releases.append(release)

                reviews.append(review)
                
            # Write data to db
            with connection.cursor() as cursor:
                if games:
                    try:
                        sql = "INSERT INTO gb_staff_games (game_id, game_name, game_api_detail_url, game_site_detail_url) " \
                            + "VALUES (%(id)s, %(name)s, %(api_detail_url)s, %(site_detail_url)s)"
                        cursor.executemany(sql, games)
                        connection.commit()
                    except pymysql.err.IntegrityError as e:
                        raise pymysql.err.IntegrityError(e.args, reviews_file, "game")

                if releases:
                    try:
                        sql = "INSERT INTO gb_staff_releases (release_id, release_name, release_api_detail_url) " \
                            + "VALUES (%(id)s, %(name)s, %(api_detail_url)s)"
                        cursor.executemany(sql, releases)
                        connection.commit()
                    except pymysql.err.IntegrityError as e:
                        raise pymysql.err.IntegrityError(e.args, reviews_file, "release")

                if reviews:
                    try:
                        sql = "INSERT INTO gb_staff_reviews (reviewer, game_id, dlc_name, score, release_id) " \
                            + "VALUES (%(reviewer)s, %(game_id)s, %(dlc_name)s, %(score)s, %(release_id)s)"    
                        cursor.executemany(sql, reviews)
                        connection.commit()
                    except pymysql.err.IntegrityError as e:
                        raise pymysql.err.IntegrityError(e.args, reviews_file, "review")

    # Close db connection
    finally:
        connection.close()