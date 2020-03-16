#!/usr/bin/env python

import csv, sys
import pymysql.cursors
from config import mysql

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_gb_user_reviews.py <csv_file>")
        sys.exit(0)
    
    reviews_file = sys.argv[1]

    # Connect to db
    connection = pymysql.connect(host=mysql["host"],
                                 user=mysql["user"],
                                 password=mysql["password"],
                                 db=mysql["db"],
                                 charset=mysql["charset"])

    try:
        with open(reviews_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            next(csv_reader)
            rows = [r for r in csv_reader]
            
            with connection.cursor() as cursor:
                sql = "INSERT INTO gb_user_reviews (score, review_date, game_name, game_id, reviewer) " \
                    + "VALUES (%s, %s, %s, %s, %s)"
                cursor.executemany(sql, rows)
                connection.commit()

    finally:
        connection.close()