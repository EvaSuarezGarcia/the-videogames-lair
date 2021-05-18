from absl import flags, app
from pyspark.sql import SparkSession, functions

import os

FLAGS = flags.FLAGS

flags.DEFINE_string("output", None, "Folder to write the CSVs to")
flags.mark_flag_as_required("output")


def get_game_stats(argv):
    with SparkSession.builder \
        .appName("Game stats") \
        .getOrCreate() \
    as spark:
        ratings = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(keyspace="videogames_lair", table="ratings") \
            .load()

        ratings_by_game = ratings.groupBy("game_id")
        players = ratings_by_game.count().withColumnRenamed("count", "players")
        popularity = ratings_by_game.sum("rating").withColumnRenamed("game_id", "this_game_id")\
            .withColumnRenamed("sum(rating)", "popularity")
        game_stats = players.join(popularity, players["game_id"] == popularity["this_game_id"])\
            .drop("this_game_id")
        game_stats = game_stats.withColumn("mean_rating", functions.round(game_stats["popularity"] / game_stats["players"], 1))\
            .cache()

        # Write stats to CSV
        game_stats.select("game_id", "popularity", "players", "mean_rating")\
            .write.csv(os.path.join(FLAGS.output, "game_stats"), sep=";")

        # Most popular games
        most_popular_games = game_stats.orderBy(game_stats.popularity.desc())
        most_popular_games.select("game_id", "popularity", "players", "mean_rating")\
            .write.csv(os.path.join(FLAGS.output, "most_popular_games"), sep=";")

        # Most popular genres
        genres = spark.read.format("org.apache.spark.sql.cassandra")\
            .options(keyspace="videogames_lair", table="game_genres").load()\
            .withColumnRenamed("user_id", "genre_id").withColumnRenamed("game_id", "this_game_id")\
            .select("genre_id", "this_game_id").distinct()
        game_stats_with_genre = game_stats.join(genres, game_stats["game_id"] == genres["this_game_id"]).drop("this_game_id")
        games_by_genre = game_stats_with_genre.groupBy("genre_id")
        genre_players = games_by_genre.sum("players").withColumnRenamed("sum(players)", "players")
        genre_popularity = games_by_genre.sum("popularity").withColumnRenamed("genre_id", "this_genre_id")\
            .withColumnRenamed("sum(popularity)", "popularity")
        genre_stats = genre_players.join(genre_popularity, genre_players["genre_id"] == genre_popularity["this_genre_id"])\
            .drop("this_genre_id")
        genre_stats = genre_stats.withColumn("mean_rating", functions.round(genre_stats["popularity"] / genre_stats["players"], 1))\
            .orderBy(genre_stats.popularity.desc()).cache()
        genre_stats.select("genre_id", "popularity", "players", "mean_rating")\
            .write.csv(os.path.join(FLAGS.output, "most_popular_genres"), sep=";")

        # Most popular games from most popular genre
        most_popular_genre = genre_stats.head(1)[0].genre_id
        games_of_most_popular_genre = game_stats_with_genre.where(game_stats_with_genre.genre_id == most_popular_genre)\
            .orderBy("popularity", ascending=False)
        games_of_most_popular_genre.select("game_id", "popularity", "players", "mean_rating", "genre_id")\
            .write.csv(os.path.join(FLAGS.output, "games_of_most_popular_genre"), sep=";")


if __name__ == '__main__':
    app.run(get_game_stats)
