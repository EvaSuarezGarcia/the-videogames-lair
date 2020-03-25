from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from absl import app, flags


def main(argv):
    # Create Spark session configured for accessing Cassandra
    with SparkSession.builder \
            .appName("Train ALS") \
            .master("yarn") \
            .config("spark.cassandra.connection.host", FLAGS.cassandraNodes) \
            .config("spark.cassandra.auth.username", FLAGS.cassandraUser) \
            .config("spark.cassandra.auth.password", FLAGS.cassandraPwd) \
            .config("spark.cassandra.input.consistency.level", "QUORUM") \
            .getOrCreate() \
            as spark:
        spark.sparkContext.setCheckpointDir(FLAGS.checkpointDir)

        # Read ratings table as dataframe
        ratings = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(keyspace=FLAGS.keyspace, table=FLAGS.tables[0]) \
            .load().select(FLAGS.userCol, FLAGS.itemCol, FLAGS.ratingCol)
        for table in FLAGS.tables[1:]:
            table_df = spark.read.format("org.apache.spark.sql.cassandra") \
                .options(keyspace=FLAGS.keyspace, table=table) \
                .load().select(FLAGS.userCol, FLAGS.itemCol, FLAGS.ratingCol)
            ratings = ratings.union(table_df)

        # Train ALS model
        als = ALS(maxIter=FLAGS.iter, userCol=FLAGS.userCol, itemCol=FLAGS.itemCol, ratingCol=FLAGS.ratingCol)
        als.setCheckpointInterval(FLAGS.checkpointInterval)
        als.setNumBlocks(FLAGS.numBlocks)
        model = als.fit(ratings)

        # Save model to HDFS
        model.write().overwrite().save(FLAGS.outputPath)


FLAGS = flags.FLAGS

# ALS parameters
flags.DEFINE_integer("iter", 1, "Number of iterations")
flags.DEFINE_string("userCol", "user_id", "Name of the user column")
flags.DEFINE_string("itemCol", "game_id", "Name of the item column")
flags.DEFINE_string("ratingCol", "rating", "Name of the ratings column")
flags.DEFINE_string("checkpointDir", "checkpoint-dir", "Checkpoint directory")
flags.DEFINE_integer("checkpointInterval", 5, "Checkpoint interval in number of iterations")
flags.DEFINE_integer("numBlocks", 50, "Number of blocks for ALS parallelization")
flags.DEFINE_string("outputPath", "als-models/als", "Path for saving the ALS model")

# Cassandra settings
flags.DEFINE_string("cassandraNodes", None, "Comma-separated list of Cassandra hosts")
flags.mark_flag_as_required("cassandraNodes")
flags.DEFINE_string("cassandraUser", None, "User to access Cassandra")
flags.mark_flag_as_required("cassandraUser")
flags.DEFINE_string("cassandraPwd", None, "Password to access Cassandra")
flags.mark_flag_as_required("cassandraPwd")
flags.DEFINE_string("keyspace", None, "Cassandra keyspace")
flags.mark_flag_as_required("keyspace")
flags.DEFINE_list("tables", None, "Comma-separated list of Cassandra rating tables")
flags.mark_flag_as_required("tables")
flags.register_validator("tables", lambda tables: len(tables) > 0, "Tables list must not be empty")

if __name__ == "__main__":
    app.run(main)
