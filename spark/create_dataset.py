from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from absl.app import run, flags
import os

FLAGS = flags.FLAGS

# Cassandra settings
flags.DEFINE_string("cassandraNodes", None, "Comma-separated list of Cassandra hosts")
flags.mark_flag_as_required("cassandraNodes")
flags.DEFINE_string("cassandraUser", None, "User to access Cassandra")
flags.mark_flag_as_required("cassandraUser")
flags.DEFINE_string("cassandraPwd", None, "Password to access Cassandra")
flags.mark_flag_as_required("cassandraPwd")
flags.DEFINE_string("keyspace", None, "Cassandra keyspace")
flags.mark_flag_as_required("keyspace")
flags.DEFINE_string("table", None, "Ratings table")
flags.mark_flag_as_required("table")
flags.DEFINE_string("userCol", "user_id", "Name of the user column")
flags.DEFINE_string("itemCol", "game_id", "Name of the item column")
flags.DEFINE_string("ratingCol", "rating", "Name of the ratings column")

# Other settings
flags.DEFINE_string("datasetName", None, "Dataset name")
flags.mark_flag_as_required("datasetName")
flags.DEFINE_string("datasetDir", "datasets", "Directory in which to save the dataset")
flags.DEFINE_integer("minRatings", 1, "Minimum number of ratings that a user needs to have to be included "
                                      "in the dataset")
flags.DEFINE_integer("folds", 5, "Number of folds")
flags.register_validator("folds", lambda x: x >= 2, "Number of folds must be >= 2")
flags.DEFINE_integer("seed", 7, "Seed for random split")
flags.DEFINE_bool("overwrite", False, "Overwrite dataset if exists")


def main(argv):
    with SparkSession.builder \
        .appName("Create Dataset %s" % FLAGS.datasetName) \
        .config("spark.cassandra.connection.host", FLAGS.cassandraNodes) \
        .config("spark.cassandra.auth.username", FLAGS.cassandraUser) \
        .config("spark.cassandra.auth.password", FLAGS.cassandraPwd) \
        .config("spark.cassandra.input.consistency.level", "QUORUM") \
        .getOrCreate() \
    as spark:
        ratings = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(keyspace=FLAGS.keyspace, table=FLAGS.table) \
            .load().select(FLAGS.userCol, FLAGS.itemCol, FLAGS.ratingCol)

        valid_users = ratings.groupBy(FLAGS.userCol).count().where("count >= " + str(FLAGS.minRatings)) \
            .select(FLAGS.userCol).withColumnRenamed(FLAGS.userCol, "this_" + FLAGS.userCol)

        valid_ratings = ratings.join(valid_users, ratings[FLAGS.userCol] == valid_users["this_" + FLAGS.userCol]) \
            .select(FLAGS.userCol, FLAGS.itemCol, FLAGS.ratingCol)

        # Split dataset into FLAG.folds splits of (aprox) the same size
        valid_ratings.cache()
        weights = [1.0] * FLAGS.folds
        splits = [split.cache() for split in valid_ratings.randomSplit(weights, seed=FLAGS.seed)]

        # Materialize splits in memory
        for split in splits:
            split.count()

        # Unpersist original ratings df as it is no longer needed
        valid_ratings.unpersist()

        # Write folds
        dataset_root_dir = os.path.join(FLAGS.datasetDir, FLAGS.datasetName)
        test_dir = os.path.join(dataset_root_dir, "test")
        base_dir = os.path.join(dataset_root_dir, "base")
        qrels_dir = os.path.join(dataset_root_dir, "qrels")

        write_mode = 'overwrite' if FLAGS.overwrite else 'error'

        for i, split in enumerate(splits):
            file_name = "u" + str(i + 1)

            # Write test set
            split.write.csv(os.path.join(test_dir, file_name + ".test"), sep="\t", mode=write_mode)

            # Write qrels
            split_qrels = split.withColumn("zero", lit(0))
            split_qrels.select(FLAGS.userCol, "zero", FLAGS.itemCol, FLAGS.ratingCol).write.csv(
                os.path.join(qrels_dir, file_name + ".qrel"), sep="\t", mode=write_mode)

            # Write training set
            other_splits = splits[:i] + splits[i+1:]
            base_split = other_splits[0]

            for other_split in other_splits[1:]:
                base_split = base_split.union(other_split)

            base_split.write.csv(os.path.join(base_dir, file_name + ".base"), sep="\t", mode=write_mode)


if __name__ == '__main__':
    run(main)
