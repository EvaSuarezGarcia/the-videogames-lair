from typing import List, Tuple
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import lit, row_number, desc, bround
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.ml.recommendation import ALS, ALSModel
from absl import app, flags
import logging
import os.path
from hdfs.client import InsecureClient, Client

FLAGS = flags.FLAGS

# ALS parameters
flags.DEFINE_multi_integer("iter", [10], "Number of iterations (can be specified more than once)")
flags.DEFINE_multi_integer("rank", [10], "Number of latent factors (can be specified more than once)")
flags.DEFINE_multi_float("regParam", [0.1], "Regularization parameter (can be specified more than once)")
flags.DEFINE_string("userCol", "user_id", "Name of the user column")
flags.DEFINE_string("itemCol", "game_id", "Name of the item column")
flags.DEFINE_string("ratingCol", "rating", "Name of the ratings column")
flags.DEFINE_string("checkpointDir", None, "Checkpoint directory")
flags.mark_flag_as_required("checkpointDir")
flags.DEFINE_integer("checkpointInterval", 5, "Checkpoint interval in number of iterations")
flags.DEFINE_integer("numBlocks", 50, "Number of blocks for ALS parallelization")

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

# Other settings
flags.DEFINE_integer("folds", 5, "Number of folds")
flags.register_validator("folds", lambda x: x >= 2, "Number of folds must be >= 2")
flags.DEFINE_integer("seed", 7, "Seed for random split")
flags.DEFINE_string("modelsDir", "als-models", "Directory to save the models to")
flags.DEFINE_string("runsDir", "als-runs", "Directory to save the runs to")
flags.DEFINE_string("qrelsDir", "qrels", "Directory to save the qrels to")
flags.DEFINE_string("hdfsUrl", None, "HDFS namenode URL")
flags.mark_flag_as_required("hdfsUrl")
flags.DEFINE_string("hdfsUser", None, "HDFS user")
flags.DEFINE_integer("batches", 100, "Number of batches to divide target recommendation list into")

# Driver logging configuration
logging.basicConfig(filename="evaluate-als.log", level=logging.INFO,
                    format="[%(asctime)s][%(levelname)s][%(name)s]: %(message)s")
logger = logging.getLogger("create_runs")


def get_folds_from_qrels(spark: SparkSession, client: Client):

    splits = []

    if client.status(FLAGS.qrelsDir, strict=False) and len(client.list(FLAGS.qrelsDir)) == FLAGS.folds:
        logger.info("Reading qrels from HDFS...")
        schema = StructType([StructField(FLAGS.userCol, IntegerType(), False),
                             StructField("zero", IntegerType(), False),
                             StructField(FLAGS.itemCol, IntegerType(), False),
                             StructField(FLAGS.ratingCol, IntegerType(), False)])
        for qrel_file in client.list(FLAGS.qrelsDir):
            splits.append(spark.read.csv(os.path.join(FLAGS.qrelsDir, qrel_file), schema=schema, sep="\t")
                          .select(FLAGS.userCol, FLAGS.itemCol, FLAGS.ratingCol).cache())

        # Materialize splits in memory and log their size
        for i, split in enumerate(splits, 1):
            logger.info("Split %d size: %d" % (i, split.count()))

    else:
        raise RuntimeError("Invalid qrels directory: %s. Check that the folder exists and "
                           "that it contains %d files" % (FLAGS.qrelsDir, FLAGS.folds))

    # Create folds
    folds = []
    for i, split in enumerate(splits):
        other_splits = splits[:i] + splits[i+1:]
        training_split = other_splits[0]

        for other_split in other_splits[1:]:
            training_split = training_split.union(other_split)

        folds.append((training_split, split))

    return folds


def get_target_list(training_set: DataFrame, test_set: DataFrame) -> Tuple[List[DataFrame], DataFrame, DataFrame]:
    logger.info("Generating list of target recommendations...")

    training_items = training_set.select(FLAGS.itemCol)
    test_items = test_set.select(FLAGS.itemCol)
    target_items = test_items.intersect(training_items).cache()

    training_users = training_set.select(FLAGS.userCol)
    test_users = test_set.select(FLAGS.userCol)
    target_users = test_users.intersect(training_users).cache()

    logger.info("Target users: %d" % target_users.count())
    logger.info("Target items: %d" % target_items.count())

    target_batches = []

    for i, batch in enumerate(target_users.randomSplit([1.0] * FLAGS.batches, seed=FLAGS.seed)):
        # Get all possible combinations of target_user,target_item
        product = batch.crossJoin(target_items)

        # Remove all combinations already present in the training set
        target_list = product.subtract(training_set.select(FLAGS.userCol, FLAGS.itemCol))

        # Add batch to list
        target_batches.append(target_list)

    return target_batches, target_users, target_items


def train_or_get_model(iterations: int, rank: int, reg_param: float, training_set: DataFrame, client: Client,
                       model_name: str) -> ALSModel:
    model_file = os.path.join(FLAGS.modelsDir, model_name)

    # Check if model already exists
    if client.status(model_file, strict=False):
        logger.warning("Not training model %s: Model already exists" % model_name)
        logger.info("Loading model %s" % model_name)
        model = ALSModel.load(model_file)
    else:
        logger.info("Training model %s" % model_name)
        als = ALS(maxIter=iterations, rank=rank, regParam=reg_param, userCol=FLAGS.userCol,
                  itemCol=FLAGS.itemCol, ratingCol=FLAGS.ratingCol)
        als.setCheckpointInterval(FLAGS.checkpointInterval)
        als.setNumBlocks(FLAGS.numBlocks)
        model = als.fit(training_set)
        # Save trained model
        model.save(model_file)
        logger.info("Training finished!")

    return model


def create_run(model: ALSModel, target_batches: List[DataFrame], model_name: str, client: Client):

    logger.info("Creating run...")

    # Run will be written as several files, so we group them inside a folder representing the whole run
    this_run_folder = os.path.join(FLAGS.runsDir, "run-" + model_name + ".tmp")
    run_batch_tmp_template = os.path.join(this_run_folder, "run-" + model_name + "_%03d.tmp")
    run_batch_final_template = os.path.join(this_run_folder, "run-" + model_name + "_%03d.txt")

    batch_count = 0

    # Iterate over target batches
    for i, batch in enumerate(target_batches):
        run_batch_tmp = run_batch_tmp_template % i
        run_batch_final = run_batch_final_template % i

        if client.status(run_batch_final, strict=False):
            logger.warning("Not writing batch %s: Batch already exists" % run_batch_final)
            batch_count += 1
        else:
            logger.info("Writing batch %d..." % i)

            # Preemptively delete incomplete temporary file of this batch
            client.delete(run_batch_tmp, recursive=True)

            # Generate recommendations
            recommendations = model.transform(batch).withColumnRenamed("prediction", "score")
            recommendations = recommendations.withColumn("Q0", lit("Q0"))\
                .withColumn("rank", row_number().over(Window.partitionBy(FLAGS.userCol).orderBy(desc("score"))) - 1)\
                .withColumn("method", lit("-"))

            # Write recommendations to tmp file
            recommendations.where("rank < 100").orderBy("user_id", "rank")\
                .select(FLAGS.userCol, "Q0", FLAGS.itemCol, "rank", bround("score", 5), "method")\
                .write.csv(run_batch_tmp, sep="\t")

            # Move from tmp file to final file
            client.rename(run_batch_tmp, run_batch_final)

            batch_count += 1
            logger.info("Progress: %d/%d batches" % (batch_count, FLAGS.batches))

    client.rename(this_run_folder, this_run_folder.replace(".tmp", ".txt"))
    logger.info("Finished model " + model_name)


def create_runs():
    with SparkSession.builder \
        .appName("Evaluate ALS") \
        .master("yarn") \
        .config("spark.cassandra.connection.host", FLAGS.cassandraNodes) \
        .config("spark.cassandra.auth.username", FLAGS.cassandraUser) \
        .config("spark.cassandra.auth.password", FLAGS.cassandraPwd) \
        .config("spark.cassandra.input.consistency.level", "QUORUM") \
        .getOrCreate() \
    as spark:

        spark.sparkContext.setCheckpointDir(FLAGS.checkpointDir)

        # Get HDFS client
        client = InsecureClient(FLAGS.hdfsUrl, user=FLAGS.hdfsUser)

        # Get dataset
        folds = get_folds_from_qrels(spark, client)

        # Iterate over folds
        for i, (training_set, test_set) in enumerate(folds, 1):
            logger.info("Processing fold %d" % i)

            # Get target list
            target_batches, target_users, target_items = get_target_list(training_set, test_set)

            # Iterate over params
            for iterations in FLAGS.iter:
                for rank in FLAGS.rank:
                    for reg_param in FLAGS.regParam:
                        model_name = "ALS-%d-%d-%d-fold%d" % (iterations, rank, reg_param, i)
                        run_file = os.path.join(FLAGS.runsDir, "run-%s.txt" % model_name)

                        # Check if this run already exists
                        if client.status(run_file, strict=False):
                            logger.warning("Not evaluating model: iter %d rank %d param %f for fold %d; run already exists")
                        else:
                            logger.info("Evaluating model: iter %d rank %d param %f for fold %d" % (iterations, rank, reg_param, i))

                            # Get ALS model
                            model = train_or_get_model(iterations, rank, reg_param, training_set, client, model_name)

                            # Generate run
                            create_run(model, target_batches, model_name, client)

            # Unpersist dataframes from this fold
            target_users.unpersist()
            target_items.unpersist()


def main(argv):
    create_runs()


if __name__ == '__main__':
    app.run(main)