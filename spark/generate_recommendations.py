from absl import app, flags
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType

import os

FLAGS = flags.FLAGS

# Settings
flags.DEFINE_integer("batches", 1000, "Number of batches to divide target recommendation list into")
flags.DEFINE_string("modelPath", "ALS-model", "Path to the ALS model to use")
flags.DEFINE_integer("numRec", 100, "Number of recommendations to generate per user")
flags.DEFINE_integer("seed", 7, "Seed for random split")
flags.DEFINE_string("output", None, "Folder to write the CSV recommendations to")
flags.mark_flag_as_required("output")


def process_row(row):
    items = row.recommendations
    recommendations = []
    for rank, item in enumerate(items, 1):
        recommendations.append((row.user_id, item.game_id, rank))
    return recommendations


def generate_recommendations():
    with SparkSession.builder \
        .appName("Generate Recommendations") \
        .getOrCreate() \
    as spark:
        # Load model from NTFS
        model: ALSModel = ALSModel.load(FLAGS.modelPath)

        # Get users from model's userFactors
        users = model.userFactors.select("id").withColumnRenamed("id", "user_id")
        users.cache()

        # Divide users in batches
        user_batches = users.randomSplit([1.0] * FLAGS.batches, seed=FLAGS.seed)

        # Recommendations schema
        schema = StructType([
            StructField("user_id", IntegerType(), False),
            StructField("game_id", IntegerType(), False),
            StructField("rank", IntegerType(), False)
        ])

        # Generate recommendations for each batch
        for i, user_batch in enumerate(user_batches, 1):
            recommendations = model.recommendForUserSubset(user_batch, FLAGS.numRec)

            # Reformat recommendations
            recommendations = recommendations.rdd.flatMap(process_row)

            # Create DataFrame
            recs_df = spark.createDataFrame(recommendations, schema=schema)

            # Save DataFrame to CSV file
            csv_file = os.path.join(FLAGS.output, f"batch_{i}.csv")
            recs_df.write.csv(csv_file, sep=";")


def main(argv):
    generate_recommendations()


if __name__ == '__main__':
    app.run(main)
