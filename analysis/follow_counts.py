"""Spark job: count followers and followees per user in the Twitter edge list.

Usage: spark-submit follow_counts.py <input_uri> <output_uri>

Each input line is "<follower> <followee>". Malformed lines, self-follows and
duplicate edges are dropped before counting. Output is a single CSV with
header user_id,followers,followees.
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def clean_edges(spark: SparkSession, input_uri: str) -> DataFrame:
    parts = F.split(F.trim(F.col("value")), r"\s+")
    return (
        spark.read.text(input_uri)
        .select(parts.alias("p"))
        .filter(F.size("p") == 2)
        .select(F.col("p")[0].alias("src"), F.col("p")[1].alias("dst"))
        .filter(F.col("src").rlike(r"^\d+$") & F.col("dst").rlike(r"^\d+$"))
        .filter(F.col("src") != F.col("dst"))
        .distinct()
    )


def compute_counts(spark: SparkSession, input_uri: str) -> DataFrame:
    edges = clean_edges(spark, input_uri)
    followees = edges.groupBy(F.col("src").alias("user_id")).agg(F.count("*").alias("followees"))
    followers = edges.groupBy(F.col("dst").alias("user_id")).agg(F.count("*").alias("followers"))
    return (
        followers.join(followees, on="user_id", how="full_outer")
        .na.fill(0, ["followers", "followees"])
        .select("user_id", "followers", "followees")
    )


def main(argv: list[str]) -> None:
    if len(argv) != 3:
        sys.exit("usage: follow_counts.py <input_uri> <output_uri>")
    input_uri, output_uri = argv[1], argv[2]

    spark = SparkSession.builder.appName("twitter-follow-counts").getOrCreate()
    counts = compute_counts(spark, input_uri)
    # ~80k rows: a single output file keeps the backend loader simple.
    counts.coalesce(1).write.mode("overwrite").option("header", True).csv(output_uri)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
