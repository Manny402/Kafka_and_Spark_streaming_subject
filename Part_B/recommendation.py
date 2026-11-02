"""Build and apply a small item-to-item recommendation model for Part B."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F, types as T


EVENT_SCHEMA = T.StructType([
    T.StructField("timestamp", T.LongType()),
    T.StructField("visitorid", T.LongType()),
    T.StructField("event", T.StringType()),
    T.StructField("itemid", T.LongType()),
    T.StructField("transactionid", T.LongType()),
])

RECOMMENDATION_COLUMNS = [
    "visitorid", "window_start", "window_end", "candidate_itemid", "rank",
    "source", "rec_score", "popularity_score", "contributing_seed_itemids",
]


def weight_expression(event_column="event"):
    """Return the shared 1/5/10 engagement score expression."""
    return (F.when(F.col(event_column) == "view", 1)
            .when(F.col(event_column) == "addtocart", 5)
            .when(F.col(event_column) == "transaction", 10)
            .otherwise(0))


def load_model(spark, model_path):
    """Load immutable similarity/popularity artifacts and their manifest."""
    root = Path(model_path)
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Recommendation model is missing: {manifest_path}. "
            "Run recommendation.py --build-model first."
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError(f"Unsupported model schema: {manifest.get('schema_version')}")
    similarity = spark.read.parquet(str(root / "item_similarity"))
    popularity = spark.read.parquet(str(root / "popularity"))
    expected_similarity = {"seed_itemid", "candidate_itemid", "similarity"}
    expected_popularity = {"candidate_itemid", "popularity_score"}
    if not expected_similarity.issubset(similarity.columns):
        raise ValueError("item_similarity has an incompatible schema")
    if not expected_popularity.issubset(popularity.columns):
        raise ValueError("popularity has an incompatible schema")
    return similarity, popularity, manifest


def eligible_expression(policy="strict_cart", threshold=5):
    """Define the single business rule used by intent and ranking."""
    base = (F.col("transactions") == 0) & (F.col("intent_score") >= threshold)
    if policy == "strict_cart":
        return base & (F.col("carts") > 0)
    if policy == "extended":
        return base
    raise ValueError(f"Unsupported recommendation policy: {policy}")


def generate_recommendations(context, similarity, popularity,
                             policy="strict_cart", threshold=5, top_k=3):
    """Rank unique unseen candidates for every eligible visitor/window."""
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    keys = ["visitorid", "window_start", "window_end"]
    seeds = (context.where(eligible_expression(policy, threshold))
             .select(*keys, F.col("itemid").alias("seed_itemid"), "intent_score"))
    seen = (context.select(*keys, F.col("itemid").alias("candidate_itemid"))
            .dropDuplicates(keys + ["candidate_itemid"]))

    candidate_scores = (seeds.join(similarity, "seed_itemid")
        .withColumn("contribution", F.col("intent_score") * F.col("similarity"))
        .groupBy(*keys, "candidate_itemid")
        .agg(F.sum("contribution").alias("rec_score"),
             F.sort_array(F.collect_set("seed_itemid")).alias("contributing_seed_itemids"))
        .join(seen, keys + ["candidate_itemid"], "left_anti"))

    cf_order = Window.partitionBy(*keys).orderBy(
        F.desc("rec_score"), F.asc("candidate_itemid"))
    cf = (candidate_scores.withColumn("rank", F.row_number().over(cf_order))
          .where(F.col("rank") <= top_k)
          .withColumn("source", F.lit("item_cf"))
          .withColumn("popularity_score", F.lit(None).cast("double")))

    groups = seeds.select(*keys).distinct()
    fallback = (groups.crossJoin(popularity)
        .join(seen, keys + ["candidate_itemid"], "left_anti")
        .join(cf.select(*keys, "candidate_itemid"),
              keys + ["candidate_itemid"], "left_anti"))
    fallback_order = Window.partitionBy(*keys).orderBy(
        F.desc("popularity_score"), F.asc("candidate_itemid"))
    fallback = fallback.withColumn("fallback_rank", F.row_number().over(fallback_order))
    cf_counts = cf.groupBy(*keys).agg(F.count("*").alias("cf_count"))
    fallback = (fallback.join(cf_counts, keys, "left")
        .fillna(0, subset=["cf_count"])
        .where(F.col("fallback_rank") <= top_k - F.col("cf_count"))
        .withColumn("rank", F.col("cf_count") + F.col("fallback_rank"))
        .withColumn("source", F.lit("popular"))
        .withColumn("rec_score", F.lit(None).cast("double"))
        .withColumn("contributing_seed_itemids",
                    F.array().cast(T.ArrayType(T.LongType()))))

    combined = (cf.select(*RECOMMENDATION_COLUMNS)
                .unionByName(fallback.select(*RECOMMENDATION_COLUMNS)))
    # count() and arithmetic around the fallback rank widen this column to long.
    # Keep the persisted contract stable across micro-batches.
    return combined.withColumn("rank", F.col("rank").cast("int")).orderBy(
        *keys, "rank")


def build_model(args):
    """Build deterministic Item-CF and popularity artifacts with Spark."""
    spark = (SparkSession.builder.appName("RetailRocketBuildItemCF")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        events = (spark.read.option("header", True).schema(EVENT_SCHEMA).csv(args.csv)
                  .where((F.col("timestamp") > 0)
                         & (F.col("timestamp") < args.history_end_ms)
                         & F.col("event").isin("view", "addtocart", "transaction")))
        if args.sample_percent < 100:
            events = events.where(
                F.pmod(F.xxhash64("visitorid"), F.lit(100)) < args.sample_percent)

        user_item = (events.withColumn("weight", weight_expression())
                     .groupBy("visitorid", "itemid")
                     .agg(F.max("weight").alias("weight")))
        active = (user_item.groupBy("visitorid").count()
                  .where((F.col("count") >= 2) & (F.col("count") <= 100))
                  .select("visitorid"))
        ui = user_item.join(active, "visitorid").persist()

        left, right = ui.alias("left"), ui.alias("right")
        pairs = (left.join(right, "visitorid")
                 .where(F.col("left.itemid") != F.col("right.itemid"))
                 .groupBy(F.col("left.itemid").alias("seed_itemid"),
                          F.col("right.itemid").alias("candidate_itemid"))
                 .agg(F.sum(F.least("left.weight", "right.weight")).alias("co_weight")))
        item_popularity = ui.groupBy("itemid").agg(F.sum("weight").alias("popularity_score"))
        seed_pop = item_popularity.select(
            F.col("itemid").alias("seed_itemid"), F.col("popularity_score").alias("seed_pop"))
        candidate_pop = item_popularity.select(
            F.col("itemid").alias("candidate_itemid"),
            F.col("popularity_score").alias("candidate_pop"))
        similarity = (pairs.join(seed_pop, "seed_itemid")
                      .join(candidate_pop, "candidate_itemid")
                      .withColumn("similarity", F.col("co_weight") /
                                  F.sqrt(F.col("seed_pop") * F.col("candidate_pop"))))
        neighbor_order = Window.partitionBy("seed_itemid").orderBy(
            F.desc("similarity"), F.asc("candidate_itemid"))
        similarity = (similarity.withColumn("neighbor_rank", F.row_number().over(neighbor_order))
                      .where(F.col("neighbor_rank") <= args.max_neighbors)
                      .select("seed_itemid", "candidate_itemid", "similarity"))
        popularity = (item_popularity.orderBy(F.desc("popularity_score"), F.asc("itemid"))
                      .limit(args.max_popular)
                      .select(F.col("itemid").alias("candidate_itemid"),
                              F.col("popularity_score").cast("double")))

        root = Path(args.output)
        root.mkdir(parents=True, exist_ok=True)
        similarity.write.mode("overwrite").parquet(str(root / "item_similarity"))
        popularity.write.mode("overwrite").parquet(str(root / "popularity"))
        manifest = {
            "schema_version": 1,
            "algorithm_version": "item-cf-v1",
            "model_version": root.name,
            "history_end_ms": args.history_end_ms,
            "sample_percent": args.sample_percent,
            "sampling": "pmod(xxhash64(visitorid), 100) < sample_percent",
            "weights": {"view": 1, "addtocart": 5, "transaction": 10},
            "max_neighbors": args.max_neighbors,
            "max_popular": args.max_popular,
            "user_item_rows": ui.count(),
            "similarity_rows": similarity.count(),
            "popularity_rows": popularity.count(),
        }
        temporary = root / ".manifest.json.tmp"
        temporary.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        temporary.replace(root / "manifest.json")
        print(json.dumps(manifest, indent=2), flush=True)
        ui.unpersist()
    finally:
        spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-model", action="store_true", required=True)
    parser.add_argument("--csv", default="/opt/data/events.csv")
    parser.add_argument("--output", default="/opt/project/models/item-cf-v1")
    parser.add_argument("--history-end-ms", type=int, required=True)
    parser.add_argument("--sample-percent", type=int, default=5, choices=range(1, 101))
    parser.add_argument("--max-neighbors", type=int, default=50)
    parser.add_argument("--max-popular", type=int, default=1000)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    args = parser.parse_args()
    if args.max_neighbors <= 0 or args.max_popular <= 0:
        parser.error("--max-neighbors and --max-popular must be positive")
    return args


if __name__ == "__main__":
    build_model(parse_args())
