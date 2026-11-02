"""Rank products from realtime intent and category-tree metadata only."""
from pyspark.sql import Window, functions as F

RECOMMENDATION_COLUMNS = [
    "visitorid", "window_start", "window_end", "candidate_itemid", "rank",
    "source", "rec_score", "candidate_categoryid", "category_similarity",
    "contributing_seed_itemids",
]


def weight_expression(event_column="event"):
    return (F.when(F.col(event_column) == "view", 1)
            .when(F.col(event_column) == "addtocart", 5)
            .when(F.col(event_column) == "transaction", 10).otherwise(0))


def eligible_expression(policy="extended", threshold=5):
    base = (F.col("transactions") == 0) & (F.col("intent_score") >= threshold)
    if policy == "extended":
        return base
    if policy == "strict_cart":
        return base & (F.col("carts") > 0)
    raise ValueError(f"Unsupported recommendation policy: {policy}")


def generate_recommendations(context, item_categories, category_similarity,
                             threshold=5, top_k=3, policy="extended"):
    """Sum seed scores per category, then rank unique unseen candidates.

    Category-local top-K pruning is exact because items in one category have
    identical scores. There is no item-pair matrix or historical popularity.
    """
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    keys = ["visitorid", "window_start", "window_end"]
    seeds = (context.where(eligible_expression(policy, threshold))
             .join(item_categories, "itemid", "inner")
             .select(*keys, F.col("itemid").alias("seed_itemid"), "intent_score",
                     F.col("categoryid").alias("seed_categoryid")))
    category_scores = (seeds.join(category_similarity, "seed_categoryid", "inner")
        .groupBy(*keys, "candidate_categoryid")
        .agg(F.sum(F.col("intent_score") * F.col("category_similarity")).alias("rec_score"),
             F.max("category_similarity").alias("category_similarity"),
             F.sort_array(F.collect_set("seed_itemid")).alias("contributing_seed_itemids")))
    candidates = item_categories.select(
        F.col("itemid").alias("candidate_itemid"),
        F.col("categoryid").alias("candidate_categoryid"))
    seen = context.select(*keys, F.col("itemid").alias("candidate_itemid")).distinct()
    scored = (category_scores.join(candidates, "candidate_categoryid", "inner")
              .join(seen, keys + ["candidate_itemid"], "left_anti"))
    local_order = Window.partitionBy(*keys, "candidate_categoryid").orderBy("candidate_itemid")
    pruned = (scored.withColumn("_category_rank", F.row_number().over(local_order))
              .where(F.col("_category_rank") <= top_k).drop("_category_rank"))
    order = Window.partitionBy(*keys).orderBy(F.desc("rec_score"), F.asc("candidate_itemid"))
    return (pruned.withColumn("rank", F.row_number().over(order).cast("int"))
            .where(F.col("rank") <= top_k).withColumn("source", F.lit("category_tree"))
            .select(*RECOMMENDATION_COLUMNS).orderBy(*keys, "rank"))
