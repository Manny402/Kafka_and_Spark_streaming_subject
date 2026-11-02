"""Category metadata preparation; no interaction history or model training.

Spark imports stay lazy so the desktop UI can load JSON without Java/PySpark.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import uuid

ALGORITHM = "category-tree-v1"


def read_category_tree(tree_path):
    parents = {}
    with Path(tree_path).open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            category = int(row["categoryid"])
            parent = int(row["parentid"]) if row["parentid"].strip() else None
            if category in parents:
                raise ValueError(f"Duplicate category: {category}")
            parents[category] = parent
    if not parents:
        raise ValueError("Category tree is empty")
    for category, parent in parents.items():
        if parent is not None and parent not in parents:
            raise ValueError(f"Missing parent {parent} for category {category}")
    for category in parents:
        seen, current = set(), category
        while current is not None:
            if current in seen:
                raise ValueError(f"Category cycle at {current}")
            seen.add(current)
            current = parents[current]
    return parents


def category_paths(parents):
    paths = {}
    for category in parents:
        path, current = [], category
        while current is not None:
            path.append(current)
            current = parents[current]
        paths[category] = tuple(reversed(path))
    return paths


def category_similarity(left, right, paths):
    if left == right:
        return 1.0
    a, b = paths[left], paths[right]
    common = 0
    for x, y in zip(a, b):
        if x != y:
            break
        common += 1
    if common <= 1:  # Only a root in common, or different roots.
        return 0.0
    return 2.0 * (common - 1) / (len(a) + len(b) - 2)


def build_category_ancestors(parents):
    return [(category, ancestor, len(path) - 1, depth, len(path) - 1 - depth)
            for category, path in sorted(category_paths(parents).items())
            for depth, ancestor in enumerate(path)]


def build_category_similarity(parents):
    paths = category_paths(parents)
    return [(a, b, score) for a in sorted(paths) for b in sorted(paths)
            if (score := category_similarity(a, b, paths)) > 0]


def source_hashes(paths):
    hashes = {}
    for path in paths:
        digest = hashlib.sha256()
        with Path(path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                digest.update(chunk)
        hashes[Path(path).name] = digest.hexdigest()
    return hashes


def catalog_location(catalog_path):
    root = Path(catalog_path)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("algorithm_version") != ALGORITHM:
        raise ValueError("Unsupported category catalog")
    relative = Path(manifest["artifact_path"])
    artifact = (root / relative).resolve()
    if relative.is_absolute() or not artifact.is_relative_to(root.resolve()):
        raise ValueError("Invalid catalog artifact path")
    return artifact, manifest


def load_ui_catalog(catalog_path):
    artifact, manifest = catalog_location(catalog_path)
    groups = json.loads((artifact / "items_by_category.json").read_text(encoding="utf-8"))
    return {int(category): items for category, items in groups.items()}, manifest


def load_catalog(spark, catalog_path):
    artifact, manifest = catalog_location(catalog_path)
    items = spark.read.parquet(str(artifact / "item_categories"))
    similarity = spark.read.parquet(str(artifact / "category_similarity"))
    if not {"itemid", "categoryid"}.issubset(items.columns):
        raise ValueError("Invalid item-category schema")
    if not {"seed_categoryid", "candidate_categoryid", "category_similarity"}.issubset(similarity.columns):
        raise ValueError("Invalid category similarity schema")
    return items, similarity, manifest


def read_item_categories(spark, property_paths):
    from pyspark.sql import functions as F, types as T
    schema = T.StructType([
        T.StructField("timestamp", T.LongType()), T.StructField("itemid", T.LongType()),
        T.StructField("property", T.StringType()), T.StructField("value", T.StringType()),
    ])
    rows = (spark.read.option("header", True).schema(schema)
            .csv([str(path) for path in property_paths])
            .where((F.col("property") == "categoryid") & (F.col("timestamp") > 0)
                   & (F.col("itemid") >= 0)))
    latest = rows.groupBy("itemid").agg(F.max("timestamp").alias("timestamp"))
    latest_rows = rows.join(latest, ["itemid", "timestamp"], "inner")
    unique = latest_rows.groupBy("itemid").agg(
        F.collect_set(F.coalesce(F.col("value"), F.lit(""))).alias("values"))
    valid = (unique.where((F.size("values") == 1) & F.col("values")[0].rlike(r"^\d+$"))
             .select("itemid", F.col("values")[0].cast("long").alias("categoryid")))
    return valid, unique.where(F.size("values") > 1)


def build_catalog(spark, data_root, output, force=False):
    from pyspark.sql import functions as F
    data_root, output = Path(data_root), Path(output)
    tree_path = data_root / "category_tree.csv"
    properties = [data_root / f"item_properties_part{part}.csv" for part in (1, 2)]
    hashes = source_hashes([tree_path, *properties])
    version = "category-" + hashlib.sha256(json.dumps({
        "sources": hashes, "algorithm": ALGORITHM,
        "snapshot_policy": "latest_category_drop_conflicts-v1",
    }, sort_keys=True).encode()).hexdigest()[:16]
    if not force and (output / "manifest.json").is_file():
        artifact, manifest = catalog_location(output)
        if (manifest["catalog_version"] == version
                and all((artifact / name / "_SUCCESS").is_file()
                        for name in ("item_categories", "category_ancestors", "category_similarity"))
                and (artifact / "items_by_category.json").is_file()):
            return manifest
    parents = read_category_tree(tree_path)
    items, conflicts = read_item_categories(spark, properties)
    categories = spark.createDataFrame([(key,) for key in parents], "categoryid LONG")
    items = items.join(categories, "categoryid", "inner").select("itemid", "categoryid").persist()
    try:
        relative = Path("artifacts") / f"{version}-{uuid.uuid4().hex[:8]}"
        artifact = output / relative
        items.write.mode("errorifexists").parquet(str(artifact / "item_categories"))
        ancestors = spark.createDataFrame(build_category_ancestors(parents),
            "categoryid LONG, ancestorid LONG, depth INT, ancestor_depth INT, distance_to_ancestor INT")
        ancestors.write.mode("errorifexists").parquet(str(artifact / "category_ancestors"))
        similarity = spark.createDataFrame(build_category_similarity(parents),
            "seed_categoryid LONG, candidate_categoryid LONG, category_similarity DOUBLE")
        similarity.write.mode("errorifexists").parquet(str(artifact / "category_similarity"))
        groups = {str(row.categoryid): row.items for row in items.groupBy("categoryid")
                  .agg(F.sort_array(F.collect_list("itemid")).alias("items")).collect()}
        (artifact / "items_by_category.json").write_text(json.dumps(groups), encoding="utf-8")
        manifest = {
            "schema_version": 1, "algorithm_version": ALGORITHM, "catalog_version": version,
            "artifact_path": relative.as_posix(), "source_sha256": hashes,
            "snapshot_policy": "latest_category_drop_conflicts-v1",
            "item_rows": items.count(), "category_rows": len(parents),
            "conflicting_items": conflicts.count(), "similarity_rows": similarity.count(),
        }
        output.mkdir(parents=True, exist_ok=True)
        temporary = output / f".manifest-{uuid.uuid4().hex}.tmp"
        temporary.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        temporary.replace(output / "manifest.json")
        return manifest
    finally:
        items.unpersist()


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true", required=True)
    parser.add_argument("--data-root", type=Path, default=Path("/opt/data"))
    parser.add_argument("--output", type=Path, default=Path("/opt/project/catalog/category-v1"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("RetailCategoryCatalog")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(build_catalog(spark, args.data_root, args.output, args.force), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
