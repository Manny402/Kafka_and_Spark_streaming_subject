from pathlib import Path
import copy
import nbformat

root = Path(__file__).resolve().parents[1]
source = root / "Part_A" / "A2_Recommender_Systems_Spark_Streaming.ipynb"
target = root / "Part_A" / "A2_Recommender_Systems_Spark_Streaming_v2.ipynb"
nb = copy.deepcopy(nbformat.read(source, as_version=4))

def find(marker):
    hits = [i for i, c in enumerate(nb.cells) if c.source.lstrip().startswith(marker)]
    assert len(hits) == 1, (marker, hits)
    return hits[0]

def replace(marker, code):
    nb.cells[find(marker)].source = code.strip() + "\n"

for cell in nb.cells:
    if cell.cell_type == "code":
        cell.outputs, cell.execution_count = [], None

replace("# 5.0  Configuration", '''
# 5.0  Configuration
import os, json, time, shutil, logging
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()
from pathlib import Path
import numpy as np
import pandas as pd

DATA_PATH, SAMPLE_PATH = Path("data/events.csv"), Path("data/events_sample.csv")
DATA_MODE = os.environ.get("DATA_MODE", "synthetic").lower()
if DATA_MODE not in {"synthetic", "real"}:
    raise ValueError("DATA_MODE must be synthetic or real")
SOURCE = os.environ.get("SOURCE", "file").lower()
if SOURCE not in {"file", "kafka"}:
    raise ValueError("SOURCE must be file or kafka")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
SHUFFLE_PARTS = int(os.environ.get("SHUFFLE_PARTITIONS", "16"))
DRIVER_MEMORY = os.environ.get("DRIVER_MEMORY", "4g")
STREAM_HOURS = int(os.environ.get("STREAM_HOURS", "3"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
KAFKA_TOPIC, KAFKA_STARTING = "retail-rocket-events", "earliest"

WORK_DIR = Path("work") / DATA_MODE
STREAM_DIR, CKPT_DIR = WORK_DIR / "stream_in", WORK_DIR / "checkpoints"
OUT_DIR, LOG_DIR, SENT_LOG_DIR = WORK_DIR / "recommendations_parquet", WORK_DIR / "logs", WORK_DIR / "sent_log"
SLICE_MINUTES = 15
WINDOW, SLIDE, WATERMARK = "10 minutes", "5 minutes", "5 minutes"
WEIGHTS = {"view": 1, "addtocart": 5, "transaction": 10}
INTENT_THRESHOLD, TOP_K, NEIGHBOURS, COOLDOWN_MIN = 5, 5, 20, 30

shutil.rmtree(WORK_DIR, ignore_errors=True)
for directory in (STREAM_DIR, CKPT_DIR, OUT_DIR, LOG_DIR, SENT_LOG_DIR):
    directory.mkdir(parents=True, exist_ok=True)
print(f"DATA MODE: {DATA_MODE.upper()} | SOURCE: {SOURCE.upper()} | working dir: {WORK_DIR.resolve()}")
''')

session = nb.cells[find("# 5.1  Spark session")]
session.source = session.source.replace('.master("local[2]")', '.master(SPARK_MASTER)')
session.source = session.source.replace('.config("spark.sql.shuffle.partitions", "4")',
                                        '.config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))\n           .config("spark.driver.memory", DRIVER_MEMORY)')

load = nb.cells[find("def generate_retailrocket_like")].source
old = '''if DATA_PATH.exists():
    src = DATA_PATH
else:
    if not SAMPLE_PATH.exists():
        generate_retailrocket_like(SAMPLE_PATH)
    src = SAMPLE_PATH
print("using:", src)'''
new = '''if DATA_MODE == "real":
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"{DATA_PATH} missing while DATA_MODE=real")
    src = DATA_PATH
else:
    if not SAMPLE_PATH.exists():
        generate_retailrocket_like(SAMPLE_PATH)
    src = SAMPLE_PATH
print(f"DATA MODE: {DATA_MODE.upper()} | using: {src}")'''
assert old in load
nb.cells[find("def generate_retailrocket_like")].source = load.replace(old, new)

model = nb.cells[find("weight_col =")]
model.source = model.source.replace('pop = history.withColumn("w", weight_col).groupBy("itemid", "visitorid").agg(F.max("w").alias("w")) \\\n             .groupBy("itemid").agg(F.sum("w").alias("pop"))',
'''pop_sim = vi.groupBy("itemid").agg(F.sum("w").alias("pop"))
pop_all = (history.withColumn("w", weight_col).groupBy("itemid", "visitorid")
           .agg(F.max("w").alias("w")).groupBy("itemid").agg(F.sum("w").alias("pop")))''')
model.source = model.source.replace('.join(pop.select(F.col("itemid").alias("item_a"), F.col("pop").alias("pop_a")), "item_a")',
                                    '.join(pop_sim.select(F.col("itemid").alias("item_a"), F.col("pop").alias("pop_a")), "item_a")')
model.source = model.source.replace('.join(pop.select(F.col("itemid").alias("item_b"), F.col("pop").alias("pop_b")), "item_b")',
                                    '.join(pop_sim.select(F.col("itemid").alias("item_b"), F.col("pop").alias("pop_b")), "item_b")')
model.source = model.source.replace('popular = (pop.orderBy(F.desc("pop")).limit(50)', 'popular = (pop_all.orderBy(F.desc("pop")).limit(50)')
model.source = model.source.replace('print(f"item_sim rows:', 'max_similarity = item_sim.agg(F.max("similarity")).first()[0]\nassert max_similarity is None or max_similarity <= 1.0, max_similarity\nprint(f"item_sim rows:')
nb.cells[find("weight_col =")].source = model.source

extract = nb.cells[find("# ---- EXTRACT")]
extract.source = extract.source.replace('.option("startingOffsets", "latest")', '.option("startingOffsets", KAFKA_STARTING)')

for cell in nb.cells:
    if cell.cell_type == "markdown":
        cell.source = cell.source.replace("Fault tolerance & exactly-once for recommendations", "Fault tolerance & idempotent recommendations")
        cell.source = cell.source.replace("cart-abandonment risk", "cart without a purchase observed yet in this window")
        cell.source = cell.source.replace("producer.py", "kafka_producer.py")
        cell.source = cell.source.replace('option("startingOffsets", "latest")', 'option("startingOffsets", "earliest")')
        cell.source = cell.source.replace("**exactly-once** recommendation output", "idempotent recommendation output")

nbformat.write(nb, target)
print(target)
