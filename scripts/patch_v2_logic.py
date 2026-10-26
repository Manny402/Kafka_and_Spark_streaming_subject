from pathlib import Path
import nbformat

root=Path(__file__).resolve().parents[1]
path=root/"Part_A"/"A2_Recommender_Systems_Spark_Streaming_v2.ipynb"
nb=nbformat.read(path,as_version=4)
def find(m):
    x=[i for i,c in enumerate(nb.cells) if c.source.lstrip().startswith(m)]
    assert len(x)==1,(m,x)
    return x[0]
def put(m,s): nb.cells[find(m)].source=s.strip()+"\n"

session=nb.cells[find("# 5.1  Spark session")]
session.source=session.source.replace("LongType, IntegerType, StringType","LongType, IntegerType, StringType, TimestampType")

put("# ---- EXTRACT",r'''
# ---- EXTRACT ----------------------------------------------------------------
if SOURCE == "kafka":
    raw=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP)
         .option("subscribe",KAFKA_TOPIC).option("startingOffsets",KAFKA_STARTING)
         .option("maxOffsetsPerTrigger",5000).load().selectExpr("CAST(value AS STRING) AS value"))
else:
    raw=spark.readStream.format("text").option("maxFilesPerTrigger",1).load(str(STREAM_DIR))
parsed=(raw.select(F.from_json("value",EVENT_SCHEMA).alias("e")).select("e.*")
 .filter(F.col("visitorid").isNotNull()&F.col("event").isin(*WEIGHTS))
 .withColumn("event_time",F.timestamp_millis("timestamp")).withColumn("weight",weight_col)
 .withColumn("is_view",(F.col("event")=="view").cast("int"))
 .withColumn("is_cart",(F.col("event")=="addtocart").cast("int"))
 .withColumn("is_txn",(F.col("event")=="transaction").cast("int")))
intent=(parsed.withWatermark("event_time",WATERMARK)
 .groupBy(F.window("event_time",WINDOW,SLIDE).alias("window"),"visitorid")
 .agg(F.collect_list(F.struct("itemid","weight","is_view","is_cart","is_txn")).alias("items"),
      F.max("event_time").alias("last_event_time")))
assert intent.isStreaming
intent.printSchema()
''')

put("# ---- structured logger",r'''
# ---- structured logger: console + file ---------------------------------------
logger=logging.getLogger("rec_engine"); logger.handlers.clear(); logger.setLevel(logging.INFO); logger.propagate=False
fh=logging.FileHandler(LOG_DIR/"recommendation_batches.log"); fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s")); logger.addHandler(fh)
batch_results,batch_meta={},[]
SENT_SCHEMA=StructType([StructField("visitorid",IntegerType()),StructField("seed_item",IntegerType()),
 StructField("rec_item",IntegerType()),StructField("sent_event_time",TimestampType()),StructField("batch_id",IntegerType())])
COOL=F.expr(f"INTERVAL {COOLDOWN_MIN} MINUTES")
def read_sent_log(bid):
    if not any(SENT_LOG_DIR.glob("batch_id=*")): return spark.createDataFrame([],SENT_SCHEMA)
    return spark.read.schema(SENT_SCHEMA).parquet(str(SENT_LOG_DIR)).filter(F.col("batch_id")!=int(bid))
def drop_recently_sent(df,sl):
    r=sl.select(F.col("visitorid").alias("r_v"),F.col("rec_item").alias("r_i"),F.col("sent_event_time").alias("r_t")).distinct()
    return df.join(r,(df.visitorid==r.r_v)&(df.rec_item==r.r_i)&(df.window_end<r.r_t+COOL),"left_anti")
def recommend_batch(batch_df,bid):
    if batch_df.isEmpty(): return
    t0=time.perf_counter(); b=batch_df.persist(); changed=b.count()
    ex=b.select("visitorid","window",F.explode("items").alias("it")).select("visitorid","window","it.*")
    pi=ex.groupBy("visitorid","window","itemid").agg(F.sum("weight").alias("intent_score"),F.sum("is_cart").alias("carts"),F.sum("is_txn").alias("txns"))
    vs=pi.groupBy("visitorid","window").agg(F.sum("intent_score").alias("visitor_score"))
    seen=ex.select("visitorid",F.col("window.end").alias("window_end"),F.col("itemid").alias("rec_item")).distinct()
    paid=pi.filter("txns>0").select("visitorid","itemid").distinct()
    latest=Window.partitionBy("visitorid","itemid").orderBy(F.col("window.end").desc())
    raw=(pi.join(vs,["visitorid","window"]).filter(F.col("intent_score")>=INTENT_THRESHOLD).join(paid,["visitorid","itemid"],"left_anti")
     .withColumn("rn",F.row_number().over(latest)).filter("rn=1").select("visitorid",F.col("itemid").alias("seed_item"),"intent_score","visitor_score","carts",F.col("window.end").alias("window_end")))
    sl=read_sent_log(bid); old=sl.select(F.col("visitorid").alias("r_v"),F.col("seed_item").alias("r_i"),F.col("sent_event_time").alias("r_t")).distinct()
    seeds=raw.join(old,(raw.visitorid==old.r_v)&(raw.seed_item==old.r_i)&(raw.window_end<old.r_t+COOL),"left_anti")
    cand=(seeds.join(F.broadcast(item_sim),seeds.seed_item==item_sim.item_a).select("visitorid",F.col("item_b").alias("rec_item"),"window_end",(F.col("similarity")*F.col("intent_score")).alias("s")).join(seen,["visitorid","window_end","rec_item"],"left_anti"))
    cand=drop_recently_sent(cand,sl).groupBy("visitorid","rec_item").agg(F.sum("s").alias("rec_score"),F.max("window_end").alias("window_end"))
    rank=Window.partitionBy("visitorid").orderBy(F.desc("rec_score"),"rec_item")
    cf=cand.withColumn("rank",F.row_number().over(rank)).filter(F.col("rank")<=TOP_K).withColumn("source",F.lit("item_cf"))
    ctx=seeds.groupBy("visitorid").agg(F.collect_set("seed_item").alias("seed_items"),F.max("intent_score").alias("max_intent"),F.max("visitor_score").alias("visitor_score"),F.max("carts").alias("carts"),F.max("window_end").alias("window_end"))
    miss=ctx.select("visitorid","window_end").join(cf.select("visitorid").distinct(),"visitorid","left_anti")
    fb=drop_recently_sent(miss.crossJoin(F.broadcast(popular)).join(seen,["visitorid","window_end","rec_item"],"left_anti"),sl)
    fb=fb.withColumn("rank",F.row_number().over(Window.partitionBy("visitorid").orderBy(F.desc("pop_score"),"rec_item"))).filter(F.col("rank")<=TOP_K).select("visitorid","rec_item",F.lit(0.0).alias("rec_score"),"rank",F.lit("popular").alias("source"))
    recs=(cf.select("visitorid","rec_item","rec_score","rank","source").unionByName(fb).join(ctx,"visitorid").withColumn("trigger_reason",F.when(F.col("carts")>0,"cart_no_purchase").otherwise("heavy_browsing")).withColumn("batch_id",F.lit(int(bid))).withColumn("generated_at",F.current_timestamp()).withColumn("event_date",F.to_date("window_end")).drop("carts").persist())
    n=recs.count()
    if n:
      recs.write.mode("overwrite").partitionBy("event_date","batch_id").parquet(str(OUT_DIR))
      sent=(recs.select("visitorid","rec_item","window_end","batch_id").join(seeds.select("visitorid","seed_item"),"visitorid").select("visitorid","seed_item","rec_item",F.col("window_end").alias("sent_event_time"),"batch_id").distinct())
      sent.write.mode("overwrite").partitionBy("batch_id").parquet(str(SENT_LOG_DIR))
    pdf=recs.orderBy("visitorid","rank").toPandas(); batch_results[bid]=pdf
    meta={"batch_id":bid,"changed_window_rows":changed,"seeds_raw":raw.count(),"seeds_after_cooldown":seeds.count(),"recommendations":n,"foreachBatch_ms":round((time.perf_counter()-t0)*1000)}
    batch_meta.append(meta); logger.info(json.dumps(meta)); recs.unpersist(); b.unpersist()
''')
sink=nb.cells[find("# The curated sink")]
sink.source=sink.source.replace('out.select("visitorid", "seed_items", "max_intent", "trigger_reason"', 'out.select("visitorid", "seed_items", "max_intent", "visitor_score", "trigger_reason"')
sink.source+=r'''
d=out.select("visitorid","rec_item","window_end").distinct().toPandas().sort_values(["visitorid","rec_item","window_end"])
if not d.empty:
 d["window_end"]=pd.to_datetime(d["window_end"],utc=True)
 gap=d.groupby(["visitorid","rec_item"])["window_end"].diff().dropna()
 assert (gap>=pd.Timedelta(minutes=COOLDOWN_MIN)).all()
print("cooldown check passed")
'''
put("def publish_slice",r'''
def publish_slice(idx, frame):
    path, tmp = STREAM_DIR / f"slice_{idx:03d}.json", STREAM_DIR / f".slice_{idx:03d}.tmp"
    tmp.write_text("\n".join(to_payload(r) for r in frame.itertuples()) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path
producer = None
def publish(idx, frame):
    global producer
    if SOURCE == "file": return publish_slice(idx, frame)
    from kafka_producer import make_producer, publish_df
    if producer is None: producer = make_producer(KAFKA_BOOTSTRAP)
    return publish_df(producer, KAFKA_TOPIC, frame)
def print_kafka_end_offsets():
    if SOURCE == "kafka" and query.lastProgress and query.lastProgress.get("sources"):
        print("Kafka endOffset:", query.lastProgress["sources"][0].get("endOffset", {}))
shown = set()
for idx, g in slices:
    publish(idx, g); query.processAllAvailable()
    p = query.lastProgress or {}; wm = (p.get("eventTime") or {}).get("watermark", "-")
    print(f"slice {idx:02d} | {len(g):>4} events | watermark {wm}")
    print_kafka_end_offsets(); show_new_batches(shown)
if producer is not None: producer.close()
''')
late = nb.cells[find("max_seen =")]
late.source = late.source.replace("publish_slice(998, late)", "publish(998, late)")
restart = nb.cells[find("last_batch =")]
restart.source = restart.source.replace("publish_slice(999, extra)", "publish(999, extra)")
nbformat.write(nb,path)
print(path)
