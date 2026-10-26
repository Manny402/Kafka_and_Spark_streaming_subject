from pathlib import Path
import nbformat

root = Path(__file__).resolve().parents[1]
path = root / "Part_A" / "A2_Recommender_Systems_Spark_Streaming_v2.ipynb"
nb = nbformat.read(path, as_version=4)
for cell in nb.cells:
    if cell.cell_type == "code" and "def reset_kafka_topic" in cell.source:
        tail = cell.source[cell.source.index("query = (intent.writeStream"):]
        cell.source = '''from kafka.admin import KafkaAdminClient, NewTopic

def reset_kafka_topic():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="retail-rocket-notebook")
    try:
        admin.delete_topics([KAFKA_TOPIC])
        time.sleep(2)
    except Exception:
        pass
    admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=2, replication_factor=1)])
    description = admin.describe_topics([KAFKA_TOPIC])[0]
    print("Kafka topic evidence:", KAFKA_TOPIC, "partitions=", len(description["partitions"]))
    admin.close()

if SOURCE == "kafka" and os.environ.get("RESET_KAFKA_TOPIC", "true").lower() == "true":
    reset_kafka_topic()
''' + "\n" + tail
nbformat.write(nb, path)
print(path)
