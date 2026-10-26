from pathlib import Path
import nbformat

root = Path(__file__).resolve().parents[1]
path = root / "Part_A" / "A2_Recommender_Systems_Spark_Streaming_v2.ipynb"
nb = nbformat.read(path, as_version=4)
for cell in nb.cells:
    if cell.cell_type == "code" and cell.source.lstrip().startswith("# 5.0  Configuration"):
        cell.source = cell.source.replace("import os, json, time, shutil, logging", "import os, json, time, shutil, logging, subprocess")
        cell.source = cell.source.replace(
            'KAFKA_TOPIC, KAFKA_STARTING = "retail-rocket-events", "earliest"',
            'KAFKA_TOPIC, KAFKA_STARTING = "retail-rocket-events", "earliest"\n'
            'SPARK_KAFKA_PACKAGE_VERSION = os.environ.get("SPARK_KAFKA_PACKAGE_VERSION", "4.0.1")\n'
            'SYNTHETIC_SESSIONS = int(os.environ.get("SYNTHETIC_SESSIONS", "3000"))')
        cell.source = cell.source.replace(
            'WORK_DIR = Path("work") / DATA_MODE',
            'WORK_DIR = Path("work") / DATA_MODE\n'
            'if DATA_MODE == "synthetic": SAMPLE_PATH = WORK_DIR / "events_sample.csv"')
        cell.source = cell.source.replace(
            'generate_retailrocket_like(SAMPLE_PATH)',
            'generate_retailrocket_like(SAMPLE_PATH, n_sessions=SYNTHETIC_SESSIONS)')
    if cell.cell_type == "code" and cell.source.lstrip().startswith("# 5.1  Spark session"):
        cell.source = cell.source.replace(
            'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1',
            'org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_KAFKA_PACKAGE_VERSION}')
        cell.source = cell.source.replace(
            '"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_KAFKA_PACKAGE_VERSION}"',
            'f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_KAFKA_PACKAGE_VERSION}"')
        while 'ff"org.apache.spark:spark-sql-kafka-0-10_2.13:' in cell.source:
            cell.source = cell.source.replace('ff"org.apache.spark:spark-sql-kafka-0-10_2.13:', 'f"org.apache.spark:spark-sql-kafka-0-10_2.13:')
    if cell.cell_type == "code" and cell.source.lstrip().startswith("query = (intent.writeStream") and "def reset_kafka_topic" not in cell.source:
        reset = '''def reset_kafka_topic():
    command = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh"]
    subprocess.run(command + ["--delete", "--if-exists", "--topic", KAFKA_TOPIC, "--bootstrap-server", "localhost:9092"], check=True)
    time.sleep(2)
    subprocess.run(command + ["--create", "--topic", KAFKA_TOPIC, "--partitions", "2", "--replication-factor", "1", "--bootstrap-server", "localhost:9092"], check=True)
    result = subprocess.run(command + ["--describe", "--topic", KAFKA_TOPIC, "--bootstrap-server", "localhost:9092"], text=True, capture_output=True, check=True)
    print(result.stdout.strip())
if SOURCE == "kafka" and os.environ.get("RESET_KAFKA_TOPIC", "true").lower() == "true":
    reset_kafka_topic()
'''
        cell.source = reset + "\n" + cell.source
nbformat.write(nb, path)
print(path)
