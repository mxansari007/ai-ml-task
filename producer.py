import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Kafka Producer").getOrCreate()

# Read Parquet file created by main.py
parquet_path = "data/final_df.parquet"
df = spark.read.parquet(parquet_path)

# Kafka configurations
bootstrap_servers = 'localhost:9092'
topic_name = 'delta_topic'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  # ✅ Convert non-serializable types
)

# Produce messages to Kafka
for row in df.collect():
    # Safely handle None values to prevent .strftime() error
    record = {
        "signal_date": row["signal_date"].strftime("%Y-%m-%d") if row["signal_date"] else None,
        "signal_ts": row["signal_ts"].strftime("%Y-%m-%dT%H:%M:%S") if row["signal_ts"] else None,
        "create_date": row["create_date"].strftime("%Y-%m-%d") if row["create_date"] else None,
        "create_ts": row["create_ts"].strftime("%Y-%m-%dT%H:%M:%S") if row["create_ts"] else None,
        "signals": row["signals"]
    }

    # Send record to Kafka
    producer.send(topic_name, value=record)

producer.flush()
print(f"✅ Data successfully produced to topic '{topic_name}'")
