from kafka import KafkaConsumer
import json

# Kafka topic name (ensure it matches the producer's topic name)
topic_name = "delta_topic"  # ✅ Corrected to match the producer's topic

# Kafka consumer setup
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # Read messages from the beginning
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None  # ✅ Safely deserialize JSON
)

print(f"✅ Consuming messages from Kafka topic '{topic_name}'...")
for message in consumer:
    try:
        if message.value:
            print(f"🔥 Consumed record: {json.dumps(message.value, indent=4)}")
        else:
            print("⚠️ Skipped empty message.")
    except json.JSONDecodeError as e:
        print(f"❌ JSON decoding error: {e}")
    except Exception as e:
        print(f"⚠️ Error while processing message: {e}")
