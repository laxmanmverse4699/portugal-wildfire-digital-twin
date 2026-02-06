from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
import time
import os


def wait_for_ai_message() -> int | None:
    """Wait for any message on the 'ai' topic"""
    print("Waiting for message on 'ai' topic...")

    from kafka import TopicPartition

    # Offset tracking file
    offset_file = "/tmp/ai_consumer_offset.txt"

    # Create consumer with retry
    consumer = None
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=["kafka:9092"],
                value_deserializer=lambda x: x.decode("utf-8") if x else None,
                consumer_timeout_ms=300000,
            )
            break
        except Exception as e:
            print(f"Failed to connect to Kafka (attempt {attempt+1}/10): {e}")
            time.sleep(5)

    if consumer is None:
        raise Exception("Could not connect to Kafka after 10 attempts")

    print("✅ Kafka consumer created successfully")

    # Assign partition 0 of 'ai' topic
    tp = TopicPartition("ai", 0)
    consumer.assign([tp])
    
    # Seek to stored offset or beginning
    start_offset = 0
    if os.path.exists(offset_file):
        try:
            with open(offset_file, "r") as f:
                start_offset = int(f.read().strip())
        except:
            start_offset = 0
    
    consumer.seek(tp, start_offset)
    print(f"🎯 Assigned to partition {tp}, seeking to offset {start_offset}.")

    try:
        poll_count = 0
        while True:
            poll_count += 1
            if poll_count % 10 == 0:
                print(f"🔄 Poll #{poll_count} - waiting for new messages...")

            message_batch = consumer.poll(timeout_ms=1000)
            if message_batch:
                print(f"📨 Received message batch with {len(message_batch)} partitions")
                for topic_partition, messages in message_batch.items():
                    print(f"📂 Partition {topic_partition}: {len(messages)} messages")
                    for msg in messages:
                        print(
                            f"💌 Raw message: key={msg.key}, value={msg.value}, offset={msg.offset}"
                        )
                        try:
                            forecast_count = int(msg.value)
                            print(f"✅ Successfully decoded to: {forecast_count}")
                            # Save next offset to file
                            with open(offset_file, "w") as f:
                                f.write(str(msg.offset + 1))
                            return forecast_count
                        except (ValueError, AttributeError) as e:
                            print(
                                f"❌ Error decoding message (expected integer): {msg.value}, error: {e}"
                            )
                            continue
            else:
                if poll_count % 30 == 0:
                    print("⏳ Still waiting for new messages on 'ai' topic...")
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("🛑 Interrupted while waiting for AI message")
    except Exception as e:
        print(f"💥 Unexpected error in consumer: {e}")
    finally:
        print("🔌 Closing consumer...")
        consumer.close()
        print("✅ Consumer closed")


if __name__ == "__main__":
    wait_for_ai_message()
