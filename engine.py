import json
import threading
import time
from dataclasses import asdict

from kafka import KafkaConsumer, KafkaProducer
from core.video_to_frames import extract_timeline_thumbnails_to_minio

def consume_timeline():
    """Kafka Consumer로 메시지를 받아 처리하고 MinIO로 결과를 업로드."""
    print("consume_timeline is starting...")
    consumer = KafkaConsumer(
        'video-timeline-requests',
        bootstrap_servers='localhost:9092',
        group_id='mock-engine-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        try:
            print(f"Received timeline message: {message.value}")
            input_source = message.value.get('url')
            bucket_name = message.value.get('bucket')
            video_id = message.value.get('video_id', 'default')

            if not input_source or not bucket_name:
                raise ValueError("Missing input_source or bucket_name")

            timeline_message = extract_timeline_thumbnails_to_minio(
                video_path=input_source,
                video_id=video_id,
            )

            print(timeline_message)
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            producer.send(topic='video-timeline-response', value=asdict(timeline_message))
            print(f"Processed result: {timeline_message}")

        except Exception as e:
            result = {
                "status": "error",
                "message": str(e),
                "requestId": message.value.get('requestId', 'unknown')
            }
            print(f'error: {result}')


def produce_assemble_response(result):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send('video-processing-response', value=result)


if __name__ == "__main__":
    # consume_frame_update()
    thread = threading.Thread(target=consume_timeline, daemon=True)
    thread.start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nShutting down...")

    thread.join()