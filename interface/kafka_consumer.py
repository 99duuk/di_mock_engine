import json
import threading
from dataclasses import asdict

from kafka import KafkaConsumer, KafkaProducer
from core.original_to_processed import process_blurring_request
from core.processed_to_final import process_finalize_request
from model.complete_message import CompleteVideoResult
from model.processed_message import ProcessedVideoResult
from multiprocessing import Process

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Consumer Group & Topics 정의
KAFKA_GROUPS = {
    "video-processing-group": {
        "request_topic": "video-processing-requests",
        "response_topic": "video-processing-responses"
    },
    "video-finalize-group": {
        "request_topic": "video-finalize-requests",
        "response_topic": "video-finalize-responses"
    }
}

# Kafka Producer (모든 그룹에서 공통 사용)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)


def process_kafka_message(group_id, message):
    """Kafka 메시지를 처리하고 결과를 생성"""
    print(f"[{group_id}] Received message: {message}")

    # 메시지 처리 로직
    result = {
        "status": "success",
        "processed_data": message,
        "group": group_id
    }

    return result

def send_message(topic, message):
    """Kafka Producer: 지정된 토픽으로 메시지 전송"""
    producer.send(topic, value=message)
    print(f"Sent message to {topic}: {message}")


def consume_kafka_messages(group_id, request_topic, response_topic):
    """Kafka Consumer 실행 → 메시지를 처리한 후 응답을 Kafka에 전송"""
    consumer = KafkaConsumer(
        request_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print(f"[{group_id}] Listening on {request_topic}...")

    for message in consumer:
        msg_value = message.value
        print(f"[{group_id}] Received message: {msg_value}")

        if group_id == "video-processing-group":  # split 요청 처리
            result = process_blurring_request(msg_value)
        elif group_id == "video-finalize-group":  # merge 요청 처리
            result = process_finalize_request(msg_value)
        else:
            result = {"status": "error", "message": "Invalid group_id"}

        # 결과를 Kafka Producer로 응답 토픽에 전송
        send_message(response_topic, result)

#
# def start_kafka_consumers():
#     """여러 Kafka Consumer를 개별 스레드에서 실행"""
#     threads = []
#
#     for group_id, topics in KAFKA_GROUPS.items():
#         thread = threading.Thread(
#             target=consume_kafka_messages,
#             args=(group_id, topics["request_topic"], topics["response_topic"]),
#             daemon=True
#         )
#         threads.append(thread)
#         thread.start()
#         print(f"Started Kafka Consumer for {group_id}")
#
#     return threads

def start_kafka_consumers():
    """여러 Kafka Consumer를 개별 프로세스로 실행"""
    processes = []

    for group_id, topics in KAFKA_GROUPS.items():
        process = Process(
            target=consume_kafka_messages,
            args=(group_id, topics["request_topic"], topics["response_topic"])
        )
        processes.append(process)
        process.start()
        print(f"Started Kafka Consumer process for {group_id}")

    return processes
