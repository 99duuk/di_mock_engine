import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from core.original_to_processed import process_blurring_request
from core.processed_to_final import finalize_video
from model.complete_message import CompleteVideoResult
from model.processed_message import ProcessedVideoResult

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Kafka 로깅 레벨 설정 (INFO 로그 비활성화)
logging.getLogger("kafka").setLevel(logging.WARNING)

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

# Kafka Producer (전역 인스턴스)
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
    if isinstance(message, (ProcessedVideoResult, CompleteVideoResult)):
        message = message.to_dict()  # JSON 직렬화 가능하도록 변환
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )
    producer.send(topic, value=message)
    producer.flush()
    print(f"Sent message to {topic}: {message}")

import threading


def consumer_thread_function(group_id, request_topic, response_topic, model, interpreter):
    """스레드에서 실행되는 Kafka 소비자 함수"""
    print(f"[{group_id}] 스레드 시작")

    try:
        # Kafka 소비자 설정
        consumer = KafkaConsumer(
            request_topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_interval_ms=1200000,
            auto_offset_reset='earliest',
        )

        print(f"[{group_id}] {request_topic} 토픽 리스닝 중...")

        # 메시지 처리 루프
        for message in consumer:
            print(f"[{group_id}] 메시지 수신: {message.value}")
            try:
                if group_id == "video-processing-group":
                    result = process_blurring_request(message.value, model=model, interpreter=interpreter)
                elif group_id == "video-finalize-group":
                    result = finalize_video(message.value)
                else:
                    result = {"status": "error", "message": "유효하지 않은 group_id"}

                send_message(response_topic, result)

            except Exception as e:
                print(f"[{group_id}] 메시지 처리 오류: {e}")
                import traceback
                traceback.print_exc()
                send_message(response_topic, {"status": "error", "message": str(e)})

    except Exception as e:
        print(f"[{group_id}] 스레드 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()


def start_kafka_consumers():
    """여러 Kafka 소비자 스레드를 시작합니다"""
    threads = []

    # 메인 프로세스에서 모델 초기화 (모든 스레드에서 공유)
    from util.model_loader import ModelLoader
    print("메인 프로세스에서 모델 로딩 중...")
    model = ModelLoader.get_yolo_model()
    interpreter = ModelLoader.get_tflite_face_mesh()
    print("모델 로딩 완료")

    # 비디오 처리 스레드
    print("비디오 처리 스레드 시작 중...")
    video_thread = threading.Thread(
        target=consumer_thread_function,
        args=(
            "video-processing-group",
            KAFKA_GROUPS["video-processing-group"]["request_topic"],
            KAFKA_GROUPS["video-processing-group"]["response_topic"],
            model,
            interpreter
        )
    )
    video_thread.daemon = True
    video_thread.start()
    threads.append(video_thread)

    # 비디오 최종화 스레드
    print("비디오 최종화 스레드 시작 중...")
    finalize_thread = threading.Thread(
        target=consumer_thread_function,
        args=(
            "video-finalize-group",
            KAFKA_GROUPS["video-finalize-group"]["request_topic"],
            KAFKA_GROUPS["video-finalize-group"]["response_topic"],
            model,
            interpreter
        )
    )
    finalize_thread.daemon = True
    finalize_thread.start()
    threads.append(finalize_thread)

    print(f"총 {len(threads)}개 Kafka 소비자 스레드 시작됨")
    return threads
