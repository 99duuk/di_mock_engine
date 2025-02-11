import time

from interface.kafka_consumer import start_kafka_consumers, send_message


# --------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Kafka Consumer 시작 (멀티스레드)
    threads = start_kafka_consumers()

    # Kafka로 테스트 메시지 전송 (필요할 경우)
    time.sleep(2)  # Consumer가 실행될 시간을 확보
    test_message = {"request": "test_process", "video_id": 12345}
    send_message("video-processing-requests", test_message)

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nShutting down...")

    # 모든 스레드 종료 대기
    for thread in threads:
        thread.join()

