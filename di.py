import time

from interface.kafka_consumer import start_kafka_consumers, send_message

import multiprocessing
#
# if __name__ == "__main__":
#     multiprocessing.set_start_method("spawn")  # fork 대신 spawn 사용
#
#     # Kafka Consumer 시작 (멀티스레드)
#     threads = start_kafka_consumers()
#
#     try:
#         while True:
#             time.sleep(0.1)
#     except KeyboardInterrupt:
#         print("\nShutting down...")
#
#     # 모든 스레드 종료 대기
#     for thread in threads:
#         thread.join()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")  # 중요!

    processes = start_kafka_consumers()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nShutting down...")

    # 모든 프로세스 종료 대기
    for process in processes:
        process.join()
