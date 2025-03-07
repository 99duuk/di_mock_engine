import os
import time

from interface.kafka_consumer import start_kafka_consumers

import multiprocessing

from util.model_loader import ModelLoader
# if __name__ == "__main__":
#     # 멀티프로세싱을 위해 spawn 방식 사용
#     from multiprocessing import set_start_method
#     set_start_method("spawn", force=True)
#
#     # Kafka Consumer 시작
#     processes = start_kafka_consumers()
#
#     try:
#         while True:
#             time.sleep(0.1)
#     except KeyboardInterrupt:
#         print("\nShutting down...")
#
#     # 모든 프로세스 종료 대기
#     for process in processes:
#         process.join()

# MPS 관련 환경 변수 설정
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    # 필요 시 메인 프로세스에서 모델 초기화, 또는 자식 프로세스가 처리
    model = ModelLoader.get_yolo_model()
    interpreter = ModelLoader.get_tflite_face_mesh()

    # Kafka 소비자 시작
    processes = start_kafka_consumers()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nShutting down...")

    for process in processes:
        process.join()