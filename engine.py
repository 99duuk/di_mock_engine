import json
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

from core.frames_to_video import frames_to_video, get_frames_from_minio
from core.video_to_frames import video_to_frames
from interface.input_handler import download_video, load_video
from interface.minio_clinet import upload_to_minio_frames
from util.file_util import get_output_paths


def process_message(message):
    """ Kafka 메시지를 처리하고 결과를 반환. """
    print(message)
    try:
        input_source = message.get('url')
        operation = message.get('operation')
        output_dir = message.get('output_dir', 'output')
        request_id = message.get('requestId')
        bucket_name = message.get('bucket', 'video-processing')

        if not input_source or not operation:
            return {
                "status": "error",
                "message": "Missing input_source or operation",
                "requestId": request_id
            }

        downloaded_video_path, frames_output_dir, video_output_path \
            = get_output_paths(input_source, output_dir, request_id)

        if operation == "split":
            if input_source.startswith("http"):
                download_video(input_source, downloaded_video_path)
                video_path = downloaded_video_path
            else:
                video_path = load_video(input_source)

            end_frame_sequence = video_to_frames(video_path, frames_output_dir)
            print(f"Frames saved to {frames_output_dir}")

            upload_result = upload_to_minio_frames(
                frames_dir=frames_output_dir,
                bucket_name=bucket_name,
                base_object_name=request_id,
                video_path=None
            )

            # TODO:  파일 업로드 후 삭제 로직 구현 필요함.
            return {
                "status": "success",
                "requestId": request_id,
                "outputPath": frames_output_dir,
                "firstFrameUrl": upload_result["firstFrameUrl"],
                "lastFrameUrl": upload_result["lastFrameUrl"],
                "startSequence": 0,
                "endSequence": end_frame_sequence,
                "videoUrl": None,
                "operation": "split",
                "message": "Frames uploaded to MinIO"
            }

        elif operation == "merge":
            frames_to_video(frames_output_dir, video_output_path)
            print(f"Video saved to {video_output_path}")

            upload_result = upload_to_minio_frames(
                frames_dir=frames_output_dir,
                bucket_name=bucket_name,
                base_object_name=request_id,
                video_path=video_output_path
            )

            return {
                "status": "success",
                "requestId": request_id,
                "outputPath": frames_output_dir,
                "firstFrameUrl": upload_result["firstFrameUrl"],
                "lastFrameUrl": upload_result["lastFrameUrl"],
                "videoUrl": upload_result["videoUrl"],
                "operation": "merge",
                "message": "Video and frames uploaded to MinIO"
            }

        else:
            return {
                "status": "error",
                "message": "Unsupported operation. Use 'split' or 'merge'.",
                "requestId": request_id
            }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "requestId": message.get('requestId', 'unknown')
        }


def safe_json_deserializer(m):
    """JSON 메시지를 안전하게 디코딩합니다."""
    if m is None:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except (json.JSONDecodeError, AttributeError, UnicodeDecodeError) as e:
        print(f"[Deserialization Error] Failed to deserialize message: {m}, Error: {e}")
        return None


def consume_and_process():
    print("consume_and_process is starting...")
    """ Kafka Consumer로 메시지를 받아 처리하고 Kafka로 결과를 발행. """
    consumer = KafkaConsumer(
        'video-processing-requests',
        bootstrap_servers='localhost:9092',
        group_id='mock-engine-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        try:
            print(f"Received message: {message.value}")
            result = process_message(message.value)
        except Exception as e:
            result = {
                "status": "error",
                "message": str(e),
                "requestId": message.value.get('requestId', 'unknown')
            }

        producer.send('video-processing-response', value=result)
        print(f"Sent result: {result}")


def consume_assemble_request():
    """
    Kafka Consumer로 메시지를 받아 Mock Engine을 실행하고 결과를 Kafka로 발행합니다.
    """
    consumer = KafkaConsumer(
        'video-assemble-request',
        bootstrap_servers='localhost:9092',
        group_id='mock-engine-group',
        value_deserializer=safe_json_deserializer
    )

    print("Kafka Consumer 시작. 메시지를 기다리는 중...")

    for message in consumer:
        try:
            # 메시지 처리 로직 (예시)
            result = {
                "status": "success",
                "message": "Processed successfully",
                "frameDir": message.value
            }
            local_frames_dir, local_vid_path = get_frames_from_minio(message.value)
            print(local_frames_dir, local_vid_path)
            output_vid = frames_to_video(local_frames_dir, local_vid_path)
            print(output_vid)
            # 결과를 Kafka로 전송
            # producer.send('video-processing-response', value=result)
            print(f"[✅ Success] Sent result: {result}")

        except Exception as e:
            print(f"[❌ Error] Exception while processing message: {e}")
            result = {
                "status": "error",
                "message": str(e),
                "frameDir": message.value if message.value else None
            }
            print(f"[❌ Failure] Sent error result: {result}")


def produce_assemble_response(result):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send('video-processing-response', value=result)


if __name__ == "__main__":
    # consume_frame_update()
    thread1 = threading.Thread(target=consume_and_process, daemon=True)
    # thread2 = threading.Thread(target=consume_assemble_request, daemon=True)

    thread1.start()
    # thread2.start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nShutting down...")

    thread1.join()
    # thread2.join()

    # 테스트 실행
# if __name__ == "__main__":
#     mock_engine(
#         input_source="https://youtu.be/GUfSHjgucts",  # 유튜브 URL
#         operation="split",                           # "merge"도 가능
#         output_dir="output"                          # 출력 디렉터리
#     )

#   https://youtu.be/GUfSHjgucts
