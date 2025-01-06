from datetime import datetime
import os
import json
from kafka import KafkaConsumer, KafkaProducer
from interface.input_handler import download_video, load_video
from interface.minio_clinet import upload_to_minio, upload_to_minio_frames
from core.video_to_frames import video_to_frames
from core.frames_to_video import frames_to_video


def get_unique_dirname(dir_path):
    """ 중복된 디렉터리 이름을 처리하여 고유한 이름 생성. """
    if not os.path.exists(dir_path):
        return dir_path

    base_dir = dir_path
    counter = 1
    while os.path.exists(dir_path):
        dir_path = f"{base_dir}_{counter}"
        counter += 1
    return dir_path


def get_unique_filename(file_path):
    """ 중복된 파일 이름을 처리하여 고유한 이름 생성. """
    if not os.path.exists(file_path):
        return file_path

    base_name, ext = os.path.splitext(file_path)
    counter = 1
    while os.path.exists(file_path):
        file_path = f"{base_name}_{counter}{ext}"
        counter += 1
    return file_path


def get_output_paths(input_source, output_dir, request_id):
    """
    출력 경로를 requestId와 타임스탬프 기반으로 생성.

    Args:
        input_source (str): 입력 비디오 경로 또는 URL (파일 이름은 무시됨).
        output_dir (str): 출력 디렉터리 경로.
        request_id (str): 요청 ID.

    Returns:
        tuple: 다운로드된 비디오 경로, 프레임 출력 디렉터리, 병합된 비디오 출력 파일 경로.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    base_name = f"{request_id}_{timestamp}"

    frames_output_dir = get_unique_dirname(os.path.join(output_dir, f"{base_name}_frames"))
    downloaded_video_path = get_unique_filename(os.path.join(frames_output_dir, f"{base_name}.mp4"))
    video_output_path = get_unique_filename(os.path.join(frames_output_dir, f"{base_name}_output.mp4"))

    return (
        os.path.abspath(downloaded_video_path),
        os.path.abspath(frames_output_dir),
        os.path.abspath(video_output_path),
    )


def upload_to_bucket_or_local(local_path, bucket_name, object_name):
    """
    MinIO 버킷에 업로드하고 실패 시 로컬에 저장.

    Args:
        local_path (str): 업로드할 파일의 로컬 경로.
        bucket_name (str): MinIO 버킷 이름.
        object_name (str): MinIO 객체 이름.

    Returns:
        str: 업로드된 URL 또는 로컬 경로.
    """
    try:
        return upload_to_minio(local_path, bucket_name, object_name)
    except Exception as e:
        print(f"Failed to upload {local_path} to bucket {bucket_name}: {e}")
        return os.path.abspath(local_path)


def process_message(message):
    """ Kafka 메시지를 처리하고 결과를 반환. """
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

        downloaded_video_path, frames_output_dir, video_output_path = get_output_paths(input_source, output_dir, request_id)

        if operation == "split":
            if input_source.startswith("http"):
                download_video(input_source, downloaded_video_path)
                video_path = downloaded_video_path
            else:
                video_path = load_video(input_source)

            video_to_frames(video_path, frames_output_dir)
            print(f"Frames saved to {frames_output_dir}")

            upload_result = upload_to_minio_frames(
                frames_dir=frames_output_dir,
                bucket_name=bucket_name,
                base_object_name=request_id,
                video_path=None
            )

            return {
                "status": "success",
                "requestId": request_id,
                "outputPath": frames_output_dir,
                "firstFrameUrl": upload_result["firstFrameUrl"],
                "lastFrameUrl": upload_result["lastFrameUrl"],
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


def consume_and_process():
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


if __name__ == "__main__":
    consume_and_process()
