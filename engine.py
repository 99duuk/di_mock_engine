from kafka import KafkaConsumer, KafkaProducer
from interface.input_handler import download_video, load_video
from core.video_to_frames import video_to_frames
from core.frames_to_video import frames_to_video
import json
import os

def get_unique_dirname(dir_path):
    """
    중복된 디렉터리 이름을 처리하여 고유한 이름 생성.

    Args:
        dir_path (str): 원래 디렉터리 경로.

    Returns:
        str: 고유한 디렉터리 경로.
    """
    if not os.path.exists(dir_path):
        return dir_path

    base_dir = dir_path
    counter = 1
    while os.path.exists(dir_path):
        dir_path = f"{base_dir}_{counter}"  # 디렉터리 이름에 번호 추가
        counter += 1
    return dir_path

def get_unique_filename(file_path):
    """
    중복된 파일 이름을 처리하여 고유한 이름 생성.

    Args:
        file_path (str): 원래 파일 경로.

    Returns:
        str: 고유한 파일 경로.
    """
    if not os.path.exists(file_path):
        return file_path

    base_name, ext = os.path.splitext(file_path)
    counter = 1
    while os.path.exists(file_path):
        file_path = f"{base_name}_{counter}{ext}"  # 파일 이름에 번호 추가
        counter += 1
    return file_path

def get_output_paths(input_source, output_dir):
    """
    입력 비디오 파일 이름 기반으로 출력 경로 생성.

    Args:
        input_source (str): 입력 비디오 경로 또는 URL.
        output_dir (str): 출력 디렉터리.

    Returns:
        tuple: 다운로드된 비디오 파일 경로, 프레임 출력 디렉터리, 병합된 비디오 출력 파일 경로.
    """
    if input_source.startswith("http"):
        video_title = os.path.splitext(os.path.basename(input_source))[0]
        base_name = video_title.replace(" ", "_")  # 공백을 밑줄로 대체
    else:
        base_name = os.path.splitext(os.path.basename(input_source))[0]

    # 프레임 디렉터리
    frames_output_dir = os.path.join(output_dir, f"{base_name}_frames")
    frames_output_dir = get_unique_dirname(frames_output_dir)  # 중복 처리

    # 절대 경로 변환
    frames_output_dir = os.path.abspath(frames_output_dir)

    # 프레임 디렉터리 안에 비디오 경로 생성
    downloaded_video_path = os.path.join(frames_output_dir, f"{base_name}.mp4")
    downloaded_video_path = get_unique_filename(downloaded_video_path)  # 중복 처리
    downloaded_video_path = os.path.abspath(downloaded_video_path)

    video_output_path = os.path.join(frames_output_dir, f"{base_name}_output.mp4")
    video_output_path = get_unique_filename(video_output_path)  # 중복 처리
    video_output_path = os.path.abspath(video_output_path)

    return downloaded_video_path, frames_output_dir, video_output_path


def mock_engine(input_source, operation, output_dir):
    """
    Mock Engine의 메인 컨트롤 함수로 입력, 변환, 출력을 관리합니다.

    Args:
        input_source (str): 입력 소스 (URL 또는 로컬 경로).
        operation (str): 작업 모드 ("split" 또는 "merge").
        output_dir (str): 출력 디렉터리.

    Returns:
        None
    """
    # 출력 경로 생성
    downloaded_video_path, frames_output_dir, video_output_path = get_output_paths(input_source, output_dir)

    # 입력 처리
    if input_source.startswith("http"):
        download_video(input_source, downloaded_video_path)
        video_path = downloaded_video_path
    else:
        video_path = load_video(input_source)

    # 작업 수행
    if operation == "split":
        video_to_frames(video_path, frames_output_dir)
        print(f"Frames saved to {frames_output_dir}")
    elif operation == "merge":
        frames_to_video(frames_output_dir, video_output_path)
        print(f"Video saved to {video_output_path}")
    else:
        raise ValueError("Unsupported operation. Use 'split' or 'merge'.")

def process_message(message):
    """
    Kafka 메시지를 기반으로 Mock Engine 실행.
    """
    input_source = message.get('url')
    operation = message.get('operation')
    output_dir = message.get('output_dir', 'output')
    request_id = message.get('requestId')  # 요청 ID 포함

    if not input_source or not operation:
        return {
            "status": "error",
            "message": "Missing input_source or operation",
            "requestId": request_id
        }

    try:
        downloaded_video_path, frames_output_dir, video_output_path = get_output_paths(input_source, output_dir)

        if operation == "split":
            if input_source.startswith("http"):
                download_video(input_source, downloaded_video_path)
                video_path = downloaded_video_path
            else:
                video_path = load_video(input_source)
            video_to_frames(video_path, frames_output_dir)
            return {
                "status": "success",
                "operation": "split",
                "frameOutputPath": frames_output_dir,
                "requestId": request_id,
                "message": f"Frames saved to {frames_output_dir}"
            }

        elif operation == "merge":
            frames_to_video(frames_output_dir, video_output_path)
            return {
                "status": "success",
                "operation": "merge",
                "videoOutputPath": video_output_path,
                "requestId": request_id,
                "message": f"Video saved to {video_output_path}"
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
            "requestId": request_id
        }


def consume_and_process():
    """
    Kafka Consumer로 메시지를 받아 Mock Engine을 실행하고 결과를 Kafka로 발행.
    """
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
                "requestId": message.value.get('requestId')  # 요청 ID 포함
            }
        producer.send('video-processing-response', value=result)
        print(f"Sent result: {result}")

if __name__ == "__main__":
    consume_and_process()


    # 테스트 실행
# if __name__ == "__main__":
#     mock_engine(
#         input_source="https://youtu.be/GUfSHjgucts",  # 유튜브 URL
#         operation="split",                           # "merge"도 가능
#         output_dir="output"                          # 출력 디렉터리
#     )

#   https://youtu.be/GUfSHjgucts
