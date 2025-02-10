import os
from io import BytesIO

from minio import Minio

# MinIO 클라이언트 생성
minio_client = Minio(
    "localhost:9000",  # MinIO 서버 주소
    access_key="admin",  # MinIO 액세스 키
    secret_key="admin123",  # MinIO 시크릿 키
    secure=False  # HTTPS 사용 여부
)


def upload_to_minio(local_path, bucket_name, object_name):
    """
    MinIO로 파일 업로드

    Args:
        local_path (str): 로컬 파일 경로.
        bucket_name (str): MinIO 버킷 이름.
        object_name (str): MinIO에 저장될 객체 이름.

    Returns:
        str: 업로드된 파일의 URL.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    # MinIO 버킷 확인 및 생성
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # MinIO로 파일 업로드
    minio_client.fput_object(bucket_name, object_name, local_path)
    return f"http://localhost:9000/{bucket_name}/{object_name}"


def upload_memory_to_minio(object_name, buffer):
    byte_io = BytesIO(buffer)

    minio_client.put_object(
        bucket_name="di-bucket",
        object_name=object_name,
        data=byte_io,
        length=len(buffer)
    )

def upload_to_minio_frames(frames_dir, dir_name, bucket_name, base_object_name, video_path=None):
    """
    MinIO로 프레임 디렉터리 내 모든 파일 업로드 및 선택적으로 비디오 파일 업로드

    Args:
        frames_dir (str): 프레임이 저장된 로컬 디렉터리 경로.
        bucket_name (str): MinIO 버킷 이름.
        base_object_name (str): MinIO 객체 이름의 기본 값.
        video_path (str, optional): 업로드할 비디오 파일 경로. 기본값은 None.

    Returns:
        dict: 업로드된 첫 번째 프레임, 마지막 프레임, 비디오 파일의 URL.
    """
    if not os.path.exists(frames_dir) or not os.path.isdir(frames_dir):
        raise FileNotFoundError(f"Directory not found: {frames_dir}")

    # 프레임 파일들 정렬
    frames = sorted(
        [f for f in os.listdir(frames_dir) if os.path.isfile(os.path.join(frames_dir, f))]
    )

    if not frames:
        raise ValueError("No frames found in the directory.")

    first_frame_url = None
    last_frame_url = None

    total_frames = len(frames)
    successful_uploads = 0
    last_logged_percentage = -1

    # 모든 프레임을 업로드
    for idx, frame in enumerate(frames, start=1):
        frame_path = os.path.join(frames_dir, frame)
        try:
            frame_url = upload_to_minio(
                local_path=frame_path,
                bucket_name=bucket_name,
                object_name=f"{base_object_name}/{dir_name}/{frame}"
            )
            successful_uploads += 1

            # 첫 번째와 마지막 프레임 URL 저장
            if idx == 1:
                first_frame_url = frame_url
            if idx == total_frames:
                last_frame_url = frame_url

            # 진행 상태 로그 출력 (10% 단위)
            percentage = int((idx / total_frames) * 100)
            if percentage // 10 > last_logged_percentage // 10:
                print(f"Uploaded frames to MinIO ... {percentage}%")
                last_logged_percentage = percentage

        except Exception as e:
            print(f"Failed to upload frame {frame}: {e}")

    # 최종 로그 출력
    print(f"Uploaded {successful_uploads}/{total_frames} frames to MinIO.")

    # 비디오 파일 업로드 (선택적)
    video_url = None
    if video_path:
        try:
            video_url = upload_to_minio(
                local_path=video_path,
                bucket_name=bucket_name,
                object_name=f"{base_object_name}/video/video_output.mp4"
            )
            print(f"Video uploaded to MinIO: {bucket_name}/video_output.mp4")
        except Exception as e:
            print(f"Failed to upload video {video_path}: {e}")

    return {
        "firstFrameUrl": first_frame_url,
        "lastFrameUrl": last_frame_url,
        "videoUrl": video_url
    }



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
