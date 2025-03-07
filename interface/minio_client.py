import os
import logging
from minio.error import S3Error
from minio import Minio
from six import BytesIO

logger = logging.getLogger(__name__)

# MinIO 클라이언트 생성
minio_client = Minio(
    "localhost:9000",  # MinIO 서버 주소
    access_key="admin",  # MinIO 액세스 키
    secret_key="admin123",  # MinIO 시크릿 키
    secure=False  # HTTPS 사용 여부
)

BUCKET_NAME = "di-bucket"



def download_from_minio(object_name, local_path):
    """MinIO에서 파일 다운로드."""
    try:
        minio_client.fget_object(BUCKET_NAME, object_name, local_path)
        print(f"Downloaded {object_name} to {local_path}")
        return local_path
    except Exception as e:
        print(f"Error downloading from MinIO: {e}")
        raise

def upload_to_minio(local_path, object_name):
    """MinIO에 파일 업로드."""
    try:
        minio_client.fput_object(BUCKET_NAME, object_name, local_path)
        print(f"Uploaded {local_path} to MinIO as {object_name}")
        return f"http://localhost:9000/{BUCKET_NAME}/{object_name}"
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        raise


def upload_memory_to_minio(object_name, buffer):
    """메모리 버퍼를 MinIO에 업로드"""
    try:
        byte_io = BytesIO(buffer)
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=byte_io,
            length=len(buffer),
        )
        print(f"Uploaded {object_name} to MinIO")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        raise


def download_images_from_minio_folder(uuid, folder_name, local_path):
    """MinIO에서 특정 폴더 내 모든 이미지 다운로드"""
    try:
        # MinIO 버킷 내 지정된 폴더의 파일 리스트 가져오기
        objects = minio_client.list_objects(BUCKET_NAME, prefix=f"{uuid}/{folder_name}/", recursive=True)

        # 디렉토리 생성
        os.makedirs(local_path, exist_ok=True)

        for obj in objects:
            if obj.is_dir:
                continue  # 디렉토리는 건너뜀

            # 개별 파일 다운로드
            file_name = os.path.basename(obj.object_name)
            local_file_path = os.path.join(local_path, file_name)
            minio_client.fget_object(BUCKET_NAME, obj.object_name, local_file_path)

            logger.info(f"Downloaded {obj.object_name} to {local_file_path}")

    except S3Error as e:
        logger.error(f"MinIO download error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while downloading images: {e}")
        return False

    return True
