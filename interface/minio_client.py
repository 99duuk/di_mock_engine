from minio import Minio
from six import BytesIO

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






def download_stream_from_minio(object_name):
    """MinIO에서 스트리밍으로 파일을 다운로드"""
    try:
        response = minio_client.get_object(BUCKET_NAME, object_name)
        return response  # 스트리밍 객체 반환 (BytesIO 아님)
    except Exception as e:
        print(f"Error downloading {object_name} from MinIO: {e}")
        raise


def upload_stream_to_minio(object_name, file_obj, size):
    """MinIO에 스트리밍으로 파일 업로드"""
    try:
        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            data=file_obj,
            length=size
        )
        print(f"Uploaded {object_name} to MinIO")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        raise
