import cv2
import os
from minio import Minio


def get_frames_from_minio(dir_path):
    # MinIO 클라이언트 설정
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False  # SSL 사용 여부
    )

    bucket_name = "di-bucket"
    directory_path = dir_path

    # 해당 디렉토리의 모든 파일 가져오기
    objects = client.list_objects(bucket_name, prefix=directory_path, recursive=True)

    frames = sorted([obj.object_name for obj in objects if obj.object_name.endswith('.jpg') or obj.object_name.endswith('.png')])

    # 로컬 디렉토리 준비
    local_frames_dir = "local_frames/"+dir_path
    os.makedirs(local_frames_dir, exist_ok=True)

    # 프레임 다운로드
    for frame in frames:
        local_path = os.path.join(local_frames_dir, os.path.basename(frame))
        client.fget_object(bucket_name, frame, local_path)
    
    return "output/"+dir_path+".avi";




def frames_to_video(frame_dir, output_video, fps=30):
    """
    프레임 이미지들을 읽어 비디오 파일로 병합합니다.

    Args:
        frame_dir (str): 프레임 이미지가 저장된 디렉터리.
        output_video (str): 생성될 비디오 파일 경로.
        fps (int): 비디오의 프레임 속도 (기본값: 30).

    Returns:
        None
    """
    # 프레임 파일 목록 정렬 (프레임 순서 유지)
    frame_files = sorted([os.path.join(frame_dir, f) for f in os.listdir(frame_dir) if f.endswith(".jpg") or f.endswith(".png")])
    if not frame_files:  # 프레임 파일이 없으면 오류 발생
        raise ValueError("No frames found in the directory.")

    # 첫 번째 프레임으로 비디오 해상도 결정
    first_frame = cv2.imread(frame_files[0])
    height, width, _ = first_frame.shape

    # 비디오 작성 객체 생성
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # mp4 코덱
    out = cv2.VideoWriter(output_video, fourcc, fps, (width, height))

    # 각 프레임을 비디오에 추가
    for frame_file in frame_files:
        frame = cv2.imread(frame_file)
        out.write(frame)

    # 비디오 작성 객체 해제
    out.release()
    print(f"Video saved to {output_video}")

    return output_video