import json
from io import BytesIO

import cv2
import os

from interface.minio_clinet import upload_memory_to_minio
from model.timeline_message import TimelineMessage


def video_to_frames(video_path, output_dir):
    """
    비디오 파일을 읽어 프레임 단위로 이미지를 추출하여 저장합니다.

    Args:
        video_path (str): 입력 비디오 파일 경로.
        output_dir (str): 프레임 이미지가 저장될 디렉터리.

    Returns:
        None
    """
    # 출력 디렉터리를 생성 (존재하지 않으면 생성)
    os.makedirs(output_dir, exist_ok=True)

    # 비디오 캡처 객체 생성
    cap = cv2.VideoCapture(video_path)
    frame_count = 0

    while cap.isOpened():
        # 한 프레임씩 읽기
        ret, frame = cap.read()
        if not ret:  # 더 이상 프레임이 없으면 종료
            break

        # 프레임 이미지를 디렉터리에 저장
        frame_path = os.path.join(output_dir, f"frame_{frame_count:04d}.jpg")
        cv2.imwrite(frame_path, frame)
        frame_count += 1

    # 캡처 객체 해제
    cap.release()
    print(f"Extracted {frame_count} frames to {output_dir}")
    return frame_count - 1


def extract_timeline_thumbnails_to_minio(video_path, video_id):
    """
    동영상에서 1초 간격으로 프레임을 추출하여 MinIO에 업로드.
    """
    print("Processing video for timeline thumbnails:", video_path)
    cap = cv2.VideoCapture(video_path)
    print(cap.isOpened())
    frame_rate = cap.get(cv2.CAP_PROP_FPS)
    success, frame = cap.read()
    total_frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    count = 0

    frame_metadata_list = []
    while success:
        if count % frame_rate == 0:  # 1초 간격으로 처리
            resized_frame = cv2.resize(frame, (100, 80), interpolation=cv2.INTER_AREA)
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, 50]

            _, buffer = cv2.imencode('.jpg', resized_frame, encode_params)  # 프레임을 메모리 버퍼로 인코딩
            # MinIO에 업로드
            object_name = f"{video_id}/timeline/timeline_{count}.jpg"
            metadata = {"sequence": count, "object_name": object_name}
            frame_metadata_list.append(metadata)
            upload_memory_to_minio(object_name, buffer)

        success, frame = cap.read()
        count += 1

    cap.release()
    print("Finished processing video.")
    
    # 메타데이터를 json 파일로 만들어서 업로드
    json_data = json.dumps(frame_metadata_list, ensure_ascii=False, indent=2)
    json_bytes = json_data.encode("utf-8")
    json_object_name= f"{video_id}/timeline/timeline.json"
    upload_memory_to_minio(json_object_name, json_bytes)
    
    timeline_message = TimelineMessage(
        videoId=video_id,
        message="extracting timeline thumbnails is completed",
        fps=frame_rate,
        totalFrameCount = total_frame_count,
    )
    return timeline_message
