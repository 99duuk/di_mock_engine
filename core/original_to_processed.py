import glob
import json
import logging
import os
import shutil

import cv2
import face_recognition
import numpy as np
import torch

from config import logger
from interface.minio_client import BUCKET_NAME, download_from_minio, upload_to_minio, download_images_from_minio_folder
from model.processed_message import ProcessedVideoResult, VideoMetadata
from util.ffmpeg import get_video_info_ffprobe
from util.state_manager import state_manager

logger.setLevel(logging.DEBUG)

# MPS 디바이스 확인
device = 'mps' if torch.backends.mps.is_available() else 'cpu'
print(f"Using device: {device}")

# YOLO 및 FaceMesh 모델 가져오기
# model = ModelLoader.get_yolo_model()
# mp_face_mesh = ModelLoader.get_mp_face_mesh()
def print_progress(video_id, frame_seq, total_frames):
    """진행 상황을 한 줄에서 퍼센트와 진행 바로 업데이트"""
    progress = (frame_seq / total_frames) * 100
    bar_length = 30  # 진행 바 길이
    filled_length = int(bar_length * frame_seq // total_frames)
    bar = "#" * filled_length + " " * (bar_length - filled_length)
    # \r로 한 줄에서 업데이트
    print(f"\r==> Processing {video_id}... {bar} {progress:.1f}%", end="", flush=True)

def process_blurring_request(message, model, interpreter):
    """Kafka 메시지에서 split 요청을 처리"""
    try:
        video_id = message.get("video_id")
        output_dir = os.path.join("output", video_id)

        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)

        # 파일 경로 설정
        original_local_path = os.path.join(output_dir, "original.mp4")

        # 원본 영상 다운로드
        download_from_minio(f"{video_id}/original.mp4", original_local_path)

        # target 폴더 내 모든 이미지 다운로드
        target_image_local_path = os.path.join(output_dir, "target")
        os.makedirs(target_image_local_path, exist_ok=True)
        download_images_from_minio_folder(video_id, "target", target_image_local_path)

        # target_images 디렉토리에서 모든 이미지 파일 가져오기
        reference_image_paths = glob.glob(os.path.join(target_image_local_path, "*"))
        logger.debug(f"Found {len(reference_image_paths)} target images: {reference_image_paths}")
        reference_encodings = []

        # 타겟 이미지가 있는 경우에만 인코딩 수행
        for reference_image_path in reference_image_paths:
            reference_image = face_recognition.load_image_file(reference_image_path)
            encodings = face_recognition.face_encodings(reference_image, num_jitters=10)
            if not encodings:
                logger.debug(f"Error: 얼굴 감지 실패 {reference_image_path}. 해당 이미지를 스킵합니다.")
                continue
            reference_encodings.append(encodings[0])
            logger.debug(f"얼굴 인코딩 로드 완료 : {reference_image_path}")

        # 영상 처리 및 JSON 생성
        json_output_path, total_frames, fps, width, height, duration = process_video(
            original_local_path, reference_encodings, model=model, interpreter=interpreter
        )

        # 처리된 영상 및 JSON 업로드
        upload_to_minio(json_output_path, f"{video_id}/metadata.json")

        # 작업 완료 후 output 디렉터리 삭제
        shutil.rmtree(output_dir)
        logger.debug(f"Deleted temporary directory: {output_dir}")

        return {
            "status": "success",
            "video_id": video_id,
            "metadata_url": f"http://localhost:9000/{BUCKET_NAME}/{video_id}/metadata.json",
            "message": "Video processed and uploaded",
            "video_metadata": {
                "width": width,
                "height": height,
                "total_frames": total_frames,
                "fps": fps,
                "duration": duration,
            },
        }

    except Exception as e:
        # 예외 발생 시에도 디렉터리 삭제 시도
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
            logger.debug(f"Deleted temporary directory on error: {output_dir}")
        return ProcessedVideoResult(
            status="error",
            video_id=video_id,
            metadata_url="",
            message=str(e),
            video_metadata=VideoMetadata(width=0, height=0, fps=0, total_frames=0, duration=0.0),
        )

def process_frame_with_reference(frame, detections, reference_encodings, width, height, interpreter, tolerance=0.6):
    """참조 이미지가 있을 때 프레임 처리"""
    frame_info = {"person": []}

    # FaceMesh 입력 전처리
    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()

    input_frame = cv2.resize(frame, (input_details[0]['shape'][2], input_details[0]['shape'][1]))  # 모델 입력 크기로 조정
    input_frame = input_frame.astype(np.float32) / 255.0  # UINT8 -> FLOAT32, 0-1 범위로 정규화
    input_frame = np.expand_dims(input_frame, axis=0)  # 배치 차원 추가 [1, H, W, C]
    interpreter.set_tensor(input_details[0]['index'], input_frame)
    interpreter.invoke()
    landmarks = interpreter.get_tensor(output_details[0]['index'])

    for detection in detections:
        x1, y1, x2, y2 = map(int, detection.xyxy[0])
        x1, y1, x2, y2 = max(0, x1 - 10), max(0, y1 - 10), min(width, x2 + 10), min(height, y2 + 10)
        face_image = frame[y1:y2, x1:x2]

        if face_image.shape[0] < 20 or face_image.shape[1] < 20:
            frame_info["person"].append({"x1": x1, "x2": x2, "y1": y1, "y2": y2})
            continue

        face_location = [(y1, x2, y2, x1)]
        face_encodings = face_recognition.face_encodings(frame, known_face_locations=face_location, num_jitters=5)

        if face_encodings:
            face_encoding = face_encodings[0]
            matches = face_recognition.compare_faces(reference_encodings, face_encoding, tolerance=tolerance)
            if True in matches:
                continue

        frame_info["person"].append({"x1": x1, "x2": x2, "y1": y1, "y2": y2})

    return frame_info


def process_frame_without_reference(detections):
    """참조 이미지가 없을 때 프레임 처리"""
    frame_info = {"person": []}

    for detection in detections:
        x1, y1, x2, y2 = map(int, detection.xyxy[0])
        frame_info["person"].append({"x1": int(x1), "x2": int(x2), "y1": int(y1), "y2": int(y2)})

    return frame_info


def process_video(input_path, reference_encodings=None, model=None, interpreter=None, tolerance=0.6):
    """split 시 원본 영상을 처리하여 metadata.json 생성"""
    state_manager.frames = []  # 프레임 리스트 초기화
    state_manager.processed_frames = []  # 편집된 프레임 리스트 초기화

    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        print("Error: Could not open video.")
        return None

    total_frames, duration = get_video_info_ffprobe(input_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))

    video_id = os.path.basename(input_path).replace(".mp4", "")  # video_id 추출
    print(f"영상 정보: 총 {total_frames} 프레임, 영상 길이 : {duration} ,FPS: {fps}, 해상도: {width}x{height}")

    sequence_data = []  # JSON 저장을 위한 데이터
    frame_seq = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_seq += 1
        print_progress(video_id, frame_seq, total_frames)
        # print(f"current seq: {frame_seq}, total seq: {total_frames}")

        results = model(frame, conf=0.4, verbose=False)  # YOLO 감지 실행
        detections = results[0].boxes

        # 참조 이미지 유무에 따라 다른 처리 함수 호출
        frame_info = (process_frame_with_reference(frame, detections, reference_encodings, width, height, interpreter, tolerance)
                      if reference_encodings
                      else process_frame_without_reference(detections))

        frame_info["seq"] = frame_seq
        sequence_data.append(frame_info)

    cap.release()

    logger.debug(f"Total frames in sequence_data: {len(sequence_data)}")
    json_output_path = input_path.replace(".mp4", ".json")
    with open(json_output_path, "w") as json_file:
        json.dump(
            {
                "sequence": sequence_data,
                "total_frames": total_frames,
                "fps": fps,
                "width": width,
                "height": height,
                "duration": duration,
            },
            json_file,
            indent=4,
        )

    logger.debug(f"JSON metadata saved as {json_output_path}")
    return json_output_path, total_frames, fps, width, height, duration
