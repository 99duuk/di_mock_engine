import os
import glob
import json
import cv2
import face_recognition

from core.process import mosaic
from interface.minio_client import download_from_minio, upload_to_minio
from util.ffmpeg import get_frame_count_ffprobe
from util.model_loader import ModelLoader
from config import logger
from util.state_manager import state_manager

# YOLO 및 FaceMesh 모델 가져오기
model = ModelLoader.get_yolo_model()
mp_face_mesh = ModelLoader.get_mp_face_mesh()


def process_blurring_request(message):
    """Kafka 메시지에서 split 요청을 처리"""
    try:
        uuid = message.get("requestId")
        bucket_name = message.get("bucket_name", "di-bucket")
        output_dir = os.path.join("output", uuid)

        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)

        # 파일 경로 설정
        original_local_path = os.path.join(output_dir, "original.mp4")
        processed_local_path = os.path.join(output_dir, "processed.mp4")

        print("Processing split operation...")

        # 원본 영상 다운로드
        download_from_minio(f"{uuid}/original.mp4", original_local_path)

        # target 폴더 내 모든 이미지 다운로드
        target_image_local_path = os.path.join(output_dir, "target_images")
        os.makedirs(target_image_local_path, exist_ok=True)

        download_from_minio(f"{uuid}/target", target_image_local_path)

        # target_images 디렉토리에서 모든 이미지 파일 가져오기
        reference_image_paths = glob.glob(os.path.join(target_image_local_path, "*"))
        reference_encodings = []

        for reference_image_path in reference_image_paths:
            reference_image = face_recognition.load_image_file(reference_image_path)
            encodings = face_recognition.face_encodings(reference_image, num_jitters=10)

            if not encodings:
                logger.debug(f"Error: 얼굴 감지 실패 {reference_image_path}. 해당 이미지를 스킵합니다.")
                continue

            reference_encodings.append(encodings[0])
            logger.debug(f"얼굴 인코딩 로드 완료 : {reference_image_path}")

        if not reference_encodings:
            logger.debug("참조 이미지에서 얼굴을 감지하지 못했습니다! 프로세스를 종료합니다.")
            return {"status": "error", "message": "No valid reference encodings available"}

        # 영상 처리 및 JSON 생성
        json_output_path = process_video(original_local_path, processed_local_path, reference_encodings)

        # 처리된 영상 및 JSON 업로드
        upload_to_minio(processed_local_path, f"{uuid}/processed.mp4")
        upload_to_minio(json_output_path, f"{uuid}/metadata.json")

        return {
            "status": "success",
            "uuid": uuid,
            "processed_video_url": f"http://localhost:9000/{bucket_name}/{uuid}/processed.mp4",
            "metadata_url": f"http://localhost:9000/{bucket_name}/{uuid}/metadata.json",
            "message": "Video processed and uploaded"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def process_video(input_path, output_path, reference_encodings, tolerance=0.55):
    """split 시 원본 영상을 처리하여 processed.mp4 및 metadata.json 생성"""
    state_manager.frames = []  # 프레임 리스트 초기화
    state_manager.processed_frames = []  # 편집된 프레임 리스트 초기화

    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        print("Error: Could not open video.")
        return None

    # FFmpeg을 사용하여 총 프레임 수 가져오기
    total_frames = get_frame_count_ffprobe(input_path)

    # 비디오 정보 가져오기
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))

    print(f"📌 영상 정보: 총 {total_frames} 프레임, FPS: {fps}, 해상도: {width}x{height}")

    # 저장할 비디오 설정
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    sequence_data = []  # JSON 저장을 위한 데이터

    frame_seq = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_seq += 1  # 정확한 프레임 번호 유지
        frame_info = {"seq": frame_seq, "person": []}

        results = model(frame, conf=0.4)  # YOLO 감지 실행
        detections = results[0].boxes

        for detection in detections:
            x1, y1, x2, y2 = map(int, detection.xyxy[0])
            x1, y1, x2, y2 = max(0, x1 - 10), max(0, y1 - 10), min(width, x2 + 10), min(height, y2 + 10)
            face_image = frame[y1:y2, x1:x2]

            if face_image.shape[0] < 20 or face_image.shape[1] < 20:
                frame = mosaic(frame, (x1, y1, x2 - x1, y2 - y1))
                frame_info["person"].append({"x1": x1, "x2": x2, "y1": y1, "y2": y2})
                continue

            face_location = [(y1, x2, y2, x1)]
            face_encodings = face_recognition.face_encodings(frame, known_face_locations=face_location, num_jitters=5)

            if face_encodings:
                face_encoding = face_encodings[0]
                matches = face_recognition.compare_faces(reference_encodings, face_encoding, tolerance=tolerance)
                if True in matches:
                    continue

            frame = mosaic(frame, (x1, y1, x2 - x1, y2 - y1))
            frame_info["person"].append({"x1": x1, "x2": x2, "y1": y1, "y2": y2})

        sequence_data.append(frame_info)
        out.write(frame)  # 모자이크 처리된 프레임 저장

    cap.release()
    out.release()

    # JSON 데이터 저장
    json_output_path = output_path.replace(".mp4", ".json")
    with open(json_output_path, "w") as json_file:
        json.dump({"sequence": sequence_data, "total_frames": total_frames, "fps": fps}, json_file, indent=4)

    logger.debug(f"Processed video saved as {output_path}")
    logger.debug(f"JSON metadata saved as {json_output_path}")

    return json_output_path

