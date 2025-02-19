import json
import os

import cv2

from interface.minio_client import upload_to_minio, download_from_minio
from util.log_util import setup_logging

logger = setup_logging()


def apply_mosaic(frame, boxes):
    """지정된 영역에 모자이크 적용"""
    for box in boxes:
        x1, y1, x2, y2 = box["x1"], box["y1"], box["x2"], box["y2"]
        roi = frame[y1:y2, x1:x2]
        mosaic_size = (10, 10)  # 모자이크 크기 조절 가능
        roi = cv2.resize(roi, mosaic_size, interpolation=cv2.INTER_LINEAR)
        roi = cv2.resize(roi, (x2 - x1, y2 - y1), interpolation=cv2.INTER_NEAREST)
        frame[y1:y2, x1:x2] = roi
    return frame


def finalize_video(message, bucket_name="di-bucket"):
    """원본 및 편집된 메타데이터를 바탕으로 모자이크 적용 후 최종 영상 생성"""
    try:
        video_id = message["video_id"]
        output_dir = os.path.join("output", video_id)
        os.makedirs(output_dir, exist_ok=True)

        # 다운로드
        original_video = os.path.join(output_dir, "original.mp4")
        metadata_path = os.path.join(output_dir, "edited_metadata.json")
        final_output_path = os.path.join(output_dir, "final.mp4")

        download_from_minio(f"{video_id}/original.mp4", original_video)
        download_from_minio(f"{video_id}/edited_metadata.json", metadata_path)

        # 메타데이터 로드
        with open(metadata_path) as f:
            metadata = json.load(f)

        cap = cv2.VideoCapture(original_video)
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        logger.info(f"Processing video {video_id}: {total_frames} frames, {fps} FPS")

        # 최종 비디오 저장 설정
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        out = cv2.VideoWriter(final_output_path, fourcc, fps, (width, height))

        frame_seq = 0
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            # 프레임 번호에 해당하는 'person' 정보 가져오기
            frame_seq += 1
            boxes = next((seq["person"] for seq in metadata["sequence"] if seq.get("seq") == frame_seq), [])
            # boxes = next((seq["person"] for seq in metadata["sequence"] if seq["seq"] == frame_seq), None)4


            if boxes:
                frame = apply_mosaic(frame, boxes)

            out.write(frame)

        cap.release()
        out.release()

        # 최종 비디오 업로드
        upload_to_minio(final_output_path, f"{video_id}/final.mp4")

        return {
            "status": "success",
            "video_id": video_id,
            "finalized_video_url": f"http://localhost:9000/{bucket_name}/{video_id}/final.mp4",
            "message": "Video finalized and uploaded"
        }

    except Exception as e:
        logger.error(f"Error processing video {video_id}: {str(e)}")
        return {"status": "error", "video_id": video_id, "message": str(e)}
