import json
import os
import subprocess

import cv2

from core.process import mosaic
from interface.minio_client import upload_to_minio, download_from_minio
from model.complete_message import CompleteVideoResult
from util.log_util import setup_logging
from util.state_manager import state_manager

logger = setup_logging()


def process_finalize_request(message):
    """Kafka 메시지에서 merge 요청을 처리"""
    try:
        uuid = message.get("requestId")
        bucket_name = message.get("bucket_name", "di-bucket")
        output_dir = os.path.join("output", uuid)

        os.makedirs(output_dir, exist_ok=True)

        original_local_path = os.path.join(output_dir, "original.mp4")
        processed_local_path = os.path.join(output_dir, "processed.mp4")
        metadata_local_path = os.path.join(output_dir, "metadata.json")
        edited_metadata_local_path = os.path.join(output_dir, "edited_metadata.json")
        final_output_path = os.path.join(output_dir, "final.mp4")

        print("Processing merge operation...")

        download_from_minio(f"{uuid}/processed.mp4", processed_local_path)
        download_from_minio(f"{uuid}/metadata.json", metadata_local_path)
        download_from_minio(f"{uuid}/edited_metadata.json", edited_metadata_local_path)

        tasks = compare_metadata(metadata_local_path, edited_metadata_local_path)

        if tasks is None:
            return {"status": "error", "message": "Metadata comparison failed"}

        if not tasks:
            os.rename(processed_local_path, final_output_path)
        else:
            download_from_minio(f"{uuid}/original.mp4", original_local_path)

            with open(metadata_local_path) as f:
                metadata = json.load(f)
                fps = metadata['fps']

            videos, frames = extract_modified_intervals_and_frame(
                original_local_path, processed_local_path, tasks, output_dir, fps, uuid
            ) or ([], [])

            merge_videos(processed_local_path, videos, frames, final_output_path)

            return {
                "status": "success",
                "video_id": uuid,
                "finalized_video_url": f"http://localhost:9000/{bucket_name}/{uuid}/final.mp4",
                "message": "Video finalized and uploaded"
            }

    # 최종 영상 업로드
        if not os.path.exists(final_output_path):
            logger.error(f"Error: Final video file not found at {final_output_path}")
            return CompleteVideoResult(
                status="error",
                video_id=uuid,
                finalized_video_url="",
                message="Final video was not created."
            )
        upload_to_minio(final_output_path, f"{uuid}/final.mp4")

    except Exception as e:
        return CompleteVideoResult(
            status="error",
            video_id=uuid,
            finalized_video_url="",
            message=str(e)
        )


# --------------------------------------------------------------------------------------------------------------


def compare_metadata(original_metadata_path, edited_metadata_path):
    """
    기존 metadata.json과 edited_metadata.json을 비교하여 모자이크 추가/제거가 필요한 부분을 찾음.
    """
    logger.info(f"Comparing metadata files: {original_metadata_path} and {edited_metadata_path}")

    # JSON 파일 읽기
    with open(original_metadata_path, 'r') as f:
        original_data = json.load(f)  # 원본 metadata 로드

    with open(edited_metadata_path, 'r') as f:
        edited_data = json.load(f)  # 편집된 metadata 로드

    tasks = []  # 수행해야 할 작업 목록

    # metadata.json과 edited_metadata.json을 dict 형태로 변환
    original_dict = {seq_data["seq"]: seq_data["person"] for seq_data in original_data["sequence"]}
    edited_dict = {seq_data["seq"]: seq_data["person"] for seq_data in edited_data["sequence"]}

    for seq_number, edited_boxes in edited_dict.items():
        orig_boxes = original_dict.get(seq_number, [])

        remove_boxes = []  # 제거할 영역
        add_boxes = []  # 추가할 영역

        for edited in edited_boxes:
            ex1, ex2, ey1, ey2 = edited["x1"], edited["x2"], edited["y1"], edited["y2"]
            found_exact_match = False  # 완전히 일치하는 좌표 여부 확인

            for meta in orig_boxes:
                mx1, mx2, my1, my2 = meta["x1"], meta["x2"], meta["y1"], meta["y2"]

                #  A가 정확히 존재하면 전체 제거
                if (mx1 == ex1 and mx2 == ex2 and my1 == ey1 and my2 == ey2):
                    found_exact_match = True
                    remove_boxes.append(edited)  # 정확한 일치만 제거
                    continue

                #  A ∩ B (교집합) 확인
                ix1 = max(mx1, ex1)
                iy1 = max(my1, ey1)
                ix2 = min(mx2, ex2)
                iy2 = min(my2, ey2)

                if ix1 < ix2 and iy1 < iy2:
                    #  (A U B) - A의 차집합 연산 수행
                    # **겹친 부분을 남기고 나머지를 제거**
                    new_meta_boxes = []

                    if mx1 < ix1:  # A의 왼쪽 부분 제거
                        remove_boxes.append({"x1": mx1, "x2": ix1, "y1": my1, "y2": my2})
                    else:
                        new_meta_boxes.append({"x1": mx1, "x2": ix1, "y1": my1, "y2": my2})

                    if mx2 > ix2:  # A의 오른쪽 부분 제거
                        remove_boxes.append({"x1": ix2, "x2": mx2, "y1": my1, "y2": my2})
                    else:
                        new_meta_boxes.append({"x1": ix2, "x2": mx2, "y1": my1, "y2": my2})

                    if my1 < iy1:  # A의 위쪽 부분 제거
                        remove_boxes.append({"x1": mx1, "x2": mx2, "y1": my1, "y2": iy1})
                    else:
                        new_meta_boxes.append({"x1": mx1, "x2": mx2, "y1": iy1, "y2": my2})

                    if my2 > iy2:  # A의 아래쪽 부분 제거
                        remove_boxes.append({"x1": mx1, "x2": mx2, "y1": iy2, "y2": my2})
                    else:
                        new_meta_boxes.append({"x1": mx1, "x2": mx2, "y1": my1, "y2": iy2})

                    #  교집합 부분을 다시 추가
                    for box in new_meta_boxes:
                        if box["x1"] < box["x2"] and box["y1"] < box["y2"]:
                            add_boxes.append(box)

        # 기존 `metadata` 좌표를 유지하고, 새로운 좌표만 추가해야 함
        orig_set = {tuple((p["x1"], p["x2"], p["y1"], p["y2"])) for p in orig_boxes}
        edited_set = {tuple((p["x1"], p["x2"], p["y1"], p["y2"])) for p in edited_boxes}
        add_boxes.extend([{"x1": x1, "x2": x2, "y1": y1, "y2": y2} for (x1, x2, y1, y2) in (edited_set - orig_set)])

        # **디버깅: 삭제 및 추가 박스 정보 출력**
        logger.debug(f"Frame {seq_number} REMOVE {remove_boxes}")
        logger.debug(f"Frame {seq_number} ADD {add_boxes}")

        # 변경사항이 있는 경우에만 tasks 추가
        if remove_boxes or add_boxes:
            tasks.append({
                "seq": seq_number,  # 프레임 번호
                "remove": remove_boxes,  # 기존 모자이크 제거
                "add": add_boxes  # 새롭게 추가할 모자이크
            })
            logger.info(f"Frame {seq_number}: {len(remove_boxes)} boxes to remove, {len(add_boxes)} boxes to add")

    logger.info(f"Total tasks found: {len(tasks)}")
    return tasks


def process_frame(original_video, processed_video, tasks):
    """프레임별로 모자이크를 추가하거나 제거합니다."""
    logger.info(f"Processing frames with {len(tasks)} tasks")
    cap_orig = cv2.VideoCapture(original_video)
    cap_proc = cv2.VideoCapture(processed_video)

    if not cap_orig.isOpened() or not cap_proc.isOpened():
        logger.error("Failed to open video files")
        return []

    edited_frames = []  # 편집된 프레임을 저장할 리스트
    frame_map = {}  # 중복 저장 방지를 위한 딕셔너리
    current_seq = 1  # 현재 프레임 번호

    # 연속된 "No task found" 로깅을 위한 변수
    no_task_start = None  # 연속된 구간 시작점
    no_task_end = None  # 연속된 구간 끝점

    while True:
        # 원본과 처리된 비디오에서 프레임 읽기
        ret_orig, frame_orig = cap_orig.read()
        ret_proc, frame_proc = cap_proc.read()

        # 더 이상 읽을 프레임이 없으면 종료
        if not ret_orig or not ret_proc:
            break  # 프레임 끝

        # 현재 프레임의 task 찾기
        current_task = next((t for t in tasks if t['seq'] == current_seq), None)

        if not current_task:  # Task가 없는 경우
            if no_task_start is None:
                no_task_start = current_seq  # 처음 발견한 경우 시작점 설정
            no_task_end = current_seq  # 끝점 갱신
            current_seq += 1
            continue  # current_task가 없으면 스킵하여 오류 방지

        # 이전까지 연속된 "No task found" 프레임이 있었다면 로그 출력
        if no_task_start is not None:
            if no_task_start == no_task_end:
                logger.info(f"No task found for frame {no_task_start}, skipping processing.")
            else:
                logger.info(f"No task found for frames {no_task_start} ~ {no_task_end}, skipping processing.")
            no_task_start = None  # 초기화
            no_task_end = None


        logger.info(f"Processing frame {current_seq}")

        # 현재 프레임 복사
        frame = frame_proc.copy()

        # 모자이크 제거: 원본 프레임에서 해당 영역을 복사
        for box in current_task['remove']:
            x1, y1, x2, y2 = box['x1'], box['y1'], box['x2'], box['y2']
            logger.debug(f"Removing mosaic at {x1},{y1},{x2},{y2}")
            frame[y1:y2, x1:x2] = frame_orig[y1:y2, x1:x2]

        # 모자이크 추가: 새로운 영역에 모자이크 적용
        for box in current_task['add']:
            x1, y1, x2, y2 = box['x1'], box['y1'], box['x2'], box['y2']
            logger.debug(f"Adding mosaic at {x1},{y1},{x2},{y2}")
            frame = mosaic(frame, (x1, y1, x2 - x1, y2 - y1))

        # 중복 방지 및 저장 (같은 프레임이 여러 번 저장되지 않도록)
        if current_seq not in frame_map:
            frame_map[current_seq] = frame
            edited_frames.append((current_seq, frame))

        current_seq += 1  # 다음 프레임으로 이동

    # 마지막 "No task found" 프레임이 있었다면 로그 출력
    if no_task_start is not None:
        if no_task_start == no_task_end:
            logger.info(f"No task found for frame {no_task_start}, skipping processing.")
        else:
            logger.info(f"No task found for frames {no_task_start} ~ {no_task_end}, skipping processing.")

    # 비디오 파일 닫기
    cap_orig.release()
    cap_proc.release()
    logger.info(f"Total frames processed: {len(edited_frames)}")
    return edited_frames



def join_frame(frames, fps, output_path):
    """
    연속된 프레임들을 하나의 비디오 파일로 변환합니다.

    Args:
        frames (list): 비디오로 만들 프레임들의 리스트
        fps (int): 출력 비디오의 초당 프레임 수
        output_path (str): 출력 비디오 파일 경로

    Returns:
        str: 생성된 비디오 파일 경로 또는 실패 시 None
    """
    if not frames:
        return None

    # 첫 번째 프레임에서 비디오 크기 가져오기
    height, width = frames[0].shape[:2]

    # VideoWriter 설정
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # MP4 코덱 설정
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    # 각 프레임을 비디오에 쓰기
    for frame in frames:
        out.write(frame)

    out.release()  # 비디오 파일 닫기
    return output_path



def extract_modified_intervals_and_frame(original_video, processed_video, tasks, output_dir, fps, uuid):
    """
    편집이 필요한 구간을 찾아 연속된 프레임은 비디오로, 단일 프레임은 이미지로 저장합니다.

    Args:
        original_video (str): 원본 비디오 파일 경로
        processed_video (str): 모자이크 처리된 비디오 파일 경로
        tasks (list): compare_metadata에서 반환된 작업 목록
        output_dir (str): 출력 파일들을 저장할 디렉토리 경로
        fps (int): 비디오의 초당 프레임 수

    Returns:
        tuple: (videos, single_frames) - 편집된 비디오 파일 목록과 단일 프레임 이미지 파일 목록
    """
    # 프레임 편집 수행
    edited_frames = process_frame(original_video, processed_video, tasks)

    if not edited_frames:
        return [], []

    videos = []          # 저장된 비디오 파일 경로 리스트
    single_frames = []    # 저장된 단일 프레임 이미지 경로 리스트
    current_sequence = [] # 현재 처리 중인 연속 프레임들
    last_seq = edited_frames[0][0]  # 마지막으로 처리한 프레임 번호

    # 편집된 프레임들을 순회하면서 연속된 구간 찾기
    for seq, frame in edited_frames:
        if seq == last_seq + 1 and current_sequence:
            # 연속된 프레임인 경우 현재 시퀀스에 추가
            current_sequence.append(frame)
        else:
            # 연속이 끊긴 경우 처리
            if len(current_sequence) > 1:
                # 2개 이상의 프레임은 비디오로 저장
                video_path = os.path.join(output_dir, f'edited_sequence_{last_seq-len(current_sequence)+1}_{last_seq}.mp4')
                join_frame(current_sequence, fps, video_path)
                videos.append(video_path)

                # MinIO에 업로드
                minio_path = f"{uuid}/temp/edited_sequence_{last_seq-len(current_sequence)+1}_{last_seq}.mp4"
                upload_to_minio(video_path, minio_path)

            elif len(current_sequence) == 1:
                # 단일 프레임은 이미지로 저장
                frame_path = os.path.join(output_dir, f'edited_frame_{last_seq}.jpg')
                cv2.imwrite(frame_path, current_sequence[0])
                single_frames.append(frame_path)

                # MinIO에 업로드
                minio_path = f"{uuid}/temp/edited_frame_{last_seq}.jpg"
                upload_to_minio(frame_path, minio_path)


            # 새로운 시퀀스 시작
            current_sequence = [frame]
        last_seq = seq

    # 마지막 시퀀스 처리
    if len(current_sequence) > 1:
        video_path = os.path.join(output_dir, f'edited_sequence_{last_seq-len(current_sequence)+1}_{last_seq}.mp4')
        join_frame(current_sequence, fps, video_path)
        videos.append(video_path)

        minio_path = f"{uuid}/temp/edited_sequence_{last_seq-len(current_sequence)+1}_{last_seq}.mp4"
        upload_to_minio(video_path, minio_path)

    elif len(current_sequence) == 1:
        frame_path = os.path.join(output_dir, f'edited_frame_{last_seq}.jpg')
        cv2.imwrite(frame_path, current_sequence[0])
        single_frames.append(frame_path)

        # MinIO에 업로드


        # MinIO에 업로드
        minio_path = f"{uuid}/temp/edited_frame_{last_seq}.jpg"
        upload_to_minio(frame_path, minio_path)


    return videos, single_frames

# --------------------------------------------------------------------------------------------------------------

def extract_frame_numbers(file_path):
    """edited_sequence_425_441.mp4 -> (425, 441)"""
    base = os.path.basename(file_path)
    parts = base.replace("edited_sequence_", "").replace(".mp4", "").split("_")
    return int(parts[0]), int(parts[1])

def extract_frame_number(file_path):
    """edited_frame_443.jpg -> 443"""
    base = os.path.basename(file_path)
    return int(base.replace("edited_frame_", "").replace(".jpg", ""))

# --------------------------------------------------------------------------------------------------------------

def merge_videos(original_video, edited_videos, edited_frames, final_output):
    """
    OpenCV와 FFmpeg를 결합하여 필요한 프레임만 교체한 후 최종 비디오 생성
    """
    state_manager.current_frame_idx = 0  # 인덱스 초기화
    state_manager.export_requested = False  # 상태 초기화
    logger.info(f" Merging video: {original_video}")

    cap = cv2.VideoCapture(original_video)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    logger.info(f"🎥 Original Video: {total_frames} frames, {fps} FPS, {width}x{height}")

    temp_output = "temp_output.mp4"

    # 편집된 비디오 & 프레임 매핑
    edited_video_map = {extract_frame_numbers(v): v for v in edited_videos}
    edited_frame_map = {extract_frame_number(f): f for f in edited_frames}

    logger.info(f"📌 Found {len(edited_video_map)} edited sequences and {len(edited_frame_map)} edited frames.")

    out = cv2.VideoWriter(temp_output, cv2.VideoWriter_fourcc(*'mp4v'), fps, (width, height))

    frame_idx = 0
    while frame_idx < total_frames:
        ret, frame = cap.read()
        if not ret:
            break

        frame_idx += 1

        # 1. edited_frame_*.jpg가 있는 경우 교체
        if frame_idx in edited_frame_map:
            edited_frame = cv2.imread(edited_frame_map[frame_idx])
            frame = cv2.resize(edited_frame, (width, height))

        # 2. edited_sequence_*.mp4의 구간에 해당하면 해당 영상에서 가져와 교체
        for (start_seq, end_seq), video_path in edited_video_map.items():
            if start_seq <= frame_idx <= end_seq:
                edit_cap = cv2.VideoCapture(video_path)
                frame_offset = frame_idx - start_seq  # 해당 비디오의 몇 번째 프레임인지 계산
                edit_cap.set(cv2.CAP_PROP_POS_FRAMES, frame_offset)
                ret_edit, edited_frame = edit_cap.read()
                edit_cap.release()

                if ret_edit:
                    frame = cv2.resize(edited_frame, (width, height))
                break  # 덮어씌웠으면 종료

        out.write(frame)

    cap.release()
    out.release()

    #  🎥 FFmpeg로 최종 압축 (오디오 유지)
    cmd = [
        "ffmpeg",
        "-y",
        "-i", temp_output,
        "-i", original_video,  # 원본에서 오디오 유지
        "-c:v", "libx264",
        "-c:a", "copy",
        "-map", "0:v:0",
        "-map", "1:a:0?",
        final_output
    ]
    subprocess.run(cmd, check=True)

    #  임시 파일 삭제
    os.remove(temp_output)

    logger.info(f" Final merged video saved: {final_output}")

