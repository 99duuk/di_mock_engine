import cv2
import os

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
