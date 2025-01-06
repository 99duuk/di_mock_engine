import cv2
import os

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
    frame_files = sorted([os.path.join(frame_dir, f) for f in os.listdir(frame_dir) if f.endswith(".jpg")])
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
