import os
from datetime import datetime


def get_unique_dirname(dir_path):
    """ 중복된 디렉터리 이름을 처리하여 고유한 이름 생성. """
    if not os.path.exists(dir_path):
        return dir_path

    base_dir = dir_path
    counter = 1
    while os.path.exists(dir_path):
        dir_path = f"{base_dir}_{counter}"
        counter += 1
    return dir_path


def get_unique_filename(file_path):
    """ 중복된 파일 이름을 처리하여 고유한 이름 생성. """
    if not os.path.exists(file_path):
        return file_path

    base_name, ext = os.path.splitext(file_path)
    counter = 1
    while os.path.exists(file_path):
        file_path = f"{base_name}_{counter}{ext}"
        counter += 1
    return file_path




def get_output_paths(input_source, output_dir, request_id):
    """
    출력 경로를 requestId와 타임스탬프 기반으로 생성.

    Args:
        input_source (str): 입력 비디오 경로 또는 URL (파일 이름은 무시됨).
        output_dir (str): 출력 디렉터리 경로.
        request_id (str): 요청 ID.

    Returns:
        tuple: 다운로드된 비디오 경로, 프레임 출력 디렉터리, 병합된 비디오 출력 파일 경로.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    base_name = f"{request_id}_{timestamp}"

    frames_output_dir = get_unique_dirname(os.path.join(output_dir, f"{base_name}"))
    downloaded_video_path = get_unique_filename(os.path.join(frames_output_dir+"/video", f"{base_name}.mp4"))
    video_output_path = get_unique_filename(os.path.join(frames_output_dir, f"{base_name}_output.mp4"))

    return (
        os.path.abspath(downloaded_video_path),
        os.path.abspath(frames_output_dir),
        os.path.abspath(video_output_path),
    )


