import os
import shutil

def save_output(output_path, data, is_video=False):
    """
    변환된 결과물을 저장합니다.

    Args:
        output_path (str): 결과물이 저장될 경로.
        data (str): 저장할 데이터(프레임 디렉터리 또는 비디오 파일 경로).
        is_video (bool): 저장 대상이 비디오인지 여부.

    Returns:
        None
    """
    # 출력 디렉터리를 생성 (존재하지 않으면 생성)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if is_video:
        # 비디오 파일 저장
        shutil.copyfile(data, output_path)
    else:
        # 프레임 디렉터리 저장
        shutil.move(data, output_path)

    print(f"Saved output to {output_path}")
