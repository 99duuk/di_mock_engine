import yt_dlp
import os

def download_video(url, output_path):
    """
    유튜브 URL에서 최고 화질 비디오를 다운로드합니다.

    Args:
        url (str): 다운로드할 비디오의 URL.
        output_path (str): 저장될 비디오 파일 경로.

    Returns:
        None
    """
    ydl_opts = {
        'outtmpl': output_path,                # 출력 파일 경로
        'format': 'bestvideo[height<=1080]+bestaudio/best',  # 1080 비디오+오디오 병합(병합 실패시 단일 파일 다운로드)
        'merge_output_format': 'mp4',         # 병합된 파일 형식을 MP4로 설정
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])  # 비디오 다운로드
    print(f"Downloaded video to {output_path}")


def load_video(video_path):
    """
    로컬 비디오 파일 경로를 확인하여 반환합니다.

    Args:
        video_path (str): 비디오 파일 경로.

    Returns:
        str: 비디오 파일 경로.

    Raises:
        FileNotFoundError: 파일이 존재하지 않으면 발생.
    """
    if os.path.exists(video_path):
        return video_path
    else:
        raise FileNotFoundError(f"Video file not found: {video_path}")
