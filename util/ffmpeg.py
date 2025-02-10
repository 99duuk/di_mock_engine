import subprocess

def get_frame_count_ffprobe(video_path):
    """FFmpeg의 ffprobe를 사용하여 비디오의 정확한 프레임 개수 가져오기"""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-count_frames",
        "-select_streams", "v:0",
        "-show_entries", "stream=nb_read_frames",
        "-of", "csv=p=0",
        video_path
    ]
    output = subprocess.check_output(cmd).decode("utf-8").strip()
    print(f"🎯 FFmpeg 기반 정확한 프레임 개수: {int(output)}")
    return int(output)

