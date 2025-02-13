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

#
# def get_video_info_ffprobe(video_path):
#     """FFmpeg의 ffprobe를 사용하여 비디오의 프레임 개수와 길이(초)를 가져오기"""
#     cmd = [
#         "ffprobe",
#         "-v", "error",
#         "-count_frames",
#         "-select_streams", "v:0",
#         "-show_entries", "stream=nb_read_frames,duration",
#         "-of", "csv=p=0",
#         video_path
#     ]
#     output = subprocess.check_output(cmd).decode("utf-8").strip().split("\n")
#
#     # 결과 파싱
#     frame_counts = int(output[0].split(",")[0]) if output[0] else 0
#     duration = float(output[0].split(",")[1]) if len(output[0].split(",")) > 1 else 0.0
#
#     return frame_counts, duration

def get_video_info_ffprobe(video_path):
    """FFmpeg의 ffprobe를 사용하여 비디오의 프레임 개수와 길이(초)를 가져오기"""

    # 정확한 프레임 개수 가져오기
    cmd_frames = [
        "ffprobe",
        "-v", "error",
        "-count_frames",
        "-select_streams", "v:0",
        "-show_entries", "stream=nb_read_frames",
        "-of", "csv=p=0",
        video_path
    ]
    frame_count_output = subprocess.check_output(cmd_frames).decode("utf-8").strip()
    frame_counts = int(frame_count_output) if frame_count_output.isdigit() else 0  # 값이 비었을 경우 대비

    # 비디오 길이 가져오기
    cmd_duration = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=duration",
        "-of", "csv=p=0",
        video_path
    ]
    duration_output = subprocess.check_output(cmd_duration).decode("utf-8").strip()
    duration = float(duration_output) if duration_output else 0.0

    return frame_counts, duration
