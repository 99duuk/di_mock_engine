from dataclasses import dataclass, asdict


@dataclass
class VideoMetadata:
    width: int
    height: int
    fps: int
    total_frames: int
    duration: float

@dataclass
class ProcessedVideoResult:
    status: str
    video_id: str
    processed_video_url: str
    metadata_url: str
    message: str
    video_metadata: VideoMetadata

    def to_dict(self):
        return asdict(self)  # dataclass를 dict로 변환