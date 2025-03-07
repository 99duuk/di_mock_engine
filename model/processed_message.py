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
    metadata_url: str
    message: str
    video_metadata: VideoMetadata

    def to_dict(self):
        return asdict(self)  # dataclass를 dict로 변환