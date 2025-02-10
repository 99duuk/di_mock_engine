import array
from dataclasses import dataclass

@dataclass
class TimelineMessage:
    videoId: str
    message: str
    fps: int
    totalFrameCount: int

