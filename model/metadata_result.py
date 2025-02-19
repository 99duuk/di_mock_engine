
from dataclasses import dataclass

@dataclass
class MetadataResult:
    json_output_path: str
    total_frames: int
    fps: int
    width: int
    height: int
    duration: float