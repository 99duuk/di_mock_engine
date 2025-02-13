from dataclasses import dataclass, asdict


@dataclass
class CompleteVideoResult:
    status: str
    video_id: str
    finalized_video_url: str
    message: str

    def to_dict(self):
        return asdict(self)  # dataclass를 dict로 변환