class StateManager:
    """전역 상태를 관리하는 클래스 (싱글톤 패턴)"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StateManager, cls).__new__(cls)
            cls._instance.reset()  # 초기화 함수 호출
        return cls._instance

    def reset(self):
        """초기 상태를 설정"""
        self.boxes = []  # 박스 좌표를 저장하는 리스트
        self.frames = []  # 원본 프레임을 저장하는 리스트
        self.processed_frames = []  # 편집된 프레임을 저장하는 리스트
        self.current_frame_idx = 0  # 현재 프레임 인덱스
        self.export_requested = False  # 내보내기 요청 상태

# 싱글톤 인스턴스 생성
state_manager = StateManager()
