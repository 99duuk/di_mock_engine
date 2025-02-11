import mediapipe as mp
from ultralytics import YOLO

# YOLO 모델 및 MediaPipe FaceMesh 초기화 (최초 1회 실행)
class ModelLoader:
    _yolo_model = None
    _mp_face_mesh = None

    @classmethod
    def get_yolo_model(cls):
        """YOLO 모델을 로드하여 반환 (싱글톤 패턴)"""
        if cls._yolo_model is None:
            cls._yolo_model = YOLO('yolov8m-face-lindevs.pt')
        return cls._yolo_model

    @classmethod
    def get_mp_face_mesh(cls):
        """MediaPipe FaceMesh 모델을 로드하여 반환 (싱글톤 패턴)"""
        if cls._mp_face_mesh is None:
            cls._mp_face_mesh = mp.solutions.face_mesh.FaceMesh()
        return cls._mp_face_mesh
