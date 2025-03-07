import tensorflow as tf
print(tf.config.list_physical_devices('GPU'))  # Metal GPU가 인식되는지 확인
import torch
from ultralytics import YOLO
import mediapipe as mp

class ModelLoader:
    _yolo_model = None
    _tflite_interpreter = None
    _device = None

    @classmethod
    def _initialize_device(cls):
        if cls._device is None:
            if torch.backends.mps.is_available():
                cls._device = 'mps'
                print("Using MPS (Metal Performance Shaders) for GPU acceleration")
            else:
                cls._device = 'cpu'
                print("MPS not available, falling back to CPU")
        return cls._device

    @classmethod
    def get_yolo_model(cls):
        if cls._yolo_model is None:
            device = cls._initialize_device()
            cls._yolo_model = YOLO('yolov8m-face-lindevs.pt')
            cls._yolo_model.to(device)
        return cls._yolo_model

    @classmethod
    def get_tflite_face_mesh(cls):
        if cls._tflite_interpreter is None:
            try:
                # GPU Delegate 대신 기본 인터프리터로 설정 (macOS에서 Metal 지원 확인 필요)
                cls._tflite_interpreter = tf.lite.Interpreter(model_path="face_landmark.tflite")
                cls._tflite_interpreter.allocate_tensors()
                print("TensorFlow Lite FaceMesh loaded (CPU)")
            except Exception as e:
                print(f"Failed to load TFLite FaceMesh: {e}")
                raise
        return cls._tflite_interpreter

    @classmethod
    def get_mp_face_mesh(cls):
        if cls._mp_face_mesh is None:
            cls._mp_face_mesh = mp.solutions.face_mesh.FaceMesh(
                max_num_faces=10,
                refine_landmarks=True,
                min_detection_confidence=0.5,
                min_tracking_confidence=0.5
            )
        return cls._mp_face_mesh