# 로깅 설정
import logging
import sys


def setup_logging():
    """로깅 설정을 초기화합니다"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger(__name__)
