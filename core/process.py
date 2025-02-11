import cv2


# 모자이크 처리
def mosaic(img, rect, size=15):
    """얼굴 영역에 모자이크 처리."""
    (x, y, w, h) = rect
    sub_img = img[y:y + h, x:x + w]
    sub_img = cv2.resize(sub_img, (size, size), interpolation=cv2.INTER_LINEAR)
    mosaic_img = cv2.resize(sub_img, (w, h), interpolation=cv2.INTER_LINEAR)
    img[y:y + h, x:x + w] = mosaic_img
    return img
