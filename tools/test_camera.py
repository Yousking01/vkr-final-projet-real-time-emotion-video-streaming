import cv2

for idx in range(3):
    cap = cv2.VideoCapture(idx, cv2.CAP_DSHOW)
    ok = cap.isOpened()
    print("index", idx, "opened", ok)
    if ok:
        ret, frame = cap.read()
        print("read", ret, "shape", None if frame is None else frame.shape)
    cap.release()