import time, json, subprocess

pairs = [("happy", 0.91), ("sad", 0.81), ("angry", 0.71), ("neutral", 0.66), ("surprise", 0.77)]
lines = []
frame_base = int(time.time()) % 100000

for i, (emo, sc) in enumerate(pairs, start=1):
    msg = {
        "source_id": "cam_01",
        "frame_id": frame_base + i,
        "face_id": 1,
        "producer_ts": time.time(),  # REAL epoch seconds
        "emotion": emo,
        "score": sc,
        "bbox_x": 10 + i,
        "bbox_y": 20 + i,
        "bbox_w": 100,
        "bbox_h": 120
    }
    lines.append(json.dumps(msg))

data = "\n".join(lines) + "\n"

subprocess.run(
    ["docker", "exec", "-i", "final_project-kafka-1", "bash", "-lc",
     "kafka-console-producer --broker-list kafka:29092 --topic test_topic"],
    input=data.encode("utf-8"),
    check=True
)

print("Sent", len(lines), "messages to Kafka.")
