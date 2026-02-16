import time
import json
import subprocess

pairs = [
    ("happy", 0.91),
    ("sad", 0.81),
    ("angry", 0.71),
    ("neutral", 0.66),
    ("surprise", 0.77),
]

msgs = []
i = 1001
now = time.time()

for emo, sc in pairs:
    msg = {
        "source_id": "cam_01",
        "frame_id": i,
        "face_id": 1,
        "producer_ts": now,          # timestamp actuel
        "emotion": emo,
        "score": sc,
        "bbox_x": 10 + (i % 5),
        "bbox_y": 20 + (i % 5),
        "bbox_w": 100,
        "bbox_h": 120,
    }
    msgs.append(json.dumps(msg))
    i += 1

data = "\n".join(msgs) + "\n"

cmd = [
    "docker", "exec", "-i", "final_project-kafka-1",
    "bash", "-lc",
    "kafka-console-producer --broker-list kafka:29092 --topic test_topic"
]

print("Sending messages to Kafka...")
subprocess.run(cmd, input=data.encode("utf-8"), check=True)
print("Done.")
