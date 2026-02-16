import argparse
import json
import subprocess
import time
import random
import sys


EMOTIONS = [
    ("happy", 0.91),
    ("sad", 0.81),
    ("angry", 0.71),
    ("neutral", 0.66),
    ("surprise", 0.77),
]


def build_messages(n: int, source_id: str, start_frame_id: int, sleep_ms: int = 0) -> bytes:
    """
    Build Kafka payload: one JSON per line + final newline.
    producer_ts is computed PER MESSAGE to avoid identical timestamps.
    Optional sleep_ms slows down to spread events across time.
    """
    lines = []
    frame_id = start_frame_id

    for _ in range(n):
        emo, sc = random.choice(EMOTIONS)

        # IMPORTANT: timestamp computed inside loop (unique per msg)
        now = time.time()  # epoch seconds float

        msg = {
            "source_id": source_id,
            "frame_id": frame_id,
            "face_id": 1,
            "producer_ts": now,
            "emotion": emo,
            "score": sc,
            "bbox_x": 10 + (frame_id % 5),
            "bbox_y": 20 + (frame_id % 5),
            "bbox_w": 100,
            "bbox_h": 120,
        }

        lines.append(json.dumps(msg, ensure_ascii=False))
        frame_id += 1

        if sleep_ms and sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    payload_str = "\n".join(lines) + "\n"
    return payload_str.encode("utf-8")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=30, help="Number of messages to send")
    p.add_argument("--topic", type=str, default="test_topic", help="Kafka topic")
    p.add_argument("--container", type=str, default="final_project-kafka-1", help="Kafka container name")
    p.add_argument("--source-id", type=str, default="cam_01", dest="source_id", help="source_id field")
    p.add_argument("--start-frame-id", type=int, default=1001, dest="start_frame_id", help="Start frame_id")
    p.add_argument("--sleep-ms", type=int, default=0, help="Sleep N milliseconds between messages (optional)")
    p.add_argument("--broker", type=str, default="kafka:29092", help="Broker inside docker network")
    p.add_argument("--key", type=str, default="", help="Optional Kafka key (rarely needed)")
    args = p.parse_args()

    payload = build_messages(args.n, args.source_id, args.start_frame_id, args.sleep_ms)

    # kafka-console-producer reads lines from stdin
    # If you want a key: use --property parse.key=true --property key.separator=:
    if args.key:
        # prefix each line with "key:"
        payload_lines = payload.decode("utf-8").splitlines()
        payload_lines = [f"{args.key}:{line}" for line in payload_lines if line.strip()]
        payload = ("\n".join(payload_lines) + "\n").encode("utf-8")
        producer_cmd = (
            f'kafka-console-producer --broker-list {args.broker} --topic "{args.topic}" '
            f'--property parse.key=true --property key.separator=":"'
        )
    else:
        producer_cmd = f'kafka-console-producer --broker-list {args.broker} --topic "{args.topic}"'

    cmd = ["docker", "exec", "-i", args.container, "bash", "-lc", producer_cmd]

    print(f"Sending {args.n} messages to topic={args.topic} (container={args.container}, sleep_ms={args.sleep_ms}) ...")
    r = subprocess.run(cmd, input=payload)

    if r.returncode != 0:
        raise SystemExit(f"Producer failed with code {r.returncode}")

    # quick sanity check: number of lines sent
    sent_lines = payload.count(b"\n")
    print(f"Done. Lines sent = {sent_lines}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
