import time
import subprocess
import redis
import json
import threading

TASK_QUEUE = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"
RESULTS_KEY = "task_results"
RETRY_INTERVAL = 5  # seconds
READ_INTERVAL = 1  # seconds

print("[Main] Agent started. Listening for tasks...")

DEVICE_ID = "raspi-01"
TAGS = ["group1", "sensor"]
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def process_task(task):
    print(f"[Task] Started Task {task['id']}")
    try:
        timeout = task.get("timeout", 0) or None
        result = subprocess.run(task["command"], shell=True, capture_output=True, text=True, timeout=timeout)
        status = "success" if result.returncode == 0 else "error"
        output = result.stdout + result.stderr
    except Exception as e:
        status = "error"
        output = str(e)

    task["attempts"] = task.get("attempts", 0) + 1
    task["last_attempt"] = time.time()

    if status == "error":
        if task.get("retries_left", 0) > 0:
            task["retries_left"] -= 1
            print(f"[Task] Failed {task['id']}, retrying {task['retries_left']} more times.")
            r.rpush(TASK_QUEUE, json.dumps(task))
        else:
            print(f"[Task] Dead {task['id']} after {task['attempts']} attempts.")
            r.hset(DEAD_LETTER_QUEUE, task["id"], json.dumps(task))

    r.hset(RESULTS_KEY, task["id"], json.dumps({
        "status": status,
        "output": output,
        "attempts": task['attempts'],
        "retries": task.get("retries", 0),
        "retries_left": task.get("retries_left", 0),
        "last_attempt": task.get("last_attempt", None)
    }))
    print(f"[Task] {status} {task['id']} : {output}")

while True:
    try:
        tasks = r.lrange(TASK_QUEUE, 0, -1)
        task_found = False
        for task_json in tasks:
            task = json.loads(task_json)
            if task.get("last_attempt") and time.time() - task["last_attempt"] < RETRY_INTERVAL:
                continue
            if task["device_id"] == DEVICE_ID or any(tag in task.get("tags", []) for tag in TAGS):
                r.lrem(TASK_QUEUE, 1, task_json)
                threading.Thread(target=process_task, args=(task,), daemon=True).start()
                task_found = True
                break

        time.sleep(READ_INTERVAL)
    except Exception as e:
        print("[Task] Agent Error:", e)
        time.sleep(READ_INTERVAL)