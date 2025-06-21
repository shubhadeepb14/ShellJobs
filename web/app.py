from flask import Flask, request, jsonify
import redis
import json
import uuid
import time

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

TASK_QUEUE = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"
RESULTS_KEY = "task_results"

@app.route("/enqueue", methods=["POST"])
def enqueue():
    data = request.json
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "device_id": data["device_id"],
        "command": data["command"],
        "timeout": data.get("timeout", 0),
        "tags": data.get("tags", []),
        "retries": data.get("retries", 0),
        "retries_left": data.get("retries", 0),
        "attempts": 0,
        "last_attempt": None
    }
    r.rpush(TASK_QUEUE, json.dumps(task))
    return {"status": "queued", "task_id": task_id}

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    response = {"status": "pending"}
    result = r.hget(RESULTS_KEY, task_id)
    if result:
        response.update(json.loads(result))

    task_json = r.hget(DEAD_LETTER_QUEUE, task_id)
    if task_json:
        response["status"] = "dead"

    return response

@app.route("/retry-dead/<task_id>", methods=["POST"])
def retry_dead(task_id):
    dead_tasks = r.lrange(DEAD_LETTER_QUEUE, 0, -1)
    for task_json in dead_tasks:
        task = json.loads(task_json)
        if task.get("id") == task_id:
            r.lrem(DEAD_LETTER_QUEUE, 1, task_json)
            task["retries"] = 3
            task["attempts"] = 0
            task["last_attempt"] = None
            r.rpush(TASK_QUEUE, json.dumps(task))
            return {"status": "retried"}
    return {"error": "Task not found"}, 404

@app.route("/delete/<task_id>", methods=["DELETE"])
def delete_task(task_id):
    tasks = r.lrange(TASK_QUEUE, 0, -1)
    for task_json in tasks:
        task = json.loads(task_json)
        if task.get("id") == task_id:
            r.lrem(TASK_QUEUE, 1, task_json)
            return {"status": "deleted from pending queue"}

    dead_tasks = r.lrange(DEAD_LETTER_QUEUE, 0, -1)
    for task_json in dead_tasks:
        task = json.loads(task_json)
        if task.get("id") == task_id:
            r.lrem(DEAD_LETTER_QUEUE, 1, task_json)
            return {"status": "deleted from dead letter queue"}

    return {"error": "Task not found"}, 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)