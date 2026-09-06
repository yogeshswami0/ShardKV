"""
ShardKV Dashboard Backend

Flask UI over the TCP binary protocol (not gRPC).
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os
import sys
import time
import subprocess
import threading
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
import tcp_kv

app = Flask(__name__, static_folder="static")
CORS(app)

ROOT = Path(__file__).resolve().parent.parent
RUN_DIR = ROOT / "run"

NODES = {
    "node1": {"address": "127.0.0.1:7071", "id": 1},
    "node2": {"address": "127.0.0.1:7072", "id": 2},
    "node3": {"address": "127.0.0.1:7073", "id": 3},
}

if os.environ.get("SHARD_IN_DOCKER") == "1":
    NODES = {
        "node1": {"address": "node1:7070", "id": 1},
        "node2": {"address": "node2:7070", "id": 2},
        "node3": {"address": "node3:7070", "id": 3},
    }

activity_log = []
MAX_LOG_ENTRIES = 100
_start_lock = threading.Lock()


def log_activity(action, details, status="success"):
    entry = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "action": action,
        "details": details,
        "status": status,
    }
    activity_log.insert(0, entry)
    if len(activity_log) > MAX_LOG_ENTRIES:
        activity_log.pop()


def shard_exe():
    for candidate in (
        ROOT / "build" / "Debug" / "shard-kv.exe",
        ROOT / "build" / "Release" / "shard-kv.exe",
        ROOT / "build" / "shard-kv",
        ROOT / "build_msvc" / "Debug" / "shard-kv.exe",
    ):
        if candidate.is_file():
            return candidate
    return ROOT / "build" / "Debug" / "shard-kv.exe"


def node_env(name, info):
    port = info["address"].split(":")[-1]
    nid = info["id"]
    peers = []
    for other, oinfo in NODES.items():
        if other == name:
            continue
        oid = oinfo["id"]
        oaddr = oinfo["address"]
        peers.append(f"{oid}:{oaddr}")
    data = RUN_DIR / name / "data"
    wal = RUN_DIR / name / "wal"
    data.mkdir(parents=True, exist_ok=True)
    wal.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "SHARD_NODE_ID": str(nid),
            "SHARD_LISTEN_ADDR": f"127.0.0.1:{port}",
            "SHARD_PEERS": ",".join(peers),
            "SHARD_DATA_DIR": str(data),
            "SHARD_WAL_DIR": str(wal),
            "SHARD_LOG_LEVEL": "INFO",
            "SHARD_ELECTION_TIMEOUT_MIN_MS": "500",
            "SHARD_ELECTION_TIMEOUT_MAX_MS": "1000",
            "SHARD_HEARTBEAT_MS": "50",
            "SHARD_RPC_TIMEOUT_MS": "400",
            "SHARD_COMMIT_TIMEOUT_MS": "5000",
            "SHARD_WAL_SYNC": "none",
        }
    )
    return env


def pid_on_port(port: int):
    try:
        out = subprocess.check_output(
            ["netstat", "-ano", "-p", "tcp"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except Exception:
        return None
    needle = f":{port}"
    for line in out.splitlines():
        if "LISTENING" not in line.upper() and "LISTEN" not in line.upper():
            continue
        if needle not in line:
            continue
        parts = line.split()
        if not parts:
            continue
        try:
            return int(parts[-1])
        except ValueError:
            continue
    return None


def stop_pid(pid: int):
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/F"],
            capture_output=True,
            timeout=10,
        )
    else:
        os.kill(pid, 15)


def start_local_node(name: str):
    info = NODES[name]
    exe = shard_exe()
    if not exe.is_file():
        raise FileNotFoundError(f"shard-kv not found at {exe}")
    log_path = RUN_DIR / name / "node.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logf = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [str(exe)],
        cwd=str(ROOT),
        env=node_env(name, info),
        stdout=logf,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    (RUN_DIR / name / "pid").write_text(str(proc.pid))
    return proc.pid


def check_node_status(node_name, node_info):
    try:
        start = time.time()
        ping = tcp_kv.ping(node_info["address"], timeout=1.0)
        latency = (time.time() - start) * 1000
        got = tcp_kv.get(node_info["address"], "__status_check__", timeout=1.5)
        if got.status == tcp_kv.OK:
            return {
                "status": "healthy",
                "role": "leader",
                "latency_ms": round(latency, 1),
                "term": ping.term,
                "leader_id": got.leader_id or ping.leader_id,
            }
        if got.status == tcp_kv.NOT_LEADER:
            return {
                "status": "healthy",
                "role": "follower",
                "latency_ms": round(latency, 1),
                "term": ping.term,
                "leader_id": got.leader_id or ping.leader_id,
            }
        if got.status == tcp_kv.TIMEOUT:
            return {
                "status": "degraded",
                "role": "candidate",
                "error": "No quorum",
                "term": ping.term,
            }
        return {
            "status": "healthy",
            "role": "unknown",
            "latency_ms": round(latency, 1),
            "error": tcp_kv.STATUS_NAME.get(got.status, str(got.status)),
        }
    except Exception as e:
        return {"status": "offline", "role": "unknown", "error": str(e)[:80]}


def find_leader_address():
    for node_name, node_info in NODES.items():
        status = check_node_status(node_name, node_info)
        if status.get("role") == "leader":
            return node_info["address"]
    return None


def require_leader():
    address = find_leader_address()
    if address is None:
        return None, "No leader available (start the cluster, or wait for election)"
    return address, None


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/cluster/status")
def cluster_status():
    status = {}
    for node_name, node_info in NODES.items():
        node_status = check_node_status(node_name, node_info)
        status[node_name] = {
            "id": node_info["id"],
            "address": node_info["address"],
            **node_status,
        }
    return jsonify(status)


@app.route("/api/activity")
def get_activity():
    return jsonify(activity_log[:50])


@app.route("/api/kv/put", methods=["POST"])
def put_key():
    data = request.json or {}
    key = data.get("key", "")
    value = data.get("value", "")
    if not key:
        return jsonify({"success": False, "error": "Key is required"}), 400
    address, err = require_leader()
    if address is None:
        return jsonify({"success": False, "error": err}), 503
    try:
        start = time.time()
        resp = tcp_kv.put(address, key, value, timeout=5.0)
        latency = (time.time() - start) * 1000
        ok = resp.status == tcp_kv.OK
        if not ok:
            log_activity("PUT", f"{key} ({tcp_kv.STATUS_NAME.get(resp.status)})", "error")
            return jsonify(
                {
                    "success": False,
                    "error": tcp_kv.STATUS_NAME.get(resp.status, str(resp.status)),
                    "leader_id": resp.leader_id,
                    "latency_ms": round(latency, 2),
                }
            ), 500
        log_activity("PUT", f"{key} = {value[:50]}", "success")
        return jsonify({"success": True, "latency_ms": round(latency, 2)})
    except Exception as e:
        log_activity("PUT", f"{key} (FAILED: {e})", "error")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kv/get", methods=["POST"])
def get_key():
    data = request.json or {}
    key = data.get("key", "")
    if not key:
        return jsonify({"found": False, "error": "Key is required"}), 400
    address, err = require_leader()
    if address is None:
        return jsonify({"found": False, "error": err}), 503
    try:
        start = time.time()
        resp = tcp_kv.get(address, key, timeout=5.0)
        latency = (time.time() - start) * 1000
        if resp.status != tcp_kv.OK:
            log_activity("GET", f"{key} ({tcp_kv.STATUS_NAME.get(resp.status)})", "error")
            return jsonify(
                {
                    "found": False,
                    "error": tcp_kv.STATUS_NAME.get(resp.status, str(resp.status)),
                    "latency_ms": round(latency, 2),
                }
            ), 500
        log_activity(
            "GET",
            f"{key} → {resp.value[:50] if resp.found else '(not found)'}",
            "success" if resp.found else "warning",
        )
        return jsonify(
            {
                "found": resp.found,
                "value": resp.value if resp.found else None,
                "latency_ms": round(latency, 2),
            }
        )
    except Exception as e:
        log_activity("GET", f"{key} (FAILED: {e})", "error")
        return jsonify({"found": False, "error": str(e)}), 500


@app.route("/api/kv/delete", methods=["POST"])
def delete_key():
    data = request.json or {}
    key = data.get("key", "")
    if not key:
        return jsonify({"success": False, "error": "Key is required"}), 400
    address, err = require_leader()
    if address is None:
        return jsonify({"success": False, "error": err}), 503
    try:
        start = time.time()
        resp = tcp_kv.delete(address, key, timeout=5.0)
        latency = (time.time() - start) * 1000
        ok = resp.status == tcp_kv.OK
        log_activity("DELETE", key if ok else f"{key} ({tcp_kv.STATUS_NAME.get(resp.status)})",
                     "success" if ok else "error")
        return jsonify({"success": ok, "latency_ms": round(latency, 2)})
    except Exception as e:
        log_activity("DELETE", f"{key} (FAILED: {e})", "error")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/test/basic-crud", methods=["POST"])
def test_basic_crud():
    results = []
    address, err = require_leader()
    if address is None:
        return jsonify({"success": False, "error": err}), 503
    try:
        key = f"test:crud:{int(time.time())}"
        value = "Hello, ShardKV!"
        start = time.time()
        tcp_kv.put(address, key, value, timeout=5.0)
        results.append({"op": "PUT", "key": key, "latency_ms": round((time.time() - start) * 1000, 2)})
        start = time.time()
        resp = tcp_kv.get(address, key, timeout=5.0)
        results.append(
            {
                "op": "GET",
                "key": key,
                "value": resp.value,
                "latency_ms": round((time.time() - start) * 1000, 2),
            }
        )
        start = time.time()
        tcp_kv.delete(address, key, timeout=5.0)
        results.append({"op": "DELETE", "key": key, "latency_ms": round((time.time() - start) * 1000, 2)})
        resp = tcp_kv.get(address, key, timeout=5.0)
        results.append({"op": "VERIFY_DELETED", "found": resp.found})
        log_activity("TEST", "Basic CRUD test completed", "success")
        return jsonify({"success": True, "results": results})
    except Exception as e:
        log_activity("TEST", f"Basic CRUD test failed: {e}", "error")
        return jsonify({"success": False, "error": str(e), "results": results}), 500


@app.route("/api/test/batch-write", methods=["POST"])
def test_batch_write():
    data = request.json or {}
    count = min(int(data.get("count", 100)), 1000)
    results = {"total": count, "successful": 0, "failed": 0, "latencies": []}
    address, err = require_leader()
    if address is None:
        return jsonify({"success": False, "error": err}), 503
    try:
        start_time = time.time()
        for i in range(count):
            key = f"batch:{int(time.time())}:{i}"
            value = f"value_{i}_" + ("x" * 100)
            try:
                op_start = time.time()
                resp = tcp_kv.put(address, key, value, timeout=5.0)
                if resp.status == tcp_kv.OK:
                    results["latencies"].append(round((time.time() - op_start) * 1000, 2))
                    results["successful"] += 1
                else:
                    results["failed"] += 1
            except Exception:
                results["failed"] += 1
        total_time = time.time() - start_time
        results["total_time_ms"] = round(total_time * 1000, 2)
        results["ops_per_second"] = round(results["successful"] / total_time, 2) if total_time else 0
        results["avg_latency_ms"] = (
            round(sum(results["latencies"]) / len(results["latencies"]), 2)
            if results["latencies"]
            else 0
        )
        log_activity(
            "TEST",
            f'Batch write: {results["successful"]}/{count} @ {results["ops_per_second"]} ops/s',
            "success",
        )
        return jsonify({"success": True, **results})
    except Exception as e:
        log_activity("TEST", f"Batch write failed: {e}", "error")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/test/consistency", methods=["POST"])
def test_consistency():
    results = []
    address, err = require_leader()
    if address is None:
        return jsonify({"success": False, "error": err}), 503
    try:
        key = f"consistency:{int(time.time())}"
        value = "initial_value"
        tcp_kv.put(address, key, value, timeout=5.0)
        results.append({"step": 1, "action": "PUT", "key": key, "value": value})
        for i in range(5):
            resp = tcp_kv.get(address, key, timeout=5.0)
            results.append(
                {
                    "step": i + 2,
                    "action": "GET",
                    "key": key,
                    "value": resp.value,
                    "consistent": resp.found and resp.value == value,
                }
            )
        new_value = "updated_value"
        tcp_kv.put(address, key, new_value, timeout=5.0)
        results.append({"step": 7, "action": "UPDATE", "key": key, "value": new_value})
        resp = tcp_kv.get(address, key, timeout=5.0)
        results.append(
            {
                "step": 8,
                "action": "GET",
                "key": key,
                "value": resp.value,
                "consistent": resp.found and resp.value == new_value,
            }
        )
        log_activity("TEST", "Consistency test completed", "success")
        return jsonify({"success": True, "results": results})
    except Exception as e:
        log_activity("TEST", f"Consistency test failed: {e}", "error")
        return jsonify({"success": False, "error": str(e), "results": results}), 500


@app.route("/api/clear-activity", methods=["POST"])
def clear_activity():
    global activity_log
    activity_log = []
    return jsonify({"success": True})


@app.route("/api/cluster/start", methods=["POST"])
def start_cluster():
    started = []
    errors = []
    with _start_lock:
        for name, info in NODES.items():
            port = int(info["address"].split(":")[-1])
            if pid_on_port(port):
                started.append({"node": name, "status": "already_running"})
                continue
            try:
                pid = start_local_node(name)
                started.append({"node": name, "pid": pid, "status": "started"})
            except Exception as e:
                errors.append({"node": name, "error": str(e)})
    log_activity("CLUSTER", f"Start local cluster: {started}", "success" if not errors else "warning")
    return jsonify({"success": not errors, "started": started, "errors": errors})


@app.route("/api/node/stop/<node_name>", methods=["POST"])
def stop_node(node_name):
    if node_name not in NODES:
        return jsonify({"success": False, "error": "unknown node"}), 404
    port = int(NODES[node_name]["address"].split(":")[-1])
    pid = pid_on_port(port)
    if pid is None:
        return jsonify({"success": False, "error": f"{node_name} is not listening"}), 404
    try:
        stop_pid(pid)
        log_activity("RAFT", f"Stopped {node_name} (pid {pid})", "warning")
        return jsonify({"success": True, "message": f"{node_name} stopped"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/node/start/<node_name>", methods=["POST"])
def start_node(node_name):
    if node_name not in NODES:
        return jsonify({"success": False, "error": "unknown node"}), 404
    port = int(NODES[node_name]["address"].split(":")[-1])
    if pid_on_port(port):
        return jsonify({"success": True, "message": f"{node_name} already running"})
    try:
        pid = start_local_node(node_name)
        log_activity("RAFT", f"Started {node_name} (pid {pid})", "success")
        return jsonify({"success": True, "message": f"{node_name} started", "pid": pid})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/raft/logs")
def get_raft_logs():
    raft_events = []
    for node_name in NODES:
        log_file = RUN_DIR / node_name / "node.log"
        if not log_file.is_file():
            continue
        try:
            text = log_file.read_text(errors="replace")[-8000:]
        except OSError:
            continue
        for line in text.splitlines():
            if not any(
                kw in line
                for kw in ("LEADER", "election", "vote", "Granted", "term", "candidate")
            ):
                continue
            parts = line.split("]")
            timestamp = parts[0].replace("[", "").strip() if len(parts) >= 2 else ""
            message = "]".join(parts[1:]).strip() if len(parts) >= 2 else line
            event_type = "election"
            if "LEADER" in line:
                event_type = "leader"
            elif "vote" in line.lower() or "Granted" in line:
                event_type = "vote"
            elif "candidate" in line.lower():
                event_type = "candidate"
            raft_events.append(
                {
                    "node": node_name,
                    "timestamp": timestamp,
                    "message": message,
                    "type": event_type,
                }
            )
    raft_events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return jsonify(raft_events[:50])


@app.route("/api/test/leader-failover", methods=["POST"])
def test_leader_failover():
    results = []
    current_leader = None
    for node_name, node_info in NODES.items():
        status = check_node_status(node_name, node_info)
        if status.get("role") == "leader":
            current_leader = node_name
            break
    if not current_leader:
        return jsonify({"success": False, "error": "No leader found"}), 500

    results.append(
        {"step": 1, "action": "IDENTIFY_LEADER", "message": f"Current leader is {current_leader}"}
    )
    log_activity("RAFT", f"Leader failover test started - current leader: {current_leader}", "info")

    port = int(NODES[current_leader]["address"].split(":")[-1])
    pid = pid_on_port(port)
    if pid is None:
        return jsonify({"success": False, "error": f"No process on {current_leader}"}), 500
    try:
        stop_pid(pid)
        results.append(
            {
                "step": 2,
                "action": "STOP_LEADER",
                "message": f"Stopped {current_leader} - election will trigger",
            }
        )
        log_activity("RAFT", f"Stopped leader {current_leader}", "warning")
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to stop leader: {e}"}), 500

    time.sleep(5)
    results.append({"step": 3, "action": "WAIT_ELECTION", "message": "Waiting for election (5s)..."})

    new_leader = None
    for node_name, node_info in NODES.items():
        if node_name == current_leader:
            continue
        status = check_node_status(node_name, node_info)
        if status.get("role") == "leader":
            new_leader = node_name
            break

    if new_leader:
        results.append({"step": 4, "action": "NEW_LEADER", "message": f"New leader elected: {new_leader}"})
        log_activity("RAFT", f"New leader elected: {new_leader}", "success")
    else:
        results.append({"step": 4, "action": "ELECTION_PENDING", "message": "Election in progress or no leader yet"})

    try:
        pid = start_local_node(current_leader)
        results.append(
            {
                "step": 6,
                "action": "RESTART_OLD_LEADER",
                "message": f"Restarted {current_leader} (pid {pid}) as follower",
            }
        )
        log_activity("RAFT", f"Restarted {current_leader} as follower", "success")
    except Exception as e:
        results.append({"step": 6, "action": "RESTART_FAILED", "message": f"Failed to restart: {e}"})

    return jsonify(
        {
            "success": True,
            "old_leader": current_leader,
            "new_leader": new_leader,
            "results": results,
        }
    )


if __name__ == "__main__":
    print("=" * 50)
    print("  ShardKV Dashboard (TCP)")
    print("=" * 50)
    port = int(os.environ.get("PORT", 8006))
    print(f"  Dashboard: http://localhost:{port}")
    print(f"  Binary:    {shard_exe()}")
    print("  Cluster:   POST /api/cluster/start")
    print("=" * 50)
    app.run(host="0.0.0.0", port=port, debug=False)
