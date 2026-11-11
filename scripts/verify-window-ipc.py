"""Black-box v11 window IPC fixture against a built codex-acp, with no model calls.

The fake owner deliberately omits the start-turn RPC response. Its canonical
user message, followed by revisioned patches, is the acceptance evidence.
Run with: npm run build && python3 scripts/verify-window-ipc.py
"""

import json
import os
import pathlib
import queue
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time

adapter = str(pathlib.Path(__file__).resolve().parents[1] / "dist/index.js")
codex = os.environ.get("CODEX_PATH") or shutil.which("codex")
if codex is None:
    raise SystemExit("Install Codex CLI or set CODEX_PATH before running this probe")
node = subprocess.check_output(["node", "-p", "process.execPath"], text=True).strip()


def router(listener, native, errors):
    try:
        conn, _ = listener.accept()
        with conn, conn.makefile("rb") as reader:
            conn.settimeout(10)

            def read():
                head = reader.read(4)
                return (
                    json.loads(reader.read(struct.unpack("<I", head)[0]))
                    if head
                    else None
                )

            def send(message):
                body = json.dumps(message).encode()
                conn.sendall(struct.pack("<I", len(body)) + body)

            def ok(req, result, **more):
                send(
                    dict(
                        type="response",
                        requestId=req["requestId"],
                        resultType="success",
                        result=result,
                        **more,
                    )
                )

            def patch(base, revision, patches):
                send(
                    {
                        "type": "broadcast",
                        "method": "thread-stream-state-changed",
                        "version": 11,
                        "sourceClientId": "owner",
                        "params": {
                            "conversationId": "thread",
                            "hostId": "local",
                            "change": {
                                "type": "patches",
                                "baseRevision": base,
                                "revision": revision,
                                "patches": patches,
                            },
                        },
                    }
                )

            while (r := read()) is not None:
                m = r["method"]
                if m == "initialize":
                    ok(r, {"clientId": "aa"})
                elif m == "thread-owner-discovery":
                    ok(r, {}, handledByClientId="owner")
                elif (
                    m == "thread-stream-following-changed" and r["params"]["following"]
                ):
                    send(
                        {
                            "type": "broadcast",
                            "method": "thread-stream-state-changed",
                            "version": 11,
                            "sourceClientId": "owner",
                            "params": {
                                "conversationId": "thread",
                                "hostId": "local",
                                "change": {
                                    "type": "snapshot",
                                    "revision": 1,
                                    "conversationState": {"cwd": "/tmp", "turns": []},
                                },
                            },
                        }
                    )
                elif m == "thread-follower-start-turn":
                    assert r["targetClientId"] == "owner"
                    message = r["params"]["turnStart"]["request"]
                    native.append(message)
                    item = {
                        "id": "u",
                        "type": "userMessage",
                        "content": message["input"],
                        "clientId": message["clientUserMessageId"],
                    }
                    approval = {
                        "id": "approval",
                        "method": "item/fileChange/requestApproval",
                        "params": {"itemId": "file", "reason": "Write fixture"},
                    }
                    patch(
                        1,
                        2,
                        [
                            {
                                "op": "add",
                                "path": ["turns", "0"],
                                "value": {
                                    "id": "turn",
                                    "status": "inProgress",
                                    "items": [item],
                                },
                            },
                            {"op": "add", "path": ["requests"], "value": [approval]},
                        ],
                    )
                elif m == "thread-follower-file-approval-decision":
                    assert r["params"]["decision"] == "accept"
                    native.append(m)
                    patch(2, 3, [{"op": "replace", "path": ["requests"], "value": []}])
                    ok(r, {"ok": True})
                elif m == "thread-follower-steer-turn":
                    native.append(m)
                    assert r["params"]["input"][0]["text"] == "add context"
                    assert r["params"]["restoreMessage"]["context"]["workspaceRoots"]
                    item = {
                        "id": "draft",
                        "type": "steeringUserMessage",
                        "status": "accepted",
                        "input": r["params"]["input"],
                        "clientUserMessageId": r["params"]["clientUserMessageId"],
                        "serverUserMessageId": "steer",
                    }
                    patch(
                        3,
                        4,
                        [
                            {
                                "op": "add",
                                "path": ["turns", "0", "items", "1"],
                                "value": item,
                            },
                            {
                                "op": "add",
                                "path": ["turns", "0", "items", "2"],
                                "value": {"id": "steer", "type": "steered"},
                            },
                        ],
                    )
                elif m == "thread-follower-interrupt-turn":
                    assert r["params"]["expectedTurnId"] == "turn"
                    native.append(m)
                    patch(
                        4,
                        5,
                        [
                            {
                                "op": "add",
                                "path": ["turns", "0", "items", "3"],
                                "value": {
                                    "id": "answer",
                                    "type": "agentMessage",
                                    "text": "window output",
                                    "phase": None,
                                },
                            },
                            {
                                "op": "replace",
                                "path": ["turns", "0", "status"],
                                "value": "interrupted",
                            },
                        ],
                    )
                    ok(r, {"ok": True})
    except (OSError, ValueError, KeyError, AssertionError) as error:
        errors.put(error)


def verify(version):
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="t326-window-") as root:
        d = pathlib.Path(root)
        endpoint = d / "win.sock"
        home = d / "codex"
        home.mkdir()
        native = []
        errors = queue.Queue()
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(endpoint))
            endpoint.chmod(0o600)
            listener.listen()
            worker = threading.Thread(
                target=router, args=(listener, native, errors), daemon=True
            )
            worker.start()
            env = {
                "PATH": os.environ["PATH"],
                "HOME": root,
                "CODEX_HOME": str(home),
                "CODEX_PATH": codex,
                "CODEX_ACP_WINDOW_SOCKET": str(endpoint),
            }
            with subprocess.Popen(
                [node, adapter],
                cwd=root,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ) as p:
                lines = queue.Queue()

                def read_updates():
                    for line in p.stdout:
                        lines.put(json.loads(line))

                threading.Thread(target=read_updates, daemon=True).start()
                seen = []

                def send(i, m, params):
                    p.stdin.write(
                        json.dumps(
                            {"jsonrpc": "2.0", "id": i, "method": m, "params": params}
                        )
                        + "\n"
                    )
                    p.stdin.flush()

                def wait(pred):
                    until = time.monotonic() + 7
                    while time.monotonic() < until:
                        r = lines.get(timeout=max(0.01, until - time.monotonic()))
                        seen.append(r)
                        if pred(r):
                            return r
                    raise RuntimeError(seen)

                def response(i):
                    r = wait(lambda r: r.get("id") == i)
                    assert "error" not in r, r
                    return r["result"]

                try:
                    client = {"name": "probe", "version": "1"}
                    send(
                        1,
                        "initialize",
                        dict(
                            protocolVersion=version,
                            _meta={"aa/sessionRead": 1},
                            **(
                                {"clientInfo": client, "clientCapabilities": {}}
                                if version == 1
                                else {"info": client, "capabilities": {}}
                            ),
                        ),
                    )
                    assert response(1)["_meta"]["aa/windowControl"] == 1
                    send(
                        2,
                        "session/resume",
                        {"sessionId": "thread", "cwd": root, "mcpServers": []},
                    )
                    response(2)
                    send(
                        3,
                        "session/prompt",
                        {
                            "sessionId": "thread",
                            "prompt": [{"type": "text", "text": "one input"}],
                            "_meta": {"aa/clientMessageId": "stable-message"},
                        },
                    )
                    approval = wait(
                        lambda x: x.get("method") == "session/request_permission"
                    )
                    p.stdin.write(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": approval["id"],
                                "result": {
                                    "outcome": {
                                        "outcome": "selected",
                                        "optionId": "accept",
                                    }
                                },
                            }
                        )
                        + "\n"
                    )
                    p.stdin.flush()
                    if version == 2:
                        accepted = next(
                            (x for x in seen if x.get("id") == 3 and "result" in x),
                            None,
                        )
                        if accepted is None:
                            assert response(3)["messageId"] == "stable-message"
                        else:
                            assert accepted["result"]["messageId"] == "stable-message"
                    until = time.monotonic() + 2
                    while len(native) < 2 and time.monotonic() < until:
                        time.sleep(0.01)
                    send(
                        4,
                        "_session/steering",
                        {
                            "sessionId": "thread",
                            "prompt": [{"type": "text", "text": "add context"}],
                            "_meta": {"aa/clientMessageId": "stable-steer"},
                        },
                    )
                    assert response(4)["outcome"] == "injected"
                    p.stdin.write(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "session/cancel",
                                "params": {"sessionId": "thread"},
                            }
                        )
                        + "\n"
                    )
                    p.stdin.flush()
                    if version == 1:
                        assert response(3)["stopReason"] == "cancelled"
                    assert any("window output" in json.dumps(x) for x in seen) or wait(
                        lambda x: "window output" in json.dumps(x)
                    )
                    assert native[0][
                        "clientUserMessageId"
                    ] == "stable-message" and native[1:] == [
                        "thread-follower-file-approval-decision",
                        "thread-follower-steer-turn",
                        "thread-follower-interrupt-turn",
                    ], native
                    send(5, "session/close", {"sessionId": "thread"})
                    response(5)
                    assert errors.empty(), list(errors.queue)
                    print(
                        f"ACP v{version}: one start, file approval, steer, interrupt and close reached the expected IPC owner."
                    )
                finally:
                    p.stdin.close()
                    try:
                        p.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        p.terminate()
                        p.wait(timeout=5)
                        raise


if __name__ == "__main__":
    for version in (1, 2):
        verify(version)
