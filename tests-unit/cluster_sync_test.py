import asyncio
import json
import os
import pytest

import folder_paths
from comfy.cli_args import args
from comfy_execution.jobs import get_all_jobs, get_job
from app.cluster_sync import (
    ClusterSyncManager,
    DistributedPromptQueue,
    atomic_write_bytes,
    compute_bytes_hash,
    merge_settings_lww,
    write_userdata_with_conflict_detection,
)


class DummyServer:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.new_event_loop()
        self.sockets = {}
        self.sockets_metadata = {}
        self.on_prompt_handlers = []
        self.sent_events = []
        self.queue_updated_count = 0
        self.client_session = None
        self.prompt_queue = None

    def add_on_prompt_handler(self, handler):
        self.on_prompt_handlers.append(handler)

    def queue_updated(self):
        self.queue_updated_count += 1

    async def send(self, event, data, sid=None):
        self.sent_events.append((event, data, sid))

    def encode_bytes(self, event, data):
        import struct
        return struct.pack(">I", event) + bytes(data)


def _make_prompt_item(number: float, prompt_id: str, origin: str = "node_a"):
    prompt = {"1": {"class_type": "KSampler", "inputs": {}}}
    extra_data = {
        "create_time": 1700000000000,
        "origin_node": origin,
        "extra_pnginfo": {"workflow": {"id": "wf-123"}},
    }
    outputs_to_execute = ["1"]
    sensitive = {"api_key_comfy_org": "secret-token"}
    return (number, prompt_id, prompt, extra_data, outputs_to_execute, sensitive)


def test_lamport_clock_and_parity_tie_breaking():
    server_a = DummyServer()
    sync_a = ClusterSyncManager(server_a, node_id="node_a", peer_url="http://peer:8188", node_parity=1)
    q_a = DistributedPromptQueue(server_a, sync_a)
    q_a.lamport_clock = 100

    server_b = DummyServer()
    sync_b = ClusterSyncManager(server_b, node_id="node_b", peer_url="http://peer:8188", node_parity=0)
    q_b = DistributedPromptQueue(server_b, sync_b)
    q_b.lamport_clock = 100

    num_a = q_a.next_lamport_number(front=False)
    num_b = q_b.next_lamport_number(front=False)

    # Both incremented from 100 -> 101, but parity tie-breaker keeps them distinct and ordered
    assert num_a == pytest.approx(101.1)
    assert num_b == pytest.approx(101.2)
    assert num_a < num_b

    # Front-of-queue prompt negates priority
    front_a = q_a.next_lamport_number(front=True)
    assert front_a == pytest.approx(-102.1)


@pytest.mark.asyncio
async def test_bidirectional_queue_and_history_sync():
    loop = asyncio.get_running_loop()
    server_a = DummyServer(loop)
    sync_a = ClusterSyncManager(server_a, node_id="node_a", peer_url="http://node-b:8188", node_parity=1)
    q_a = DistributedPromptQueue(server_a, sync_a)
    server_a.prompt_queue = q_a

    server_b = DummyServer(loop)
    sync_b = ClusterSyncManager(server_b, node_id="node_b", peer_url="http://node-a:8188", node_parity=0)
    q_b = DistributedPromptQueue(server_b, sync_b)
    server_b.prompt_queue = q_b

    # 1. Enqueue a prompt on Node A
    item_a = _make_prompt_item(10.1, "prompt-1", origin="node_a")
    q_a.put(item_a)

    # Drain outbound event from Node A -> deliver to Node B
    msg_type, envelope = sync_a.outbound_queue.get_nowait()
    assert msg_type == "json"
    assert envelope["action"] == "queue_put"
    # Sensitive keys must be stripped before cross-node transit
    assert envelope["payload"]["item"][5] == {}

    await sync_b.process_peer_json(envelope)

    # Node B should see the job in remote_queued (not popped by Node B's local worker when work_stealing=False)
    assert "prompt-1" in q_b.remote_queued
    assert q_b.get_tasks_remaining() == 1
    running_b, queued_b = q_b.get_current_queue_volatile()
    assert len(running_b) == 0
    assert len(queued_b) == 1
    assert queued_b[0][1] == "prompt-1"

    # 2. Node A's worker pops the job for execution
    popped = q_a.get(timeout=0.01)
    assert popped is not None
    _, item_id = popped

    msg_type, envelope = sync_a.outbound_queue.get_nowait()
    assert envelope["action"] == "queue_running"
    await sync_b.process_peer_json(envelope)

    # Node B now shows prompt-1 in running state
    running_b, queued_b = q_b.get_current_queue_volatile()
    assert len(queued_b) == 0
    assert len(running_b) == 1
    assert running_b[0][1] == "prompt-1"

    # 3. Node A completes the job
    q_a.task_done(
        item_id,
        {"outputs": {"1": {"images": [{"filename": "ComfyUI_00001_.png", "type": "output", "subfolder": ""}]}}, "meta": {}},
        status=DistributedPromptQueue.ExecutionStatus(
            status_str="success",
            completed=True,
            messages=[
                ("execution_start", {"prompt_id": "prompt-1", "timestamp": 1700000001000}),
                ("execution_success", {"prompt_id": "prompt-1", "timestamp": 1700000002000}),
            ],
        ),
        process_item=lambda p: p[:5] + p[6:],
    )

    msg_type, envelope = sync_a.outbound_queue.get_nowait()
    assert envelope["action"] == "task_done"
    await sync_b.process_peer_json(envelope)

    # Verify Node B's /history and /api/jobs normalization match Node A
    assert q_b.get_tasks_remaining() == 0
    hist_b = q_b.get_history(prompt_id="prompt-1")
    assert "prompt-1" in hist_b
    assert isinstance(hist_b["prompt-1"]["prompt"], tuple)

    running_b, queued_b = q_b.get_current_queue_volatile()
    jobs_b, total_b = get_all_jobs(running_b, queued_b, q_b.get_history())
    assert total_b == 1
    assert jobs_b[0]["id"] == "prompt-1"
    assert jobs_b[0]["status"] == "completed"
    job_detail = get_job("prompt-1", running_b, queued_b, hist_b)
    assert job_detail is not None
    assert job_detail["outputs_count"] == 1


@pytest.mark.asyncio
async def test_split_brain_hello_sync_reconciliation():
    loop = asyncio.get_running_loop()
    server_a = DummyServer(loop)
    sync_a = ClusterSyncManager(server_a, node_id="node_a", peer_url="http://node-b:8188", node_parity=1)
    q_a = DistributedPromptQueue(server_a, sync_a)
    server_a.prompt_queue = q_a

    server_b = DummyServer(loop)
    sync_b = ClusterSyncManager(server_b, node_id="node_b", peer_url="http://node-a:8188", node_parity=0)
    q_b = DistributedPromptQueue(server_b, sync_b)
    server_b.prompt_queue = q_b

    # Simulate Node A completing a job while disconnected from Node B
    item_a = _make_prompt_item(55.1, "offline-prompt", origin="node_a")
    q_a.put(item_a)
    _, item_id = q_a.get(timeout=0.01)
    q_a.task_done(
        item_id,
        {"outputs": {}, "meta": {}},
        status=DistributedPromptQueue.ExecutionStatus(status_str="success", completed=True, messages=[]),
        process_item=lambda p: p[:5],
    )

    # Upon reconnect, Node A sends hello_sync snapshot to Node B
    hello = sync_a.build_hello_snapshot()
    await sync_b.process_peer_json(hello)

    assert "offline-prompt" in q_b.get_history()
    assert q_b.lamport_clock >= 55


def test_atomic_write_and_workflow_conflict_detection(tmp_path):
    wf_path = str(tmp_path / "workflows" / "test_flow.json")
    v1_bytes = b'{"version": 1}'
    v2_node_a = b'{"version": 2, "author": "node_a"}'
    v2_node_b = b'{"version": 2, "author": "node_b"}'

    # Initial write
    conflict, cpath = write_userdata_with_conflict_detection(wf_path, v1_bytes)
    assert not conflict
    assert cpath is None
    v1_hash = compute_bytes_hash(v1_bytes)

    # Node A updates cleanly from v1
    conflict, cpath = write_userdata_with_conflict_detection(
        wf_path, v2_node_a, base_hash=v1_hash, origin_node="node_a"
    )
    assert not conflict

    # Node B concurrently submits an edit based on stale v1_hash -> conflict fork must be created
    conflict, cpath = write_userdata_with_conflict_detection(
        wf_path, v2_node_b, base_hash=v1_hash, origin_node="node_b"
    )
    assert conflict is True
    assert cpath is not None
    assert os.path.isfile(cpath)

    # Conflict backup preserves Node A's version while main file has Node B's latest write
    with open(cpath, "rb") as f:
        assert f.read() == v2_node_a
    with open(wf_path, "rb") as f:
        assert f.read() == v2_node_b


def test_settings_lww_key_level_merge():
    local = {
        "Comfy.ColorPalette": "dark",
        "Comfy.DevMode": True,
        "_sync_key_ts": {"Comfy.ColorPalette": 1000, "Comfy.DevMode": 2000},
    }
    incoming = {
        "Comfy.ColorPalette": "light",
        "Comfy.DevMode": False,
        "Comfy.SnapToGrid": True,
        "_sync_key_ts": {"Comfy.ColorPalette": 1500, "Comfy.DevMode": 1200, "Comfy.SnapToGrid": 1600},
    }
    merged = merge_settings_lww(local, incoming)
    # ColorPalette (1500 > 1000) and SnapToGrid (1600 > 0) win; DevMode (2000 > 1200) keeps local value
    assert merged["Comfy.ColorPalette"] == "light"
    assert merged["Comfy.DevMode"] is True
    assert merged["Comfy.SnapToGrid"] is True


def test_parity_save_image_counter_isolation(tmp_path, monkeypatch):
    out_dir = str(tmp_path / "output")
    os.makedirs(out_dir, exist_ok=True)

    # Node A (odd parity = 1)
    monkeypatch.setattr(args, "sync_node_parity", 1)
    _, filename_a, counter_a, _, _ = folder_paths.get_save_image_path("ComfyUI", out_dir)
    assert counter_a == 1

    # Node B (even parity = 0) on the same empty directory picks 2 instead of colliding on 1
    monkeypatch.setattr(args, "sync_node_parity", 0)
    _, filename_b, counter_b, _, _ = folder_paths.get_save_image_path("ComfyUI", out_dir)
    assert counter_b == 2

    # Simulate ComfyUI_00002_.png written to disk; Node A should pick 3 next
    atomic_write_bytes(os.path.join(out_dir, f"{filename_b}_00002_.png"), b"png")
    monkeypatch.setattr(args, "sync_node_parity", 1)
    _, _, counter_a2, _, _ = folder_paths.get_save_image_path("ComfyUI", out_dir)
    assert counter_a2 == 3


@pytest.mark.asyncio
async def test_websocket_broadcast_fanout_to_all_clients_and_peer():
    loop = asyncio.get_running_loop()
    server_a = DummyServer(loop)
    sync_a = ClusterSyncManager(server_a, node_id="node_a", peer_url="http://node-b:8188", node_parity=1)
    server_a.prompt_queue = DistributedPromptQueue(server_a, sync_a)
    sync_a.install_hooks()

    # PromptExecutor normally sends "executing" with sid="client-1" only
    await server_a.send("executing", {"node": "3", "prompt_id": "p-1"}, sid="client-1")

    # 1. Local send must have been rewritten to sid=None so all local tabs receive node highlights
    assert len(server_a.sent_events) == 1
    event, data, target_sid = server_a.sent_events[0]
    assert event == "executing"
    assert target_sid is None

    # 2. Peer outbound queue must also receive the ui_broadcast envelope
    msg_type, envelope = sync_a.outbound_queue.get_nowait()
    assert msg_type == "json"
    assert envelope["action"] == "ui_broadcast"
    assert envelope["payload"]["event"] == "executing"
    assert envelope["payload"]["data"]["node"] == "3"


@pytest.mark.asyncio
async def test_live_workspace_graph_and_browser_state_sync():
    loop = asyncio.get_running_loop()
    server_a = DummyServer(loop)
    sync_a = ClusterSyncManager(server_a, node_id="node_a", peer_url="http://node-b:8188", node_parity=1)
    server_a.prompt_queue = DistributedPromptQueue(server_a, sync_a)
    sync_a.install_hooks()

    server_b = DummyServer(loop)
    sync_b = ClusterSyncManager(server_b, node_id="node_b", peer_url="http://node-a:8188", node_parity=0)
    server_b.prompt_queue = DistributedPromptQueue(server_b, sync_b)
    sync_b.install_hooks()

    class MockWS:
        def __init__(self):
            self.closed = False
            self.messages = []

        async def send_json(self, payload):
            self.messages.append(payload)

    ws_browser_1 = MockWS()
    ws_browser_2 = MockWS()
    server_a.sockets["b1"] = ws_browser_1
    server_a.sockets["b2"] = ws_browser_2

    # Browser 1 edits the canvas graph and sends sync_workspace over /ws
    sample_wf = {"nodes": [{"id": 1, "type": "KSampler", "pos": [100, 200], "widgets_values": [42]}], "links": []}
    await sync_a.handle_client_ws_message(
        "b1",
        {"type": "sync_workspace", "data": {"workflow": sample_wf, "ts": 1700000050000}},
    )
    await asyncio.sleep(0.01)

    # Browser 2 on the same server receives sync_workspace (Browser 1 does not get echoed)
    assert len(ws_browser_1.messages) == 0
    assert len(ws_browser_2.messages) == 1
    assert ws_browser_2.messages[0]["type"] == "sync_workspace"
    assert ws_browser_2.messages[0]["data"]["workflow"] == sample_wf

    # Peer Node B also receives workspace_sync and updates its authoritative workspace_state
    msg_type, envelope = sync_a.outbound_queue.get_nowait()
    assert msg_type == "json"
    assert envelope["action"] == "workspace_sync"
    await sync_b.process_peer_json(envelope)
    assert sync_b.workspace_state["workflow"] == sample_wf

    # Newly connecting Browser 3 on Node B immediately receives sync_full_state with the synced workflow
    ws_browser_3 = MockWS()
    server_b.sockets["b3"] = ws_browser_3
    await sync_b.on_client_connected("b3", ws_browser_3)
    assert len(ws_browser_3.messages) == 1
    assert ws_browser_3.messages[0]["type"] == "sync_full_state"
    assert ws_browser_3.messages[0]["data"]["workflow"] == sample_wf


@pytest.mark.asyncio
async def test_multi_user_independent_tabs_and_live_spectator_switching():
    loop = asyncio.get_running_loop()
    server = DummyServer(loop)
    sync = ClusterSyncManager(server, node_id="node_a")
    server.prompt_queue = DistributedPromptQueue(server, sync)
    sync.install_hooks()

    class MockWS:
        def __init__(self):
            self.closed = False
            self.messages = []

        async def send_json(self, payload):
            self.messages.append(payload)

    ws_alice = MockWS()
    ws_bob = MockWS()
    server.sockets["sid_alice"] = ws_alice
    server.sockets["sid_bob"] = ws_bob

    wf_alice = {"nodes": [{"id": 1, "type": "CLIPTextEncode", "widgets_values": ["Alice cat"]}], "links": []}
    wf_bob = {"nodes": [{"id": 2, "type": "KSampler", "widgets_values": [999]}], "links": []}

    # 1. Alice and Bob identify with their usernames on opening the Tunnel link
    await sync.handle_client_ws_message(
        "sid_alice",
        {"type": "sync_identify", "data": {"username": "Alice", "viewing_user": "Alice", "initial_workflow": wf_alice}},
    )
    await sync.handle_client_ws_message(
        "sid_bob",
        {"type": "sync_identify", "data": {"username": "Bob", "viewing_user": "Bob", "initial_workflow": wf_bob}},
    )
    ws_alice.messages.clear()
    ws_bob.messages.clear()

    # 2. Bob edits his graph in his own tab -> Alice (on Alice's tab) must NOT have her canvas overwritten
    wf_bob_v2 = {"nodes": [{"id": 2, "type": "KSampler", "widgets_values": [12345]}], "links": []}
    await sync.handle_client_ws_message(
        "sid_bob",
        {"type": "sync_workspace", "data": {"workspace_user": "Bob", "workflow": wf_bob_v2, "ts": 1700000060000}},
    )
    await asyncio.sleep(0.01)
    assert not any(m.get("type") == "sync_workspace" for m in ws_alice.messages)

    # 3. Bob starts executing a prompt -> only Bob's tab receives executing event; Alice's canvas is undisturbed
    sync.prompt_user_map["prompt_bob_1"] = "Bob"
    server.sent_events.clear()
    await server.send("executing", {"node": "2", "prompt_id": "prompt_bob_1"}, sid="sid_bob")
    assert server.sent_events == [("executing", {"node": "2", "prompt_id": "prompt_bob_1"}, "sid_bob")]

    # 4. Alice clicks on Bob's tab to watch what Bob is doing in real time
    ws_alice.messages.clear()
    await sync.handle_client_ws_message(
        "sid_alice",
        {"type": "sync_switch_view", "data": {"username": "Alice", "viewing_user": "Bob"}},
    )
    full_state_msgs = [m for m in ws_alice.messages if m.get("type") == "sync_full_state"]
    assert len(full_state_msgs) == 1
    assert full_state_msgs[0]["data"]["workspace_user"] == "Bob"
    assert full_state_msgs[0]["data"]["workflow"] == wf_bob_v2
    assert full_state_msgs[0]["data"]["executing_node"] == "2"

    # 5. While Alice is on Bob's tab, Bob's live graph edits, cursor moves, and execution events stream to Alice in real time
    ws_alice.messages.clear()
    wf_bob_v3 = {"nodes": [{"id": 2, "type": "KSampler", "widgets_values": [77777]}], "links": []}
    await sync.handle_client_ws_message(
        "sid_bob",
        {"type": "sync_workspace", "data": {"workspace_user": "Bob", "workflow": wf_bob_v3, "ts": 1700000070000}},
    )
    await sync.handle_client_ws_message(
        "sid_bob",
        {"type": "sync_cursor", "data": {"username": "Bob", "workspace_user": "Bob", "x": 320, "y": 480}},
    )
    await asyncio.sleep(0.01)
    assert any(m.get("type") == "sync_workspace" and m["data"]["workflow"] == wf_bob_v3 for m in ws_alice.messages)
    assert any(m.get("type") == "sync_cursor" and m["data"]["x"] == 320 for m in ws_alice.messages)

    server.sent_events.clear()
    await server.send("progress", {"value": 10, "max": 20, "prompt_id": "prompt_bob_1"}, sid="sid_bob")
    recipient_sids = {ev[2] for ev in server.sent_events if ev[0] == "progress"}
    assert recipient_sids == {"sid_bob", "sid_alice"}

    # 6. Alice switches back to her own tab ("Alice") -> her own workflow wf_alice is restored untouched
    ws_alice.messages.clear()
    await sync.handle_client_ws_message(
        "sid_alice",
        {"type": "sync_switch_view", "data": {"username": "Alice", "viewing_user": "Alice"}},
    )
    alice_state_msgs = [m for m in ws_alice.messages if m.get("type") == "sync_full_state"]
    assert len(alice_state_msgs) == 1
    assert alice_state_msgs[0]["data"]["workspace_user"] == "Alice"
    assert alice_state_msgs[0]["data"]["workflow"] == wf_alice


