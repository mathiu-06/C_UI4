from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import heapq
import json
import logging
import os
import re
import struct
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
from io import BytesIO
from typing import Any, Callable, List, Literal, NamedTuple, Optional

import folder_paths
from comfy.cli_args import args
from comfy_api import feature_flags
from protocol import BinaryEventTypes

try:
    import aiohttp
    from aiohttp import web
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    web = None  # type: ignore


MAX_INBAND_ASSET_BYTES = 32 * 1024 * 1024  # 32 MB max for in-band WebSocket file push


def compute_bytes_hash(data: bytes) -> str:
    """Return SHA-256 hex digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def compute_file_hash(path: str) -> Optional[str]:
    """Return SHA-256 hex digest of a file on disk, or None if missing."""
    if not os.path.isfile(path):
        return None
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def atomic_write_bytes(dest_path: str, data: bytes) -> None:
    """Atomically write bytes to dest_path using a temporary file + fsync + os.replace."""
    parent = os.path.dirname(os.path.abspath(dest_path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=parent, prefix=".tmp.sync.")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, dest_path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def write_userdata_with_conflict_detection(
    dest_path: str,
    data: bytes,
    base_hash: Optional[str] = None,
    origin_node: str = "local",
) -> tuple[bool, Optional[str]]:
    """
    Write a userdata file atomically while detecting concurrent edit conflicts.
    If base_hash is provided and the existing file's hash differs from both base_hash
    and the incoming data's hash, the existing file is preserved as a conflict copy
    before writing the incoming version.

    Returns:
        tuple: (conflict_detected: bool, conflict_filepath: Optional[str])
    """
    conflict_path: Optional[str] = None
    if base_hash and os.path.isfile(dest_path):
        current_hash = compute_file_hash(dest_path)
        new_hash = compute_bytes_hash(data)
        if current_hash and current_hash != base_hash and current_hash != new_hash:
            stem, ext = os.path.splitext(dest_path)
            ts_str = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
            conflict_path = f"{stem}.conflict-{origin_node}-{ts_str}{ext}"
            try:
                with open(dest_path, "rb") as existing_f:
                    atomic_write_bytes(conflict_path, existing_f.read())
            except OSError as e:
                logging.warning(f"[ClusterSync] Failed to write conflict backup {conflict_path}: {e}")
                conflict_path = None

    atomic_write_bytes(dest_path, data)
    return (conflict_path is not None, conflict_path)


def merge_settings_lww(
    local_settings: dict[str, Any],
    incoming_settings: dict[str, Any],
    incoming_ts_ms: Optional[int] = None,
) -> dict[str, Any]:
    """
    Key-level Last-Write-Wins (LWW) CRDT merge for comfy.settings.json.
    Preserves non-overlapping keys edited concurrently on Device A and Device B.
    """
    now_ms = incoming_ts_ms if incoming_ts_ms is not None else int(time.time() * 1000)
    merged = copy.deepcopy(local_settings)
    local_ts_map: dict[str, int] = dict(merged.get("_sync_key_ts", {}))
    incoming_ts_map: dict[str, int] = dict(incoming_settings.get("_sync_key_ts", {}))

    for key, val in incoming_settings.items():
        if key == "_sync_key_ts":
            continue
        key_incoming_ts = incoming_ts_map.get(key, now_ms)
        key_local_ts = local_ts_map.get(key, 0)
        if key not in merged or merged[key] != val:
            if key_incoming_ts >= key_local_ts:
                merged[key] = val
                local_ts_map[key] = key_incoming_ts

    merged["_sync_key_ts"] = local_ts_map
    return merged


MAXIMUM_HISTORY_SIZE = 10000


class _FallbackPromptQueue:
    """
    Pure-Python fallback matching execution.PromptQueue (execution.py:1143)
    for lightweight environments where torch/execution is not loaded.
    """

    def __init__(self, server: Any):
        self.server = server
        self.mutex = threading.RLock()
        self.not_empty = threading.Condition(self.mutex)
        self.task_counter = 0
        self.queue: list = []
        self.currently_running: dict = {}
        self.history: dict = {}
        self.flags: dict = {}

    class ExecutionStatus(NamedTuple):
        status_str: Literal["success", "error"]
        completed: bool
        messages: List[Any]

    def put(self, item: tuple) -> None:
        with self.mutex:
            heapq.heappush(self.queue, item)
            self.server.queue_updated()
            self.not_empty.notify()

    def get(self, timeout: Optional[float] = None) -> Optional[tuple[tuple, int]]:
        with self.not_empty:
            while len(self.queue) == 0:
                self.not_empty.wait(timeout=timeout)
                if timeout is not None and len(self.queue) == 0:
                    return None
            item = heapq.heappop(self.queue)
            i = self.task_counter
            self.currently_running[i] = copy.deepcopy(item)
            self.task_counter += 1
            self.server.queue_updated()
            return (item, i)

    def task_done(
        self,
        item_id: int,
        history_result: dict,
        status: Optional["DistributedPromptQueue.ExecutionStatus"],
        process_item: Optional[Callable] = None,
    ) -> None:
        with self.mutex:
            prompt = self.currently_running.pop(item_id)
            if len(self.history) > MAXIMUM_HISTORY_SIZE:
                self.history.pop(next(iter(self.history)))
            status_dict: Optional[dict] = None
            if status is not None:
                status_dict = copy.deepcopy(status._asdict())
            if process_item is not None:
                prompt = process_item(prompt)
            self.history[prompt[1]] = {
                "prompt": prompt,
                "outputs": {},
                "status": status_dict,
            }
            self.history[prompt[1]].update(history_result)
            self.server.queue_updated()

    def get_current_queue(self) -> tuple[list, list]:
        with self.mutex:
            out = [x for x in self.currently_running.values()]
            return (out, copy.deepcopy(self.queue))

    def get_current_queue_volatile(self) -> tuple[list, list]:
        with self.mutex:
            running = [x for x in self.currently_running.values()]
            queued = copy.copy(self.queue)
            return (running, queued)

    def get_tasks_remaining(self) -> int:
        with self.mutex:
            return len(self.queue) + len(self.currently_running)

    def wipe_queue(self) -> None:
        with self.mutex:
            self.queue = []
            self.server.queue_updated()

    def delete_queue_item(self, function: Callable[[tuple], bool]) -> bool:
        with self.mutex:
            for x in range(len(self.queue)):
                if function(self.queue[x]):
                    if len(self.queue) == 1:
                        self.wipe_queue()
                    else:
                        self.queue.pop(x)
                        heapq.heapify(self.queue)
                    self.server.queue_updated()
                    return True
        return False

    def get_history(self, prompt_id=None, max_items=None, offset=-1, map_function=None) -> dict:
        with self.mutex:
            if prompt_id is None:
                out = {}
                i = 0
                if offset < 0 and max_items is not None:
                    offset = len(self.history) - max_items
                for k in self.history:
                    if i >= offset:
                        p = self.history[k]
                        if map_function is not None:
                            p = map_function(p)
                        out[k] = p
                        if max_items is not None and len(out) >= max_items:
                            break
                    i += 1
                return out
            elif prompt_id in self.history:
                p = self.history[prompt_id]
                p = copy.deepcopy(p) if map_function is None else map_function(p)
                return {prompt_id: p}
            else:
                return {}

    def wipe_history(self) -> None:
        with self.mutex:
            self.history = {}

    def delete_history_item(self, id_to_delete: str) -> None:
        with self.mutex:
            self.history.pop(id_to_delete, None)

    def set_flag(self, name: str, data: Any) -> None:
        with self.mutex:
            self.flags[name] = data
            self.not_empty.notify()

    def get_flags(self, reset: bool = True) -> dict:
        with self.mutex:
            if reset:
                ret = self.flags
                self.flags = {}
                return ret
            return self.flags.copy()


def _resolve_base_prompt_queue():
    if "execution" in sys.modules and hasattr(sys.modules["execution"], "PromptQueue"):
        return sys.modules["execution"].PromptQueue
    return _FallbackPromptQueue


class DistributedPromptQueue(_resolve_base_prompt_queue()):
    """
    Bidirectionally synchronized extension of execution.PromptQueue.
    Maintains a Lamport logical clock for total queue ordering across nodes and
    mirrors peer-queued and peer-running jobs for unified /queue, /history, and /api/jobs views.
    """

    def __init__(self, prompt_server: Any, sync_manager: Optional["ClusterSyncManager"] = None):
        super().__init__(prompt_server)
        self.sync_manager = sync_manager
        self.lamport_clock: int = int(time.time() * 1000)
        self.remote_running: dict[str, tuple] = {}
        self.remote_queued: dict[str, tuple] = {}

    @property
    def node_id(self) -> str:
        if self.sync_manager is not None:
            return self.sync_manager.node_id
        return getattr(args, "sync_node_id", "node_a")

    @property
    def node_parity(self) -> int:
        if self.sync_manager is not None:
            return self.sync_manager.node_parity
        parity = getattr(args, "sync_node_parity", -1)
        if parity in (0, 1):
            return parity
        return 1 if self.node_id.endswith("a") or self.node_id.endswith("1") else 0

    @property
    def work_stealing(self) -> bool:
        if self.sync_manager is not None:
            return self.sync_manager.work_stealing
        return bool(getattr(args, "sync_work_stealing", False))

    def next_lamport_number(self, front: bool = False) -> float:
        with self.mutex:
            self.lamport_clock += 1
            tie_breaker = 0.1 if self.node_parity == 1 else 0.2
            val = float(self.lamport_clock) + tie_breaker
            return -val if front else val

    def observe_lamport(self, remote_number: float) -> None:
        with self.mutex:
            self.lamport_clock = max(self.lamport_clock, int(abs(float(remote_number))))

    def put(self, item: tuple, _from_peer: bool = False) -> None:
        number, prompt_id, prompt, extra_data, outputs_to_execute, sensitive = item
        if not isinstance(extra_data, dict):
            extra_data = {}
        extra_data.setdefault("origin_node", self.node_id if not _from_peer else "peer")
        if not self.work_stealing:
            extra_data.setdefault("executor_node", extra_data["origin_node"])

        normalized_item = (float(number), prompt_id, prompt, extra_data, outputs_to_execute, sensitive)

        with self.mutex:
            self.observe_lamport(number)
            target_executor = extra_data.get("executor_node", self.node_id)
            if _from_peer and target_executor != self.node_id and not self.work_stealing:
                self.remote_queued[prompt_id] = normalized_item
                self.server.queue_updated()
            else:
                heapq.heappush(self.queue, normalized_item)
                self.server.queue_updated()
                self.not_empty.notify()

        if self.sync_manager is not None:
            ws_user = extra_data.get("workspace_user")
            if not ws_user:
                ws_user = self.sync_manager.get_client_username(extra_data.get("client_id"))
            ws_user = (ws_user or "default").strip() or "default"
            extra_data["workspace_user"] = ws_user
            self.sync_manager.prompt_user_map[prompt_id] = ws_user

            if not _from_peer:
                safe_item = (float(number), prompt_id, prompt, extra_data, outputs_to_execute, {})
                self.sync_manager.emit_peer_event("queue_put", {"item": safe_item})

            # Capture workflow from extra_pnginfo if present so browsers viewing this user sync the queued graph
            wf = extra_data.get("extra_pnginfo", {}).get("workflow")
            if isinstance(wf, dict) and wf.get("nodes"):
                self.sync_manager.update_workspace_graph(
                    wf,
                    sender_sid=extra_data.get("client_id"),
                    from_peer=True,
                    workspace_user=ws_user,
                )

    def get(self, timeout: Optional[float] = None) -> Optional[tuple[tuple, int]]:
        result = super().get(timeout=timeout)
        if result is not None and self.sync_manager is not None:
            item, item_id = result
            prompt_id = item[1]
            extra_data = item[3] if len(item) > 3 and isinstance(item[3], dict) else {}
            ws_user = (
                extra_data.get("workspace_user")
                or self.sync_manager.prompt_user_map.get(prompt_id)
                or self.sync_manager.get_client_username(extra_data.get("client_id"))
                or "default"
            )
            self.sync_manager.active_execution_user = ws_user
            safe_item = (item[0], item[1], item[2], item[3], item[4], {})
            self.sync_manager.emit_peer_event(
                "queue_running",
                {"prompt_id": prompt_id, "item": safe_item, "executor_node": self.node_id, "workspace_user": ws_user},
            )
        return result

    def task_done(
        self,
        item_id: int,
        history_result: dict,
        status: Optional[Any],
        process_item: Optional[Callable] = None,
    ) -> None:
        with self.mutex:
            prompt = self.currently_running.get(item_id)
            prompt_id = prompt[1] if prompt else None
            super().task_done(item_id, history_result, status, process_item=process_item)
            history_entry = copy.deepcopy(self.history.get(prompt_id)) if prompt_id else None

        if prompt_id and history_entry and self.sync_manager is not None:
            self.sync_manager.on_local_task_done(prompt_id, history_entry)

    def apply_peer_running(self, prompt_id: str, item: tuple) -> None:
        with self.mutex:
            self.remote_queued.pop(prompt_id, None)
            self.queue = [x for x in self.queue if x[1] != prompt_id]
            heapq.heapify(self.queue)
            normalized = tuple(item)
            if len(normalized) == 5:
                normalized = normalized + ({},)
            self.remote_running[prompt_id] = normalized
            self.server.queue_updated()

    def apply_peer_task_done(self, prompt_id: str, history_entry: dict) -> None:
        with self.mutex:
            self.remote_queued.pop(prompt_id, None)
            self.remote_running.pop(prompt_id, None)
            self.queue = [x for x in self.queue if x[1] != prompt_id]
            heapq.heapify(self.queue)

            if len(self.history) >= MAXIMUM_HISTORY_SIZE:
                self.history.pop(next(iter(self.history)))

            entry_copy = copy.deepcopy(history_entry)
            if isinstance(entry_copy.get("prompt"), list):
                entry_copy["prompt"] = tuple(entry_copy["prompt"])
            self.history[prompt_id] = entry_copy
            self.server.queue_updated()

    def get_current_queue(self) -> tuple[list, list]:
        with self.mutex:
            running, queued = super().get_current_queue()
            running.extend(copy.deepcopy(list(self.remote_running.values())))
            combined_queued = queued + copy.deepcopy(list(self.remote_queued.values()))
            combined_queued.sort(key=lambda x: x[0])
            return (running, combined_queued)

    def get_current_queue_volatile(self) -> tuple[list, list]:
        with self.mutex:
            running, queued = super().get_current_queue_volatile()
            running.extend(list(self.remote_running.values()))
            combined_queued = list(queued) + list(self.remote_queued.values())
            combined_queued.sort(key=lambda x: x[0])
            return (running, combined_queued)

    def get_tasks_remaining(self) -> int:
        with self.mutex:
            return (
                len(self.queue)
                + len(self.currently_running)
                + len(self.remote_queued)
                + len(self.remote_running)
            )

    def wipe_queue(self, _from_peer: bool = False) -> None:
        with self.mutex:
            self.queue = []
            self.remote_queued.clear()
            self.server.queue_updated()
        if not _from_peer and self.sync_manager is not None:
            self.sync_manager.emit_peer_event("queue_wipe", {})

    def delete_queue_item(
        self,
        function: Callable[[tuple], bool],
        _from_peer_id: Optional[str] = None,
    ) -> bool:
        deleted_id: Optional[str] = None
        with self.mutex:
            for x in range(len(self.queue)):
                if function(self.queue[x]):
                    deleted_id = self.queue[x][1]
                    self.queue.pop(x)
                    heapq.heapify(self.queue)
                    break
            if deleted_id is None:
                for pid, item in list(self.remote_queued.items()):
                    if function(item):
                        deleted_id = pid
                        self.remote_queued.pop(pid, None)
                        break
            if deleted_id is not None:
                self.server.queue_updated()

        if deleted_id is not None and _from_peer_id is None and self.sync_manager is not None:
            self.sync_manager.emit_peer_event("queue_delete", {"prompt_id": deleted_id})
        return deleted_id is not None

    def wipe_history(self, _from_peer: bool = False) -> None:
        super().wipe_history()
        if not _from_peer and self.sync_manager is not None:
            self.sync_manager.emit_peer_event("history_wipe", {})

    def delete_history_item(self, id_to_delete: str, _from_peer: bool = False) -> None:
        super().delete_history_item(id_to_delete)
        if not _from_peer and self.sync_manager is not None:
            self.sync_manager.emit_peer_event("history_delete", {"prompt_id": id_to_delete})


class ClusterSyncManager:
    """
    Coordinates real-time bidirectional synchronization across:
    1. Multiple browsers/devices connected to the same ComfyUI server (e.g. via Cloudflare Tunnel).
    2. Multiple independent ComfyUI servers (Device A & Device B) connected via full-duplex WebSocket.
    """

    # Note: "status" is intentionally excluded from unconditional sid=None override because
    # server.py sends {"status": ..., "sid": sid} unicast during initial /ws connection.
    # Broadcasting a "status" message containing "sid" would overwrite other browsers' clientId!
    BROADCAST_EVENTS = frozenset({
        "executing",
        "execution_start",
        "execution_cached",
        "executed",
        "execution_error",
        "execution_interrupted",
        "execution_success",
        "progress",
        "progress_state",
        "sync_conflict",
        "userdata_updated",
        "sync_workspace",
        "sync_full_state",
        "history_updated",
    })

    def __init__(
        self,
        prompt_server: Any,
        node_id: Optional[str] = None,
        peer_url: Optional[str] = None,
        sync_secret: Optional[str] = None,
        node_parity: Optional[int] = None,
        work_stealing: Optional[bool] = None,
    ):
        self.server = prompt_server
        self.loop = prompt_server.loop
        self.node_id: str = node_id or getattr(args, "sync_node_id", "node_a")
        raw_peer = peer_url if peer_url is not None else getattr(args, "sync_peer_url", None)
        self.peer_url: str = (raw_peer or "").rstrip("/")
        self.sync_secret: str = sync_secret or getattr(args, "sync_secret", "comfy-cluster-sync")
        self.self_tunnel_url: Optional[str] = None

        cfg_parity = node_parity if node_parity is not None else getattr(args, "sync_node_parity", -1)
        if cfg_parity in (0, 1):
            self.node_parity = cfg_parity
        elif self.peer_url:
            self.node_parity = 1 if (self.node_id.endswith("a") or self.node_id.endswith("1")) else 0
            args.sync_node_parity = self.node_parity
        else:
            self.node_parity = -1

        self.work_stealing: bool = (
            work_stealing if work_stealing is not None else bool(getattr(args, "sync_work_stealing", False))
        )

        self.peer_ws: Optional[Any] = None
        self.inbound_peer_ws: Optional[Any] = None
        self._inbound_send_lock: asyncio.Lock = asyncio.Lock()
        self._hooks_installed: bool = False
        self.outbound_queue: asyncio.Queue = asyncio.Queue(maxsize=4096)
        self.seq_id: int = 0
        self.peer_last_seq: int = 0
        self.peer_connected: bool = False
        self._connector_task: Optional[asyncio.Task] = None
        self._discovery_task: Optional[asyncio.Task] = None
        self._scan_task: Optional[asyncio.Task] = None
        self._seen_event_ids: set[str] = set()
        self._pending_asset_fetches: dict[str, asyncio.Future] = {}

        # Per-user independent workspaces + live cross-tab spectator tracking
        self.user_workspaces: dict[str, dict[str, Any]] = {}
        self.client_states: dict[str, dict[str, Any]] = {}
        self.prompt_user_map: dict[str, str] = {}
        self.active_execution_user: str = "default"
        self.workspace_state: dict[str, Any] = self.get_or_create_workspace("default")

        try:
            self.journal_dir = folder_paths.get_system_user_directory("sync")
            os.makedirs(self.journal_dir, exist_ok=True)
            self.journal_path: Optional[str] = os.path.join(self.journal_dir, "journal.jsonl")
            self.state_cache_path: Optional[str] = os.path.join(self.journal_dir, "workspace_state.json")
            self._load_cached_workspace_state()
        except Exception:
            self.journal_path = None
            self.state_cache_path = None

    def get_or_create_workspace(self, username: Optional[str] = None) -> dict[str, Any]:
        key = (username or "default").strip() or "default"
        if key not in self.user_workspaces:
            self.user_workspaces[key] = {
                "username": key,
                "workflow": None,
                "rev": 0,
                "ts": 0,
                "sender": None,
                "node_outputs": {},
                "executing_prompt_id": None,
                "executing_node": None,
                "progress": None,
                "last_active": int(time.time() * 1000),
            }
        return self.user_workspaces[key]

    def get_client_username(self, sid: Optional[str]) -> str:
        if not sid:
            return "default"
        return (self.client_states.get(sid, {}).get("username") or "default").strip() or "default"

    def get_client_viewing_user(self, sid: Optional[str]) -> str:
        if not sid:
            return "default"
        st = self.client_states.get(sid)
        if not st:
            return "default"
        return (st.get("viewing_user") or st.get("username") or "default").strip() or "default"

    def get_target_sids_for_workspace(
        self, workspace_user: str, exclude_sid: Optional[str] = None
    ) -> Optional[list[str]]:
        """
        Return socket sids currently viewing `workspace_user`.
        Returns None when no browser has registered a multi-user identity yet (fallback to broadcast all).
        """
        if not self.client_states:
            return None
        ws_key = (workspace_user or "default").strip() or "default"
        matched: list[str] = []
        for sock_sid in list(self.server.sockets.keys()):
            if exclude_sid and sock_sid == exclude_sid:
                continue
            if self.get_client_viewing_user(sock_sid) == ws_key:
                matched.append(sock_sid)
        return matched

    def get_users_summary(self) -> list[dict[str, Any]]:
        all_names: set[str] = set(self.user_workspaces.keys())
        for st in self.client_states.values():
            uname = (st.get("username") or "").strip()
            if uname:
                all_names.add(uname)

        named_users = {u for u in all_names if u != "default"}
        active_sids = set(self.server.sockets.keys())
        default_has_clients = any(
            (st.get("username") or "default") == "default" and s in active_sids
            for s, st in self.client_states.items()
        )
        default_ws = self.user_workspaces.get("default", {})
        if named_users and not default_has_clients and not default_ws.get("workflow"):
            all_names.discard("default")

        summary: list[dict[str, Any]] = []
        for uname in sorted(all_names):
            ws_state = self.get_or_create_workspace(uname)
            owner_sids = [
                s
                for s, st in self.client_states.items()
                if s in active_sids and (st.get("username") or "default") == uname
            ]
            watcher_sids = [
                s
                for s, st in self.client_states.items()
                if s in active_sids and (st.get("viewing_user") or st.get("username") or "default") == uname
            ]
            summary.append({
                "username": uname,
                "online": len(owner_sids) > 0,
                "client_count": len(owner_sids),
                "watchers_count": len(watcher_sids),
                "executing": ws_state.get("executing_node") is not None
                or ws_state.get("executing_prompt_id") is not None,
                "executing_node": ws_state.get("executing_node"),
                "progress": ws_state.get("progress"),
                "rev": ws_state.get("rev", 0),
                "has_workflow": bool(ws_state.get("workflow")),
                "last_active": ws_state.get("last_active", 0),
            })
        return summary

    def schedule_broadcast_users_list(self) -> None:
        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is self.loop:
                self.loop.create_task(self._broadcast_users_list())
            else:
                self.loop.call_soon_threadsafe(lambda: self.loop.create_task(self._broadcast_users_list()))
        except RuntimeError:
            pass

    async def _broadcast_users_list(self) -> None:
        msg = {"type": "sync_users_list", "data": {"users": self.get_users_summary()}}
        for ws in list(self.server.sockets.values()):
            try:
                await ws.send_json(msg)
            except Exception:
                pass

    def _load_cached_workspace_state(self) -> None:
        if self.state_cache_path and os.path.isfile(self.state_cache_path):
            try:
                with open(self.state_cache_path, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                if isinstance(saved, dict):
                    if isinstance(saved.get("user_workspaces"), dict):
                        for uname, ustate in saved["user_workspaces"].items():
                            if isinstance(ustate, dict):
                                target = self.get_or_create_workspace(uname)
                                target["workflow"] = ustate.get("workflow")
                                target["rev"] = ustate.get("rev", 0)
                                target["ts"] = ustate.get("ts", 0)
                                target["node_outputs"] = ustate.get("node_outputs", {})
                                target["last_active"] = ustate.get("last_active", 0)
                    elif saved.get("workflow"):
                        self.workspace_state["workflow"] = saved["workflow"]
                        self.workspace_state["rev"] = saved.get("rev", 1)
                        self.workspace_state["ts"] = saved.get("ts", 0)
                        self.workspace_state["node_outputs"] = saved.get("node_outputs", {})
            except Exception:
                pass

    def _save_cached_workspace_state(self) -> None:
        if not self.state_cache_path:
            return
        try:
            serialized_users = {}
            for uname, ustate in self.user_workspaces.items():
                serialized_users[uname] = {
                    "username": uname,
                    "workflow": ustate.get("workflow"),
                    "rev": ustate.get("rev", 0),
                    "ts": ustate.get("ts", 0),
                    "node_outputs": ustate.get("node_outputs", {}),
                    "last_active": ustate.get("last_active", 0),
                }
            payload = {
                "workflow": self.workspace_state.get("workflow"),
                "rev": self.workspace_state.get("rev", 0),
                "ts": self.workspace_state.get("ts", 0),
                "node_outputs": self.workspace_state.get("node_outputs", {}),
                "user_workspaces": serialized_users,
            }
            atomic_write_bytes(self.state_cache_path, json.dumps(payload).encode("utf-8"))
        except Exception:
            pass

    @property
    def enabled(self) -> bool:
        """True if either a peer URL is configured OR an inbound peer WebSocket is connected."""
        return bool(self.peer_url) or (
            self.inbound_peer_ws is not None and not getattr(self.inbound_peer_ws, "closed", False)
        )

    @property
    def has_active_peer_ws(self) -> bool:
        out_ok = self.peer_ws is not None and not getattr(self.peer_ws, "closed", False)
        in_ok = self.inbound_peer_ws is not None and not getattr(self.inbound_peer_ws, "closed", False)
        return out_ok or in_ok

    def install_hooks(self) -> None:
        """Attach Lamport prompt numbering and WebSocket broadcast fan-out hooks to PromptServer."""
        def assign_lamport_number(json_data: dict) -> dict:
            if "number" not in json_data and isinstance(self.server.prompt_queue, DistributedPromptQueue):
                front = bool(json_data.get("front", False))
                json_data["number"] = self.server.prompt_queue.next_lamport_number(front=front)
            client_id = json_data.get("client_id")
            if client_id:
                extra_data = json_data.setdefault("extra_data", {})
                if isinstance(extra_data, dict) and not extra_data.get("workspace_user"):
                    extra_data["workspace_user"] = self.get_client_username(client_id)
            return json_data

        self.server.add_on_prompt_handler(assign_lamport_number)

        orig_send = self.server.send

        async def hooked_send(event, data, sid=None, _from_peer: bool = False):
            # Resolve which user's workspace this event belongs to
            event_ws_user = self._resolve_event_workspace_user(event, data, sid)
            self._track_execution_event(event, data, workspace_user=event_ws_user)

            # Never broadcast initial "status" message containing "sid" (would overwrite other browsers' clientId!)
            if event == "status":
                is_initial_sid_handshake = isinstance(data, dict) and "sid" in data
                target_sid = sid if is_initial_sid_handshake else None
                await orig_send(event, data, sid=target_sid)
            elif event in ("userdata_updated", "sync_conflict", "sync_users_list"):
                await orig_send(event, data, sid=None)
            else:
                is_broadcast_event = (
                    event in self.BROADCAST_EVENTS
                    or isinstance(event, int)
                    or event in (
                        BinaryEventTypes.UNENCODED_PREVIEW_IMAGE,
                        BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA,
                        BinaryEventTypes.PREVIEW_IMAGE,
                        BinaryEventTypes.TEXT,
                    )
                )
                target_sids = (
                    self.get_target_sids_for_workspace(event_ws_user) if is_broadcast_event else None
                )
                if event == BinaryEventTypes.UNENCODED_PREVIEW_IMAGE:
                    await self._fanout_unencoded_preview(data, target_sids=target_sids)
                elif event == BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA:
                    await self._fanout_unencoded_preview(data[0], metadata=data[1], target_sids=target_sids)
                elif target_sids is not None:
                    for t_sid in target_sids:
                        await orig_send(event, data, sid=t_sid)
                else:
                    target_sid = None if is_broadcast_event else sid
                    await orig_send(event, data, sid=target_sid)

            if event in (
                "execution_start",
                "executing",
                "execution_success",
                "execution_error",
                "execution_interrupted",
            ):
                self.schedule_broadcast_users_list()

            # When a node produces output images/files locally, push those files to peer immediately
            if not _from_peer and event == "executed" and isinstance(data, dict):
                self._schedule_push_executed_outputs(data.get("output"))

            if not _from_peer and self.enabled:
                await self._forward_ui_event_to_peer(event, data, workspace_user=event_ws_user)

        self.server.send = hooked_send
        self._hooks_installed = True

    async def _send_to_local_clients(self, event: Any, data: Any, sid: Optional[str] = None) -> None:
        if self._hooks_installed:
            await self.server.send(event, data, sid=sid, _from_peer=True)
        else:
            await self.server.send(event, data, sid=sid)

    def observe_request_host(self, request: Any) -> None:
        """Auto-detect Cloudflare tunnel URL from incoming HTTP/WS request headers."""
        if self.self_tunnel_url:
            return
        try:
            for hdr in ("X-Forwarded-Host", "Host", "Origin", "Referer"):
                val = request.headers.get(hdr, "")
                if "trycloudflare.com" in val:
                    m = re.search(r"([-a-zA-Z0-9]+\.trycloudflare\.com)", val)
                    if m:
                        self.self_tunnel_url = f"https://{m.group(1)}"
                        logging.info(f"[ClusterSync] Auto-detected Cloudflare tunnel URL: {self.self_tunnel_url}")
                        break
        except Exception:
            pass

    def _resolve_event_workspace_user(self, event: Any, data: Any, sid: Optional[str]) -> str:
        if isinstance(data, dict):
            if data.get("workspace_user"):
                return str(data["workspace_user"]).strip() or "default"
            pid = data.get("prompt_id")
            if pid and pid in self.prompt_user_map:
                return self.prompt_user_map[pid]
        if event == BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA and isinstance(data, tuple) and len(data) > 1:
            meta = data[1]
            if isinstance(meta, dict):
                pid = meta.get("prompt_id")
                if pid and pid in self.prompt_user_map:
                    return self.prompt_user_map[pid]
        if sid and sid in self.client_states:
            return self.get_client_username(sid)
        return self.active_execution_user or "default"

    def _track_execution_event(self, event: Any, data: Any, workspace_user: str = "default") -> None:
        if not isinstance(data, dict):
            return
        ws_state = self.get_or_create_workspace(workspace_user)
        ws_state["last_active"] = int(time.time() * 1000)
        if event == "execution_start":
            ws_state["executing_prompt_id"] = data.get("prompt_id")
            ws_state["progress"] = None
        elif event == "executing":
            node = data.get("display_node") or data.get("node")
            ws_state["executing_node"] = node
            if node is None:
                ws_state["executing_prompt_id"] = None
                ws_state["progress"] = None
            elif data.get("prompt_id"):
                ws_state["executing_prompt_id"] = data.get("prompt_id")
        elif event == "progress":
            ws_state["progress"] = data
        elif event == "executed":
            node_id = str(data.get("display_node") or data.get("node") or "")
            output = data.get("output")
            if node_id and output:
                ws_state["node_outputs"][node_id] = output
                self._save_cached_workspace_state()
        elif event in ("execution_success", "execution_error", "execution_interrupted"):
            ws_state["executing_node"] = None
            ws_state["executing_prompt_id"] = None
            ws_state["progress"] = None

    def update_workspace_graph(
        self,
        workflow: dict[str, Any],
        sender_sid: Optional[str] = None,
        ts: Optional[int] = None,
        from_peer: bool = False,
        workspace_user: Optional[str] = None,
    ) -> bool:
        """Update the authoritative live canvas workflow for `workspace_user` and broadcast to browsers viewing that user."""
        if not isinstance(workflow, dict):
            return False
        ws_user = (workspace_user or self.get_client_username(sender_sid) or "default").strip() or "default"
        ws_state = self.get_or_create_workspace(ws_user)

        now_ts = ts if ts is not None else int(time.time() * 1000)
        current_ts = ws_state.get("ts", 0)
        if from_peer and ts is not None and now_ts < current_ts:
            return False
        if not from_peer and now_ts <= current_ts:
            now_ts = max(current_ts + 1, int(time.time() * 1000))

        ws_state["workflow"] = workflow
        ws_state["rev"] = int(ws_state.get("rev", 0)) + 1
        ws_state["ts"] = now_ts
        ws_state["sender"] = sender_sid
        ws_state["last_active"] = now_ts
        self._save_cached_workspace_state()

        msg_data = {
            "workspace_user": ws_user,
            "workflow": workflow,
            "rev": ws_state["rev"],
            "ts": now_ts,
            "sender": sender_sid,
            "origin": self.node_id,
        }
        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is self.loop:
                self.loop.create_task(
                    self._broadcast_workspace_to_local_browsers(
                        msg_data, exclude_sid=sender_sid, workspace_user=ws_user
                    )
                )
            else:
                self.loop.call_soon_threadsafe(
                    lambda: self.loop.create_task(
                        self._broadcast_workspace_to_local_browsers(
                            msg_data, exclude_sid=sender_sid, workspace_user=ws_user
                        )
                    )
                )
        except RuntimeError:
            pass

        if not from_peer and self.enabled:
            self.emit_peer_event("workspace_sync", msg_data, journal=False)
        return True

    async def _broadcast_workspace_to_local_browsers(
        self,
        msg_data: dict[str, Any],
        exclude_sid: Optional[str] = None,
        workspace_user: str = "default",
    ) -> None:
        message = {"type": "sync_workspace", "data": msg_data}
        target_sids = self.get_target_sids_for_workspace(workspace_user, exclude_sid=exclude_sid)
        if target_sids is None:
            for sock_sid, ws in list(self.server.sockets.items()):
                if exclude_sid and sock_sid == exclude_sid:
                    continue
                try:
                    await ws.send_json(message)
                except Exception:
                    pass
        else:
            for sock_sid in target_sids:
                ws = self.server.sockets.get(sock_sid)
                if ws is not None:
                    try:
                        await ws.send_json(message)
                    except Exception:
                        pass

    async def on_client_connected(
        self, sid: str, ws: Any, workspace_user: Optional[str] = None
    ) -> None:
        """Send current live workspace state, node outputs, execution state, and active user tabs to a browser."""
        if ws is None or getattr(ws, "closed", False):
            return
        try:
            ws_user = (workspace_user or self.get_client_viewing_user(sid) or "default").strip() or "default"
            ws_state = self.get_or_create_workspace(ws_user)
            last_node = getattr(self.server, "last_node_id", None) if ws_user == self.active_execution_user else None
            active_node = ws_state.get("executing_node") or last_node
            await ws.send_json({
                "type": "sync_full_state",
                "data": {
                    "node_id": self.node_id,
                    "peer_url": self.peer_url,
                    "peer_connected": self.has_active_peer_ws,
                    "self_tunnel_url": self.self_tunnel_url,
                    "connected_browsers": len(self.server.sockets),
                    "workspace_user": ws_user,
                    "username": self.get_client_username(sid),
                    "viewing_user": ws_user,
                    "users": self.get_users_summary(),
                    "workflow": ws_state.get("workflow"),
                    "rev": ws_state.get("rev", 0),
                    "ts": ws_state.get("ts", 0),
                    "node_outputs": ws_state.get("node_outputs", {}),
                    "executing_prompt_id": ws_state.get("executing_prompt_id"),
                    "executing_node": active_node,
                    "progress": ws_state.get("progress"),
                },
            })
            if active_node is not None:
                await ws.send_json({
                    "type": "executing",
                    "data": {
                        "node": active_node,
                        "display_node": active_node,
                        "prompt_id": ws_state.get("executing_prompt_id") or getattr(self.server, "last_prompt_id", None),
                        "workspace_user": ws_user,
                    },
                })
        except Exception as e:
            logging.debug(f"[ClusterSync] Failed sending initial state to {sid}: {e}")

    async def on_client_disconnected(self, sid: str) -> None:
        """Clean up disconnected browser session while preserving its user workspace."""
        removed = self.client_states.pop(sid, None)
        if removed is not None:
            await self._broadcast_users_list()

    async def handle_client_ws_message(self, sid: str, data: dict[str, Any]) -> None:
        """Process real-time synchronization messages sent from a browser over /ws."""
        msg_type = data.get("type")
        payload = data.get("data") or {}
        if msg_type == "sync_identify":
            username = (payload.get("username") or "").strip() or "default"
            viewing_user = (payload.get("viewing_user") or username).strip() or username
            now_ms = int(time.time() * 1000)
            self.client_states[sid] = {
                "sid": sid,
                "username": username,
                "viewing_user": viewing_user,
                "connected_at": now_ms,
            }
            user_ws = self.get_or_create_workspace(username)
            user_ws["last_active"] = now_ms
            init_wf = payload.get("initial_workflow")
            if isinstance(init_wf, dict) and init_wf.get("nodes") and not user_ws.get("workflow"):
                user_ws["workflow"] = init_wf
                user_ws["rev"] = 1
                user_ws["ts"] = now_ms
                self._save_cached_workspace_state()

            ws = self.server.sockets.get(sid)
            if ws is not None:
                await self.on_client_connected(sid, ws, workspace_user=viewing_user)
            await self._broadcast_users_list()
            if self.enabled:
                self.emit_peer_event(
                    "user_presence",
                    {"username": username, "viewing_user": viewing_user, "ts": now_ms},
                    journal=False,
                )

        elif msg_type == "sync_switch_view":
            viewing_user = (payload.get("viewing_user") or "").strip() or self.get_client_username(sid)
            st = self.client_states.setdefault(
                sid,
                {
                    "sid": sid,
                    "username": (payload.get("username") or "default").strip() or "default",
                    "viewing_user": viewing_user,
                    "connected_at": int(time.time() * 1000),
                },
            )
            st["viewing_user"] = viewing_user
            self.get_or_create_workspace(viewing_user)
            ws = self.server.sockets.get(sid)
            if ws is not None:
                await self.on_client_connected(sid, ws, workspace_user=viewing_user)
            await self._broadcast_users_list()

        elif msg_type == "sync_request_state":
            req_user = (payload.get("workspace_user") or "").strip() or None
            ws = self.server.sockets.get(sid)
            if ws is not None:
                await self.on_client_connected(sid, ws, workspace_user=req_user)

        elif msg_type == "sync_workspace":
            wf = payload.get("workflow")
            ws_user = (payload.get("workspace_user") or self.get_client_username(sid) or "default").strip()
            if isinstance(wf, dict):
                self.update_workspace_graph(
                    wf,
                    sender_sid=sid,
                    ts=payload.get("ts"),
                    from_peer=False,
                    workspace_user=ws_user,
                )

        elif msg_type == "sync_cursor":
            ws_user = (payload.get("workspace_user") or self.get_client_viewing_user(sid) or "default").strip()
            cursor_msg = {
                "username": payload.get("username") or self.get_client_username(sid),
                "workspace_user": ws_user,
                "x": payload.get("x"),
                "y": payload.get("y"),
                "node_id": payload.get("node_id"),
                "sender": sid,
                "ts": int(time.time() * 1000),
            }
            target_sids = self.get_target_sids_for_workspace(ws_user, exclude_sid=sid)
            if target_sids:
                out_msg = {"type": "sync_cursor", "data": cursor_msg}
                for t_sid in target_sids:
                    ws = self.server.sockets.get(t_sid)
                    if ws is not None:
                        try:
                            await ws.send_json(out_msg)
                        except Exception:
                            pass
            if self.enabled:
                self.emit_peer_event("cursor_sync", cursor_msg, journal=False)

        elif msg_type == "sync_node_outputs":
            outputs = payload.get("node_outputs")
            ws_user = (payload.get("workspace_user") or self.get_client_username(sid) or "default").strip()
            if isinstance(outputs, dict):
                ws_state = self.get_or_create_workspace(ws_user)
                ws_state["node_outputs"].update(outputs)
                self._save_cached_workspace_state()
                await self._broadcast_json_to_workspace(
                    "sync_node_outputs",
                    {"node_outputs": outputs, "workspace_user": ws_user, "sender": sid},
                    workspace_user=ws_user,
                    exclude_sid=sid,
                )
                if self.enabled:
                    self.emit_peer_event(
                        "node_outputs_sync",
                        {"node_outputs": outputs, "workspace_user": ws_user},
                        journal=False,
                    )

    async def _broadcast_json_to_workspace(
        self,
        event: str,
        data: dict,
        workspace_user: str = "default",
        exclude_sid: Optional[str] = None,
    ) -> None:
        message = {"type": event, "data": data}
        target_sids = self.get_target_sids_for_workspace(workspace_user, exclude_sid=exclude_sid)
        if target_sids is None:
            await self._broadcast_json_except(event, data, exclude_sid=exclude_sid)
            return
        for sock_sid in target_sids:
            ws = self.server.sockets.get(sock_sid)
            if ws is not None:
                try:
                    await ws.send_json(message)
                except Exception:
                    pass

    async def _broadcast_json_except(self, event: str, data: dict, exclude_sid: Optional[str] = None) -> None:
        message = {"type": event, "data": data}
        for sock_sid, ws in list(self.server.sockets.items()):
            if exclude_sid and sock_sid == exclude_sid:
                continue
            try:
                await ws.send_json(message)
            except Exception:
                pass

    def on_local_task_done(self, prompt_id: str, history_entry: dict[str, Any]) -> None:
        """Called when a local prompt finishes execution and is recorded in PromptQueue.history."""
        prompt_tuple = history_entry.get("prompt")
        extra_data = (
            prompt_tuple[3]
            if isinstance(prompt_tuple, (tuple, list)) and len(prompt_tuple) > 3 and isinstance(prompt_tuple[3], dict)
            else {}
        )
        ws_user = (
            extra_data.get("workspace_user")
            or self.prompt_user_map.get(prompt_id)
            or self.active_execution_user
            or "default"
        )
        ws_state = self.get_or_create_workspace(ws_user)
        ws_state["executing_node"] = None
        ws_state["executing_prompt_id"] = None
        ws_state["progress"] = None

        outputs = history_entry.get("outputs")
        if isinstance(outputs, dict) and outputs:
            for node_id, node_out in outputs.items():
                if isinstance(node_out, dict):
                    ws_state["node_outputs"][str(node_id)] = node_out
            self._save_cached_workspace_state()
        if self.enabled:
            self.emit_peer_event(
                "task_done",
                {"prompt_id": prompt_id, "history_entry": history_entry, "workspace_user": ws_user},
            )
        self.schedule_asset_scan(("output",))
        self.schedule_broadcast_users_list()
        if hasattr(self.server, "send_sync"):
            self.server.send_sync(
                "history_updated",
                {
                    "prompt_id": prompt_id,
                    "workspace_user": ws_user,
                    "outputs": outputs,
                    "status": history_entry.get("status"),
                },
                None,
            )

    def _schedule_push_executed_outputs(self, output_ui: Optional[dict]) -> None:
        if not self.enabled or not isinstance(output_ui, dict):
            return
        files_to_push: list[tuple[str, str, str]] = []
        for media_list in output_ui.values():
            if not isinstance(media_list, list):
                continue
            for item in media_list:
                if isinstance(item, dict) and "filename" in item:
                    fname = item["filename"]
                    subfolder = item.get("subfolder", "")
                    dir_type = item.get("type", "output")
                    files_to_push.append((dir_type, subfolder, fname))
        if files_to_push:
            try:
                self.loop.call_soon_threadsafe(
                    lambda: self.loop.create_task(self._push_assets_to_peer(files_to_push))
                )
            except RuntimeError:
                pass

    async def _push_assets_to_peer(self, files: list[tuple[str, str, str]]) -> None:
        for dir_type, subfolder, fname in files:
            base_dir = folder_paths.get_directory_by_type(dir_type)
            if not base_dir:
                continue
            full_path = os.path.join(base_dir, subfolder, os.path.basename(fname))
            if not os.path.isfile(full_path):
                continue
            try:
                size = os.path.getsize(full_path)
                if 0 < size <= MAX_INBAND_ASSET_BYTES:
                    data = await asyncio.to_thread(self._read_file_bytes, full_path)
                    self.emit_peer_event(
                        "asset_push",
                        {
                            "type": dir_type,
                            "subfolder": subfolder,
                            "filename": os.path.basename(fname),
                            "data_b64": base64.b64encode(data).decode("ascii"),
                        },
                        journal=False,
                    )
            except Exception as e:
                logging.debug(f"[ClusterSync] Failed pushing asset {fname}: {e}")

    @staticmethod
    def _read_file_bytes(path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def add_routes(self, routes: Any) -> None:
        """Register peer synchronization WebSocket, runtime configuration, and client JS routes."""
        @routes.get("/cluster/sync/ws")
        @routes.get("/internal/sync/ws")
        async def handle_peer_ws(request):
            return await self._handle_peer_ws(request)

        @routes.get("/cluster/sync/status")
        @routes.get("/internal/sync/status")
        async def handle_sync_status(request):
            return web.json_response(self.get_status_dict())

        @routes.post("/cluster/sync/config")
        async def handle_sync_config(request):
            body = await request.json()
            if "peer_url" in body:
                new_peer = (body.get("peer_url") or "").strip().rstrip("/")
                self.configure_peer(new_peer, node_id=body.get("node_id"), work_stealing=body.get("work_stealing"))
            return web.json_response(self.get_status_dict())

        @routes.get("/cluster/sync/users")
        async def handle_get_users(request):
            self.observe_request_host(request)
            return web.json_response({"users": self.get_users_summary()})

        @routes.get("/cluster/sync/workspace")
        async def handle_get_workspace(request):
            self.observe_request_host(request)
            req_user = (request.rel_url.query.get("user") or "").strip() or "default"
            ws_state = self.get_or_create_workspace(req_user)
            last_node = (
                getattr(self.server, "last_node_id", None) if req_user == self.active_execution_user else None
            )
            return web.json_response({
                "node_id": self.node_id,
                "peer_url": self.peer_url,
                "peer_connected": self.has_active_peer_ws,
                "self_tunnel_url": self.self_tunnel_url,
                "connected_browsers": len(self.server.sockets),
                "workspace_user": req_user,
                "users": self.get_users_summary(),
                "workflow": ws_state.get("workflow"),
                "rev": ws_state.get("rev", 0),
                "ts": ws_state.get("ts", 0),
                "node_outputs": ws_state.get("node_outputs", {}),
                "executing_prompt_id": ws_state.get("executing_prompt_id"),
                "executing_node": ws_state.get("executing_node") or last_node,
                "progress": ws_state.get("progress"),
            })

        @routes.post("/cluster/sync/workspace")
        async def handle_post_workspace(request):
            body = await request.json()
            wf = body.get("workflow")
            sender = body.get("sender")
            ws_user = (body.get("workspace_user") or self.get_client_username(sender) or "default").strip()
            if isinstance(wf, dict):
                self.update_workspace_graph(
                    wf, sender_sid=sender, ts=body.get("ts"), from_peer=False, workspace_user=ws_user
                )
            ws_state = self.get_or_create_workspace(ws_user)
            return web.json_response({"ok": True, "workspace_user": ws_user, "rev": ws_state.get("rev", 0)})

        @routes.get("/cluster/sync/client.js")
        async def handle_sync_client_js(request):
            js_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                "custom_nodes",
                "comfy_cluster_sync",
                "web",
                "sync_ui.js",
            )
            if os.path.isfile(js_path):
                return web.FileResponse(
                    js_path,
                    headers={
                        "Content-Type": "application/javascript; charset=utf-8",
                        "Cache-Control": "no-cache",
                    },
                )
            return web.Response(text="// sync_ui.js not found", content_type="application/javascript")

    def get_status_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_parity": self.node_parity,
            "peer_url": self.peer_url,
            "self_tunnel_url": self.self_tunnel_url,
            "enabled": self.enabled,
            "peer_connected": self.has_active_peer_ws,
            "work_stealing": self.work_stealing,
            "connected_browsers": len(self.server.sockets),
            "users": self.get_users_summary(),
            "workspace_rev": self.workspace_state.get("rev", 0),
            "seq_id": self.seq_id,
            "peer_last_seq": self.peer_last_seq,
            "lamport_clock": getattr(self.server.prompt_queue, "lamport_clock", 0),
            "remote_running_count": len(getattr(self.server.prompt_queue, "remote_running", {})),
            "remote_queued_count": len(getattr(self.server.prompt_queue, "remote_queued", {})),
        }

    def configure_peer(
        self,
        peer_url: str,
        node_id: Optional[str] = None,
        work_stealing: Optional[bool] = None,
    ) -> None:
        """Dynamically set or update the peer URL at runtime without restarting the server."""
        if node_id:
            self.node_id = node_id
        if work_stealing is not None:
            self.work_stealing = bool(work_stealing)

        cleaned = peer_url.strip().rstrip("/")
        if cleaned and self.self_tunnel_url and cleaned == self.self_tunnel_url.rstrip("/"):
            return  # Do not connect to self

        self.peer_url = cleaned
        if self.peer_url and self.node_parity not in (0, 1):
            self.node_parity = 1 if (self.node_id.endswith("a") or self.node_id.endswith("1")) else 0
            args.sync_node_parity = self.node_parity

        if self._connector_task and not self._connector_task.done():
            self._connector_task.cancel()
        if self.peer_url:
            self._connector_task = self.loop.create_task(self._peer_connection_loop())
            logging.info(f"[ClusterSync] Dynamically configured peer URL: {self.peer_url}")

    def start(self) -> None:
        """Start background peer connection loop and cloudflared / shared-folder auto-discovery."""
        if self._discovery_task is None or self._discovery_task.done():
            self._discovery_task = self.loop.create_task(self._auto_discovery_loop())
        if self.peer_url and (self._connector_task is None or self._connector_task.done()):
            self._connector_task = self.loop.create_task(self._peer_connection_loop())
            logging.info(
                f"[ClusterSync] Started on {self.node_id} (parity={self.node_parity}, peer={self.peer_url})"
            )

    async def _auto_discovery_loop(self) -> None:
        """
        Automatically discovers:
        1. Local cloudflared trycloudflare.com tunnel URL (via metrics port 20241 or process args).
        2. Peer node URLs written to user/default/cluster_nodes.json (when Google Drive or NFS is shared).
        """
        registry_dir = os.path.join(folder_paths.get_user_directory(), "default")
        os.makedirs(registry_dir, exist_ok=True)
        registry_file = os.path.join(registry_dir, "cluster_nodes.json")

        while True:
            try:
                # 1. Check local cloudflared metrics endpoint for trycloudflare.com URL
                if not self.self_tunnel_url and self.server.client_session:
                    for metrics_port in (20241, 20242, 20243, 45678):
                        try:
                            async with self.server.client_session.get(
                                f"http://127.0.0.1:{metrics_port}/metrics",
                                timeout=aiohttp.ClientTimeout(total=0.8),
                            ) as resp:
                                if resp.status == 200:
                                    text = await resp.text()
                                    m = re.search(r"https://[-a-zA-Z0-9]+\.trycloudflare\.com", text)
                                    if m:
                                        self.self_tunnel_url = m.group(0)
                                        logging.info(f"[ClusterSync] Detected local Cloudflare tunnel: {self.self_tunnel_url}")
                                        break
                        except Exception:
                            pass

                # 2. Register self & check for peer in user/default/cluster_nodes.json
                now_ts = int(time.time())
                nodes_map: dict[str, dict] = {}
                if os.path.isfile(registry_file):
                    try:
                        with open(registry_file, "r", encoding="utf-8") as f:
                            nodes_map = json.load(f)
                    except Exception:
                        nodes_map = {}

                if self.self_tunnel_url:
                    nodes_map[self.node_id] = {
                        "url": self.self_tunnel_url,
                        "ts": now_ts,
                    }
                    try:
                        atomic_write_bytes(registry_file, json.dumps(nodes_map, indent=2).encode("utf-8"))
                    except Exception:
                        pass

                if not self.peer_url and not self.has_active_peer_ws:
                    for other_id, info in nodes_map.items():
                        other_url = (info.get("url") or "").rstrip("/")
                        if (
                            other_id != self.node_id
                            and other_url
                            and other_url != (self.self_tunnel_url or "").rstrip("/")
                            and (now_ts - info.get("ts", 0)) < 3600
                        ):
                            logging.info(f"[ClusterSync] Auto-discovered peer {other_id} at {other_url}")
                            self.configure_peer(other_url)
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.debug(f"[ClusterSync] Auto-discovery tick error: {e}")

            await asyncio.sleep(5.0)

    async def _fanout_unencoded_preview(
        self,
        image_data: tuple,
        metadata: Optional[dict] = None,
        target_sids: Optional[list[str]] = None,
    ) -> None:
        """Deliver binary preview frames to connected browsers viewing the active workspace."""
        sids = target_sids if target_sids is not None else list(self.server.sockets.keys())
        for sock_sid in sids:
            if (
                metadata is not None
                and feature_flags.supports_feature(
                    self.server.sockets_metadata, sock_sid, "supports_preview_metadata"
                )
            ):
                await self.server.send_image_with_metadata(image_data, metadata, sid=sock_sid)
            else:
                await self.server.send_image(image_data, sid=sock_sid)

    async def _forward_ui_event_to_peer(
        self, event: Any, data: Any, workspace_user: str = "default"
    ) -> None:
        if event == "status":
            return
        if event in (
            BinaryEventTypes.UNENCODED_PREVIEW_IMAGE,
            BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA,
        ):
            from PIL import Image, ImageOps

            img_tuple = data[0] if event == BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA else data
            metadata = data[1] if (event == BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA and len(data) > 1) else None
            image_type, image, max_size = img_tuple[0], img_tuple[1], img_tuple[2]
            if max_size is not None:
                resampling = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.LANCZOS
                image = ImageOps.contain(image, (max_size, max_size), resampling)
            if isinstance(metadata, dict):
                meta_copy = dict(metadata)
                meta_copy["image_type"] = "image/png" if image_type == "PNG" else "image/jpeg"
                meta_copy.setdefault("workspace_user", workspace_user)
                meta_bytes = json.dumps(meta_copy).encode("utf-8")
                img_buf = BytesIO()
                image.save(img_buf, format=image_type, quality=90, compress_level=1)
                combined = bytearray(struct.pack(">I", len(meta_bytes)))
                combined.extend(meta_bytes)
                combined.extend(img_buf.getvalue())
                raw_frame = self.server.encode_bytes(BinaryEventTypes.PREVIEW_IMAGE_WITH_METADATA, combined)
            else:
                buf = BytesIO()
                type_num = 2 if image_type == "PNG" else 1
                buf.write(struct.pack(">I", type_num))
                image.save(buf, format=image_type, quality=90, compress_level=1)
                raw_frame = self.server.encode_bytes(BinaryEventTypes.PREVIEW_IMAGE, buf.getvalue())
            self._enqueue_outbound(("bytes", raw_frame))
        elif isinstance(data, (bytes, bytearray)) and isinstance(event, int):
            raw_frame = self.server.encode_bytes(event, data)
            self._enqueue_outbound(("bytes", raw_frame))
        elif isinstance(event, str) and event in self.BROADCAST_EVENTS:
            out_data = dict(data) if isinstance(data, dict) else data
            if isinstance(out_data, dict):
                out_data.setdefault("workspace_user", workspace_user)
            self.emit_peer_event(
                "ui_broadcast",
                {"event": event, "data": out_data, "workspace_user": workspace_user},
                journal=False,
            )

    def _enqueue_outbound(self, item: tuple[str, Any]) -> None:
        if not self.outbound_queue.full():
            self.outbound_queue.put_nowait(item)
        # If we only have an inbound_peer_ws (and no outbound _peer_connection_loop draining outbound_queue),
        # drain and send directly over inbound_peer_ws!
        if (
            (self.peer_ws is None or getattr(self.peer_ws, "closed", True))
            and self.inbound_peer_ws is not None
            and not getattr(self.inbound_peer_ws, "closed", True)
        ):
            try:
                self.loop.create_task(self._drain_outbound_to_ws(self.inbound_peer_ws))
            except RuntimeError:
                pass

    async def _drain_outbound_to_ws(self, ws: Any) -> None:
        async with self._inbound_send_lock:
            while not self.outbound_queue.empty() and ws is not None and not getattr(ws, "closed", True):
                try:
                    msg_type, payload = self.outbound_queue.get_nowait()
                    if msg_type == "json":
                        await ws.send_json(payload)
                    elif msg_type == "bytes":
                        await ws.send_bytes(payload)
                except Exception:
                    break

    def emit_peer_event(self, action: str, payload: dict, journal: bool = True) -> None:
        if not self.enabled:
            return
        self.seq_id += 1
        event_id = f"{self.node_id}:{self.seq_id}:{uuid.uuid4().hex[:8]}"
        self._seen_event_ids.add(event_id)
        if len(self._seen_event_ids) > 10000:
            self._seen_event_ids.clear()

        envelope = {
            "id": event_id,
            "seq": self.seq_id,
            "origin": self.node_id,
            "ts": int(time.time() * 1000),
            "action": action,
            "payload": payload,
        }
        if journal and self.journal_path:
            try:
                with open(self.journal_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(envelope) + "\n")
            except OSError:
                pass

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is self.loop:
            self._enqueue_outbound(("json", envelope))
        else:
            try:
                self.loop.call_soon_threadsafe(self._enqueue_outbound, ("json", envelope))
            except RuntimeError:
                pass

    def schedule_asset_scan(self, roots: tuple[str, ...]) -> None:
        async def _run():
            await asyncio.sleep(0.25)
            try:
                from app.assets.scanner import seed_assets
                await asyncio.to_thread(seed_assets, roots, False)
            except Exception as e:
                logging.debug(f"[ClusterSync] Asset scan skipped: {e}")

        try:
            if self._scan_task is None or self._scan_task.done():
                self._scan_task = self.loop.create_task(_run())
        except RuntimeError:
            pass

    def create_middleware(self):
        @web.middleware
        async def sync_middleware(request, handler):
            self.observe_request_host(request)
            is_peer_req = request.headers.get("X-Comfy-Peer-Origin") is not None

            # 1. Read-Through Proxy on /view to prevent View-Before-Sync 404s
            if request.method == "GET" and request.path in ("/view", "/api/view"):
                response = await handler(request)
                if response.status == 404 and self.enabled and not is_peer_req:
                    return await self._proxy_and_cache_view(request)
                return response

            # 2. Broadcast & Replicate /userdata and /settings mutations to all local browsers + peer node
            if (
                request.method in ("POST", "DELETE")
                and (
                    request.path.startswith("/userdata/")
                    or request.path.startswith("/api/userdata/")
                    or request.path.startswith("/settings")
                    or request.path.startswith("/api/settings")
                )
                and not is_peer_req
            ):
                body_bytes = await request.read()
                response = await handler(request)
                if response.status in (200, 201, 204):
                    # Always notify all connected local browsers even when no second server is configured
                    self.server.send_sync(
                        "userdata_updated",
                        {"path": request.path_qs, "method": request.method, "origin": self.node_id},
                        None,
                    )
                    if self.enabled:
                        # Push userdata/settings in-band over WebSocket AND via HTTP if peer_url is set
                        self.emit_peer_event(
                            "userdata_mutation",
                            {
                                "method": request.method,
                                "path_qs": request.path_qs,
                                "body_b64": base64.b64encode(body_bytes).decode("ascii"),
                                "comfy_user": request.headers.get("comfy-user", "default"),
                                "base_hash": request.headers.get("X-Comfy-Base-Hash"),
                            },
                            journal=False,
                        )
                return response

            # 3. Replicate /interrupt and /free to peer node
            if (
                request.method == "POST"
                and request.path in ("/interrupt", "/api/interrupt", "/free", "/api/free")
                and not is_peer_req
            ):
                body_bytes = await request.read()
                response = await handler(request)
                if self.enabled:
                    self.emit_peer_event(
                        "control_mutation",
                        {
                            "path": request.path,
                            "body_b64": base64.b64encode(body_bytes).decode("ascii"),
                        },
                        journal=False,
                    )
                return response

            # 4. Push uploaded /upload/image or /upload/mask directly to peer
            if (
                request.method == "POST"
                and request.path in ("/upload/image", "/api/upload/image", "/upload/mask", "/api/upload/mask")
                and not is_peer_req
            ):
                response = await handler(request)
                if response.status == 200 and response.body:
                    try:
                        resp_data = json.loads(response.body)
                        folder_paths.invalidate_filename_cache()
                        if self.enabled:
                            fname = resp_data.get("name")
                            subfolder = resp_data.get("subfolder", "")
                            dir_type = resp_data.get("type", "input")
                            if fname:
                                self.loop.create_task(
                                    self._push_assets_to_peer([(dir_type, subfolder, fname)])
                                )
                    except Exception:
                        pass
                return response

            return await handler(request)

        return sync_middleware

    async def _proxy_and_cache_view(self, request) -> Any:
        query = request.rel_url.query
        filename = query.get("filename", "")
        filename, output_dir = folder_paths.annotated_filepath(filename)
        dir_type = query.get("type", "output")
        if output_dir is None:
            output_dir = folder_paths.get_directory_by_type(dir_type)
        subfolder = query.get("subfolder", "")

        # 1. Try direct HTTP fetch if peer_url is reachable
        if self.server.client_session and self.peer_url:
            target_url = f"{self.peer_url}{request.path_qs}"
            try:
                async with self.server.client_session.get(
                    target_url,
                    headers={
                        "X-Comfy-Peer-Origin": self.node_id,
                        "X-Comfy-Sync-Secret": self.sync_secret,
                    },
                    timeout=aiohttp.ClientTimeout(total=10.0),
                ) as peer_resp:
                    if peer_resp.status == 200:
                        data = await peer_resp.read()
                        if "preview" not in query and query.get("channel", "rgba") == "rgba" and output_dir and filename:
                            self._save_fetched_asset(output_dir, subfolder, filename, data, dir_type)
                        return web.Response(
                            body=data,
                            status=200,
                            content_type=peer_resp.content_type,
                            headers={"Content-Disposition": peer_resp.headers.get("Content-Disposition", "")},
                        )
            except Exception as e:
                logging.debug(f"[ClusterSync] HTTP proxy fallback to WS RPC for {filename}: {e}")

        # 2. Fallback: In-band WebSocket RPC fetch (works even when peer has no public URL / behind NAT)
        if self.has_active_peer_ws and filename:
            req_id = uuid.uuid4().hex
            fut = self.loop.create_future()
            self._pending_asset_fetches[req_id] = fut
            try:
                self.emit_peer_event(
                    "asset_fetch_req",
                    {
                        "req_id": req_id,
                        "filename": os.path.basename(filename),
                        "subfolder": subfolder,
                        "type": dir_type,
                    },
                    journal=False,
                )
                result = await asyncio.wait_for(fut, timeout=8.0)
                if result and result.get("ok") and result.get("data_b64"):
                    raw_bytes = base64.b64decode(result["data_b64"])
                    if output_dir:
                        self._save_fetched_asset(output_dir, subfolder, filename, raw_bytes, dir_type)
                    return web.Response(
                        body=raw_bytes,
                        status=200,
                        content_type=result.get("content_type", "image/png"),
                        headers={"Content-Disposition": f'filename="{os.path.basename(filename)}"'},
                    )
            except Exception as e:
                logging.debug(f"[ClusterSync] WS asset fetch failed for {filename}: {e}")
            finally:
                self._pending_asset_fetches.pop(req_id, None)

        return web.Response(status=404)

    def _save_fetched_asset(
        self, output_dir: str, subfolder: str, filename: str, data: bytes, dir_type: str
    ) -> None:
        full_dir = os.path.abspath(os.path.join(output_dir, subfolder))
        if os.path.commonpath((output_dir, full_dir)) == output_dir:
            os.makedirs(full_dir, exist_ok=True)
            dest_path = os.path.join(full_dir, os.path.basename(filename))
            atomic_write_bytes(dest_path, data)
            folder_paths.invalidate_filename_cache()
            if dir_type in ("input", "output"):
                self.schedule_asset_scan((dir_type,))

    async def _handle_peer_ws(self, request) -> Any:
        secret = request.headers.get("X-Comfy-Sync-Secret") or request.rel_url.query.get("secret")
        if secret and secret != self.sync_secret:
            return web.Response(status=403, text="Invalid sync secret")
        ws = web.WebSocketResponse(heartbeat=15.0, max_msg_size=64 * 1024 * 1024)
        await ws.prepare(request)
        self.inbound_peer_ws = ws
        self.peer_connected = True
        logging.info("[ClusterSync] Peer node connected to inbound sync WebSocket")

        # Send full state snapshot back over this same inbound WebSocket so one-way tunnels work full-duplex!
        await ws.send_json(self.build_hello_snapshot())

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self.process_peer_json(json.loads(msg.data))
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await self.process_peer_bytes(msg.data)
        finally:
            if self.inbound_peer_ws is ws:
                self.inbound_peer_ws = None
            self.peer_connected = self.has_active_peer_ws
        return ws

    def build_hello_snapshot(self) -> dict[str, Any]:
        q: DistributedPromptQueue = self.server.prompt_queue
        return {
            "action": "hello_sync",
            "origin": self.node_id,
            "seq": self.seq_id,
            "payload": {
                "lamport_clock": q.lamport_clock,
                "self_tunnel_url": self.self_tunnel_url,
                "workspace_state": {
                    "workflow": self.workspace_state.get("workflow"),
                    "rev": self.workspace_state.get("rev", 0),
                    "ts": self.workspace_state.get("ts", 0),
                    "node_outputs": self.workspace_state.get("node_outputs", {}),
                },
                "history": q.get_history(max_items=200),
                "running": [
                    (x[0], x[1], x[2], x[3], x[4], {})
                    for x in q.currently_running.values()
                ],
                "queued": [
                    (x[0], x[1], x[2], x[3], x[4], {})
                    for x in q.queue
                ],
            },
        }

    async def _peer_connection_loop(self) -> None:
        while self.peer_url:
            ws_url = (
                self.peer_url.replace("http://", "ws://").replace("https://", "wss://")
                + "/cluster/sync/ws"
            )
            try:
                if self.server.client_session is None:
                    await asyncio.sleep(0.5)
                    continue
                async with self.server.client_session.ws_connect(
                    ws_url,
                    headers={
                        "X-Comfy-Sync-Secret": self.sync_secret,
                        "X-Comfy-Peer-Origin": self.node_id,
                    },
                    heartbeat=15.0,
                    max_msg_size=64 * 1024 * 1024,
                ) as ws:
                    self.peer_ws = ws
                    self.peer_connected = True
                    logging.info(f"[ClusterSync] Connected to peer {ws_url}")
                    await ws.send_json(self.build_hello_snapshot())

                    # Run concurrent reader & writer on outbound ws so full-duplex works in both directions
                    reader_task = self.loop.create_task(self._read_from_peer_ws(ws))
                    writer_task = self.loop.create_task(self._write_to_peer_ws(ws))
                    done, pending = await asyncio.wait(
                        [reader_task, writer_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.debug(f"[ClusterSync] Peer reconnect pending ({ws_url}): {e}")
                await asyncio.sleep(2.0)
            finally:
                self.peer_ws = None
                self.peer_connected = self.has_active_peer_ws

    async def _read_from_peer_ws(self, ws: Any) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self.process_peer_json(json.loads(msg.data))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self.process_peer_bytes(msg.data)

    async def _write_to_peer_ws(self, ws: Any) -> None:
        while not ws.closed:
            msg_type, payload = await self.outbound_queue.get()
            if msg_type == "json":
                await ws.send_json(payload)
            elif msg_type == "bytes":
                await ws.send_bytes(payload)

    async def process_peer_json(self, envelope: dict[str, Any]) -> None:
        event_id = envelope.get("id")
        if event_id:
            if event_id in self._seen_event_ids:
                return
            self._seen_event_ids.add(event_id)

        action = envelope.get("action")
        payload = envelope.get("payload", {})
        seq = envelope.get("seq", 0)
        if seq:
            self.peer_last_seq = max(self.peer_last_seq, int(seq))

        q: DistributedPromptQueue = self.server.prompt_queue

        if action == "hello_sync":
            peer_tunnel = payload.get("self_tunnel_url")
            if peer_tunnel and not self.peer_url:
                self.peer_url = peer_tunnel.rstrip("/")

            ws_state = payload.get("workspace_state")
            if isinstance(ws_state, dict) and ws_state.get("workflow"):
                if ws_state.get("ts", 0) > self.workspace_state.get("ts", 0):
                    self.workspace_state["node_outputs"].update(ws_state.get("node_outputs", {}))
                    self.update_workspace_graph(
                        ws_state["workflow"],
                        sender_sid=None,
                        ts=ws_state.get("ts"),
                        from_peer=True,
                    )

            with q.mutex:
                q.observe_lamport(payload.get("lamport_clock", 0))
                for pid, hentry in payload.get("history", {}).items():
                    if pid not in q.history:
                        entry_copy = copy.deepcopy(hentry)
                        if isinstance(entry_copy.get("prompt"), list):
                            entry_copy["prompt"] = tuple(entry_copy["prompt"])
                        q.history[pid] = entry_copy
                q.remote_running = {
                    item[1]: tuple(item) for item in payload.get("running", [])
                }
                q.remote_queued = {
                    item[1]: tuple(item)
                    for item in payload.get("queued", [])
                    if item[1] not in q.history
                }
                self.server.queue_updated()

        elif action == "workspace_sync":
            wf = payload.get("workflow")
            ws_user = (payload.get("workspace_user") or "default").strip() or "default"
            if isinstance(wf, dict):
                self.update_workspace_graph(
                    wf, sender_sid=None, ts=payload.get("ts"), from_peer=True, workspace_user=ws_user
                )

        elif action == "user_presence":
            uname = (payload.get("username") or "").strip()
            if uname:
                ws_state = self.get_or_create_workspace(uname)
                ws_state["last_active"] = payload.get("ts") or int(time.time() * 1000)
                await self._broadcast_users_list()

        elif action == "cursor_sync":
            ws_user = (payload.get("workspace_user") or "default").strip() or "default"
            target_sids = self.get_target_sids_for_workspace(ws_user)
            if target_sids:
                out_msg = {"type": "sync_cursor", "data": payload}
                for t_sid in target_sids:
                    ws = self.server.sockets.get(t_sid)
                    if ws is not None:
                        try:
                            await ws.send_json(out_msg)
                        except Exception:
                            pass

        elif action == "node_outputs_sync":
            outputs = payload.get("node_outputs")
            ws_user = (payload.get("workspace_user") or "default").strip() or "default"
            if isinstance(outputs, dict):
                ws_state = self.get_or_create_workspace(ws_user)
                ws_state["node_outputs"].update(outputs)
                self._save_cached_workspace_state()
                await self._broadcast_json_to_workspace(
                    "sync_node_outputs",
                    {"node_outputs": outputs, "workspace_user": ws_user},
                    workspace_user=ws_user,
                )

        elif action == "queue_put":
            item = tuple(payload["item"])
            q.put(item, _from_peer=True)

        elif action == "queue_running":
            ws_user = (payload.get("workspace_user") or "default").strip() or "default"
            self.active_execution_user = ws_user
            self.prompt_user_map[payload["prompt_id"]] = ws_user
            q.apply_peer_running(payload["prompt_id"], tuple(payload["item"]))
            self.schedule_broadcast_users_list()

        elif action == "task_done":
            hentry = payload.get("history_entry") or {}
            ws_user = (payload.get("workspace_user") or self.prompt_user_map.get(payload["prompt_id"]) or "default").strip() or "default"
            ws_state = self.get_or_create_workspace(ws_user)
            ws_state["executing_node"] = None
            ws_state["executing_prompt_id"] = None
            ws_state["progress"] = None
            outputs = hentry.get("outputs")
            if isinstance(outputs, dict) and outputs:
                for node_id, node_out in outputs.items():
                    if isinstance(node_out, dict):
                        ws_state["node_outputs"][str(node_id)] = node_out
                self._save_cached_workspace_state()
            q.apply_peer_task_done(payload["prompt_id"], hentry)
            self.schedule_asset_scan(("output",))
            self.schedule_broadcast_users_list()
            await self._send_to_local_clients(
                "history_updated",
                {
                    "prompt_id": payload["prompt_id"],
                    "workspace_user": ws_user,
                    "outputs": outputs,
                    "status": hentry.get("status"),
                },
                sid=None,
            )

        elif action == "queue_wipe":
            q.wipe_queue(_from_peer=True)

        elif action == "queue_delete":
            target_id = payload["prompt_id"]
            q.delete_queue_item(lambda a: a[1] == target_id, _from_peer_id=target_id)

        elif action == "history_wipe":
            q.wipe_history(_from_peer=True)

        elif action == "history_delete":
            q.delete_history_item(payload["prompt_id"], _from_peer=True)

        elif action == "asset_push":
            dir_type = payload.get("type", "output")
            subfolder = payload.get("subfolder", "")
            fname = payload.get("filename", "")
            data_b64 = payload.get("data_b64", "")
            base_dir = folder_paths.get_directory_by_type(dir_type)
            if base_dir and fname and data_b64:
                raw_bytes = base64.b64decode(data_b64)
                self._save_fetched_asset(base_dir, subfolder, fname, raw_bytes, dir_type)

        elif action == "asset_fetch_req":
            req_id = payload.get("req_id")
            fname = payload.get("filename", "")
            subfolder = payload.get("subfolder", "")
            dir_type = payload.get("type", "output")
            base_dir = folder_paths.get_directory_by_type(dir_type)
            if req_id and base_dir and fname:
                full_path = os.path.join(base_dir, subfolder, os.path.basename(fname))
                if os.path.isfile(full_path):
                    raw_bytes = await asyncio.to_thread(self._read_file_bytes, full_path)
                    import mimetypes
                    ctype = mimetypes.guess_type(fname)[0] or "image/png"
                    self.emit_peer_event(
                        "asset_fetch_resp",
                        {
                            "req_id": req_id,
                            "ok": True,
                            "content_type": ctype,
                            "data_b64": base64.b64encode(raw_bytes).decode("ascii"),
                        },
                        journal=False,
                    )
                else:
                    self.emit_peer_event("asset_fetch_resp", {"req_id": req_id, "ok": False}, journal=False)

        elif action == "asset_fetch_resp":
            req_id = payload.get("req_id")
            fut = self._pending_asset_fetches.get(req_id)
            if fut and not fut.done():
                fut.set_result(payload)

        elif action == "userdata_mutation":
            await self._apply_peer_userdata_mutation(payload, origin=envelope.get("origin", "peer"))

        elif action == "control_mutation":
            await self._apply_peer_control_mutation(payload)

        elif action == "asset_uploaded":
            name = payload.get("name")
            subfolder = payload.get("subfolder", "")
            img_type = payload.get("type", "input")
            if name and self.server.client_session and self.peer_url:
                qs = urllib.parse.urlencode({"filename": name, "subfolder": subfolder, "type": img_type})
                self.loop.create_task(self._prefetch_asset(qs, img_type, subfolder, name))

        elif action == "ui_broadcast":
            event = payload["event"]
            data = payload["data"]
            await self._send_to_local_clients(event, data, sid=None)

    async def _apply_peer_userdata_mutation(self, payload: dict[str, Any], origin: str) -> None:
        method = payload.get("method", "POST")
        path_qs = payload.get("path_qs", "")
        body_bytes = base64.b64decode(payload.get("body_b64", ""))
        user_id = payload.get("comfy_user", "default")
        base_hash = payload.get("base_hash")

        parsed = urllib.parse.urlsplit(path_qs)
        path = parsed.path
        if path.startswith("/api/"):
            path = path[4:]

        user_root = folder_paths.get_public_user_directory(user_id)
        if not user_root:
            return

        if path.startswith("/userdata/"):
            rel_file = urllib.parse.unquote(path[len("/userdata/"):])
            if "/move/" in rel_file:
                src_rel, dst_rel = rel_file.split("/move/", 1)
                src_abs = os.path.abspath(os.path.join(user_root, urllib.parse.unquote(src_rel)))
                dst_abs = os.path.abspath(os.path.join(user_root, urllib.parse.unquote(dst_rel)))
                if (
                    os.path.commonpath((user_root, src_abs)) == user_root
                    and os.path.commonpath((user_root, dst_abs)) == user_root
                    and os.path.isfile(src_abs)
                ):
                    os.makedirs(os.path.dirname(dst_abs), exist_ok=True)
                    os.replace(src_abs, dst_abs)
            else:
                target_abs = os.path.abspath(os.path.join(user_root, rel_file))
                if os.path.commonpath((user_root, target_abs)) != user_root:
                    return
                if method == "DELETE":
                    if os.path.isfile(target_abs):
                        try:
                            os.remove(target_abs)
                        except OSError:
                            pass
                else:
                    write_userdata_with_conflict_detection(
                        target_abs, body_bytes, base_hash=base_hash, origin_node=origin
                    )
        elif path.startswith("/settings"):
            settings_path = os.path.join(user_root, "comfy.settings.json")
            current = {}
            if os.path.isfile(settings_path):
                try:
                    with open(settings_path, "r", encoding="utf-8") as f:
                        current = json.load(f)
                except Exception:
                    current = {}
            try:
                incoming_val = json.loads(body_bytes.decode("utf-8"))
                if path == "/settings" and isinstance(incoming_val, dict):
                    merged = merge_settings_lww(current, incoming_val)
                else:
                    setting_id = urllib.parse.unquote(path[len("/settings/"):])
                    merged = merge_settings_lww(current, {setting_id: incoming_val})
                atomic_write_bytes(settings_path, json.dumps(merged, indent=4).encode("utf-8"))
            except Exception as e:
                logging.debug(f"[ClusterSync] Settings apply failed: {e}")

        await self._send_to_local_clients(
            "userdata_updated",
            {"path": path_qs, "method": method, "origin": origin},
            sid=None,
        )

    async def _apply_peer_control_mutation(self, payload: dict[str, Any]) -> None:
        path = payload.get("path", "")
        if path.startswith("/api/"):
            path = path[4:]
        body_bytes = base64.b64decode(payload.get("body_b64", ""))
        try:
            json_data = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        except Exception:
            json_data = {}

        if path == "/interrupt":
            try:
                import nodes
                nodes.interrupt_processing()
            except Exception:
                pass
        elif path == "/free":
            if json_data.get("unload_models"):
                self.server.prompt_queue.set_flag("unload_models", True)
            if json_data.get("free_memory"):
                self.server.prompt_queue.set_flag("free_memory", True)

    async def _prefetch_asset(self, qs: str, img_type: str, subfolder: str, name: str) -> None:
        try:
            async with self.server.client_session.get(
                f"{self.peer_url}/view?{qs}",
                headers={
                    "X-Comfy-Peer-Origin": self.node_id,
                    "X-Comfy-Sync-Secret": self.sync_secret,
                },
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    base_dir = folder_paths.get_directory_by_type(img_type)
                    if base_dir:
                        self._save_fetched_asset(base_dir, subfolder, name, data, img_type)
        except Exception as e:
            logging.debug(f"[ClusterSync] Prefetch failed for {name}: {e}")

    async def process_peer_bytes(self, raw_frame: bytes) -> None:
        target_sids = self.get_target_sids_for_workspace(self.active_execution_user)
        sockets = (
            [self.server.sockets[s] for s in target_sids if s in self.server.sockets]
            if target_sids is not None
            else list(self.server.sockets.values())
        )
        for ws in sockets:
            try:
                await ws.send_bytes(raw_frame)
            except Exception as err:
                logging.warning(f"send error: {err}")
