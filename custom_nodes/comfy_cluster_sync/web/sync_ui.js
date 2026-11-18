import { api } from "../../scripts/api.js";
import { app } from "../../scripts/app.js";

if (!window.__COMFY_CLUSTER_SYNC_INITIALIZED__) {
    window.__COMFY_CLUSTER_SYNC_INITIALIZED__ = true;

    let myUsername = sessionStorage.getItem("comfy_sync_username") || "";
    let viewingUser = myUsername || "";
    let collaborateOnOtherTab = false;

    let isApplyingRemote = false;
    let lastSyncedFingerprint = "";
    let lastAppliedTs = 0;
    let pushDebounceTimer = null;

    // Per-user local caches so switching tabs back and forth is instant and lossless
    const userWorkflowCache = {};
    const userNodeOutputsCache = {};

    let knownUsers = [];
    let latestStatus = {
        node_id: "node_a",
        peer_url: "",
        peer_connected: false,
        self_tunnel_url: null,
        connected_browsers: 1,
    };

    let userTabsBarEl = null;
    let spectatorBannerEl = null;
    let statusBadgeEl = null;
    let statusPopoverEl = null;
    let usernameModalEl = null;
    const remoteCursors = new Map();
    let lastCursorSentAt = 0;

    function isEditingAllowed() {
        if (!myUsername) return false;
        return viewingUser === myUsername || collaborateOnOtherTab;
    }

    function getActiveOutputsMap(user = viewingUser) {
        const key = user || myUsername || "default";
        if (!userNodeOutputsCache[key]) {
            userNodeOutputsCache[key] = {};
        }
        return userNodeOutputsCache[key];
    }

    function stripViewportFromGraph(rawGraph) {
        if (!rawGraph || typeof rawGraph !== "object") return null;
        const clone = {};
        for (const k of Object.keys(rawGraph)) {
            if (k === "extra" && rawGraph.extra && typeof rawGraph.extra === "object") {
                const extraCopy = { ...rawGraph.extra };
                delete extraCopy.ds;
                clone.extra = extraCopy;
            } else {
                clone[k] = rawGraph[k];
            }
        }
        return clone;
    }

    function computeWorkflowFingerprint(rawGraph) {
        try {
            const stripped = stripViewportFromGraph(rawGraph);
            if (!stripped) return "";
            return JSON.stringify(stripped);
        } catch {
            return "";
        }
    }

    function canPatchGraphInPlace(graph, incomingWf) {
        if (!graph || !Array.isArray(graph._nodes) || !incomingWf || !Array.isArray(incomingWf.nodes)) {
            return false;
        }
        if (graph._nodes.length === 0 || graph._nodes.length !== incomingWf.nodes.length) {
            return false;
        }
        const currentLinks = Array.isArray(graph.links)
            ? graph.links.filter(Boolean)
            : Object.values(graph.links || {}).filter(Boolean);
        const incomingLinks = Array.isArray(incomingWf.links) ? incomingWf.links : [];
        if (currentLinks.length !== incomingLinks.length) {
            return false;
        }

        const nodeMap = new Map();
        for (const n of graph._nodes) {
            nodeMap.set(String(n.id), n);
        }
        for (const incNode of incomingWf.nodes) {
            const existing = nodeMap.get(String(incNode.id));
            if (!existing || existing.type !== incNode.type) {
                return false;
            }
        }

        const linkSig = (l) =>
            Array.isArray(l)
                ? `${l[0]}:${l[1]}:${l[2]}:${l[3]}:${l[4]}`
                : `${l.id}:${l.origin_id}:${l.origin_slot}:${l.target_id}:${l.target_slot}`;
        const curLinkSet = new Set(currentLinks.map(linkSig));
        for (const incLink of incomingLinks) {
            if (!curLinkSet.has(linkSig(incLink))) {
                return false;
            }
        }
        return true;
    }

    function patchGraphInPlace(graph, incomingWf) {
        for (const incNode of incomingWf.nodes) {
            const node = graph.getNodeById(incNode.id);
            if (!node) continue;

            if (incNode.pos) {
                const px = Array.isArray(incNode.pos) ? incNode.pos[0] : incNode.pos[0] ?? incNode.pos.x;
                const py = Array.isArray(incNode.pos) ? incNode.pos[1] : incNode.pos[1] ?? incNode.pos.y;
                if (typeof px === "number" && typeof py === "number" && node.pos) {
                    node.pos[0] = px;
                    node.pos[1] = py;
                }
            }
            if (incNode.size) {
                const sx = Array.isArray(incNode.size) ? incNode.size[0] : incNode.size[0] ?? incNode.size.width;
                const sy = Array.isArray(incNode.size) ? incNode.size[1] : incNode.size[1] ?? incNode.size.height;
                if (typeof sx === "number" && typeof sy === "number" && node.size) {
                    node.size[0] = sx;
                    node.size[1] = sy;
                }
            }
            if (typeof incNode.mode === "number") {
                node.mode = incNode.mode;
            }
            if (typeof incNode.title === "string") {
                node.title = incNode.title;
            }
            if ("color" in incNode) node.color = incNode.color;
            if ("bgcolor" in incNode) node.bgcolor = incNode.bgcolor;
            if (incNode.flags && typeof incNode.flags === "object") {
                node.flags = { ...(node.flags || {}), ...incNode.flags };
            }
            if (incNode.properties && typeof incNode.properties === "object") {
                node.properties = { ...(node.properties || {}), ...incNode.properties };
            }

            if (Array.isArray(incNode.widgets_values) && Array.isArray(node.widgets)) {
                let valIdx = 0;
                for (let wIdx = 0; wIdx < node.widgets.length; wIdx++) {
                    const w = node.widgets[wIdx];
                    if (!w || w.serialize === false) continue;
                    if (valIdx >= incNode.widgets_values.length) break;
                    const newVal = incNode.widgets_values[valIdx++];
                    if (w.value !== newVal) {
                        w.value = newVal;
                        const el = w.inputEl || w.element;
                        if (el && document.activeElement !== el && "value" in el) {
                            el.value = newVal ?? "";
                        }
                    }
                }
            }
        }
        graph.setDirtyCanvas(true, true);
    }

    function hookNodeWidgets(node) {
        if (!node || !Array.isArray(node.widgets)) return;
        for (const w of node.widgets) {
            if (!w || w.__clusterSyncHooked) continue;
            w.__clusterSyncHooked = true;
            const origCallback = w.callback;
            w.callback = function (...args) {
                const res = origCallback ? origCallback.apply(this, args) : undefined;
                if (!isApplyingRemote && isEditingAllowed()) {
                    schedulePushWorkspace(60);
                }
                return res;
            };
            const el = w.inputEl || w.element;
            if (el && !el.__clusterSyncInputHooked) {
                el.__clusterSyncInputHooked = true;
                el.addEventListener("input", () => {
                    if (!isApplyingRemote && isEditingAllowed()) {
                        w.value = el.value;
                        schedulePushWorkspace(90);
                    }
                });
            }
        }
    }

    function hookAllGraphNodes(graph) {
        if (!graph || !Array.isArray(graph._nodes)) return;
        for (const node of graph._nodes) {
            hookNodeWidgets(node);
        }
    }

    async function applyRemoteWorkflow(workflow, ts = 0, forceReconfigure = false) {
        if (!workflow || typeof workflow !== "object" || !Array.isArray(workflow.nodes)) {
            return;
        }
        const incomingFp = computeWorkflowFingerprint(workflow);
        if (!forceReconfigure && (!incomingFp || incomingFp === lastSyncedFingerprint)) {
            if (ts) lastAppliedTs = Math.max(lastAppliedTs, ts);
            return;
        }
        if (!app.graph) return;

        isApplyingRemote = true;
        try {
            if (ts) lastAppliedTs = Math.max(lastAppliedTs, ts);
            if (viewingUser) {
                userWorkflowCache[viewingUser] = JSON.parse(JSON.stringify(workflow));
            }

            if (!forceReconfigure && canPatchGraphInPlace(app.graph, workflow)) {
                patchGraphInPlace(app.graph, workflow);
            } else {
                const hadNodes = Array.isArray(app.graph._nodes) && app.graph._nodes.length > 0;
                const savedScale = app.canvas?.ds?.scale;
                const savedOffset = app.canvas?.ds?.offset
                    ? [app.canvas.ds.offset[0], app.canvas.ds.offset[1]]
                    : null;

                const wfClone = JSON.parse(JSON.stringify(workflow));
                app.graph.configure(wfClone);

                if (!forceReconfigure && hadNodes && savedScale && savedOffset && app.canvas?.ds) {
                    app.canvas.ds.scale = savedScale;
                    app.canvas.ds.offset[0] = savedOffset[0];
                    app.canvas.ds.offset[1] = savedOffset[1];
                }
                hookAllGraphNodes(app.graph);
                applyNodeOutputsMap(getActiveOutputsMap(viewingUser), false);
                app.graph.setDirtyCanvas(true, true);
            }

            try {
                lastSyncedFingerprint = computeWorkflowFingerprint(app.graph.serialize());
            } catch {
                lastSyncedFingerprint = incomingFp;
            }
        } catch (err) {
            console.warn("[ClusterSync] Failed applying remote workflow:", err);
        } finally {
            setTimeout(() => {
                isApplyingRemote = false;
            }, 50);
        }
    }

    function clearCanvasPreviews() {
        if (app.nodeOutputs) {
            for (const k of Object.keys(app.nodeOutputs)) {
                delete app.nodeOutputs[k];
            }
        }
        if (app.graph && Array.isArray(app.graph._nodes)) {
            for (const n of app.graph._nodes) {
                if (n && n.imgs) {
                    delete n.imgs;
                }
            }
        }
    }

    function renderNodeOutputPreview(nodeId, output, targetUser = viewingUser) {
        if (!output || typeof output !== "object") return;
        const key = String(nodeId);
        const userMap = getActiveOutputsMap(targetUser);
        userMap[key] = output;

        if (targetUser && targetUser !== viewingUser) {
            return;
        }

        if (!app.nodeOutputs) app.nodeOutputs = {};
        app.nodeOutputs[key] = output;

        const node = app.graph?.getNodeById?.(nodeId) || app.graph?.getNodeById?.(Number(nodeId));
        if (node) {
            try {
                node.onExecuted?.(output);
            } catch {}

            const mediaItems = output.images || output.gifs;
            if (Array.isArray(mediaItems) && mediaItems.length > 0) {
                const loadedImgs = [];
                let remaining = mediaItems.length;
                for (const item of mediaItems) {
                    if (!item || !item.filename) {
                        remaining--;
                        continue;
                    }
                    const params = new URLSearchParams({
                        filename: item.filename,
                        type: item.type || "output",
                        subfolder: item.subfolder || "",
                    });
                    const img = new Image();
                    img.onload = () => {
                        remaining--;
                        if (remaining <= 0) {
                            node.imgs = loadedImgs;
                            node.imageIndex = 0;
                            try {
                                node.setSizeForImage?.();
                            } catch {}
                            app.graph?.setDirtyCanvas?.(true, true);
                        }
                    };
                    img.onerror = () => {
                        remaining--;
                    };
                    img.src = api.apiURL(`/view?${params.toString()}`);
                    loadedImgs.push(img);
                }
                node.imgs = loadedImgs;
            }
        }
    }

    function applyNodeOutputsMap(outputsMap, dispatchToStores = true) {
        if (!outputsMap || typeof outputsMap !== "object") return;
        for (const [nodeId, output] of Object.entries(outputsMap)) {
            if (!output || typeof output !== "object") continue;
            renderNodeOutputPreview(nodeId, output, viewingUser);
            if (dispatchToStores) {
                try {
                    api.dispatchEvent(
                        new CustomEvent("executed", {
                            detail: {
                                node: String(nodeId),
                                display_node: String(nodeId),
                                output,
                                prompt_id: "cluster-sync",
                                _from_cluster_sync: true,
                            },
                        })
                    );
                } catch {}
            }
        }
        app.graph?.setDirtyCanvas?.(true, true);
    }

    function pushWorkspaceNow(targetWorkspaceUser = null) {
        if (isApplyingRemote || !app.graph || !myUsername) return;
        const targetUser = targetWorkspaceUser || viewingUser || myUsername;
        if (targetUser !== myUsername && !collaborateOnOtherTab) return;

        try {
            const serialized = app.graph.serialize();
            if (!serialized || !Array.isArray(serialized.nodes)) return;
            userWorkflowCache[targetUser] = JSON.parse(JSON.stringify(serialized));

            const fp = computeWorkflowFingerprint(serialized);
            if (!targetWorkspaceUser && (!fp || fp === lastSyncedFingerprint)) return;

            lastSyncedFingerprint = fp;
            const ts = Date.now();
            lastAppliedTs = Math.max(lastAppliedTs, ts);

            const payload = {
                workspace_user: targetUser,
                workflow: serialized,
                ts,
                sender: api.clientId,
            };

            if (api.socket && api.socket.readyState === WebSocket.OPEN) {
                api.socket.send(
                    JSON.stringify({
                        type: "sync_workspace",
                        data: payload,
                    })
                );
            } else {
                fetch(api.apiURL("/cluster/sync/workspace"), {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                }).catch(() => {});
            }
        } catch (err) {
            console.debug("[ClusterSync] Workspace push skipped:", err);
        }
    }

    function schedulePushWorkspace(delayMs = 90) {
        if (isApplyingRemote || !isEditingAllowed()) return;
        if (pushDebounceTimer) clearTimeout(pushDebounceTimer);
        pushDebounceTimer = setTimeout(() => {
            pushDebounceTimer = null;
            pushWorkspaceNow();
        }, delayMs);
    }

    function attachGraphHooks() {
        if (!app.graph || app.graph.__clusterSyncGraphHooked) return;
        app.graph.__clusterSyncGraphHooked = true;

        const origNodeAdded = app.graph.onNodeAdded;
        app.graph.onNodeAdded = function (node) {
            const r = origNodeAdded ? origNodeAdded.apply(this, arguments) : undefined;
            hookNodeWidgets(node);
            if (!isApplyingRemote && isEditingAllowed()) schedulePushWorkspace(60);
            return r;
        };

        const origNodeRemoved = app.graph.onNodeRemoved;
        app.graph.onNodeRemoved = function () {
            const r = origNodeRemoved ? origNodeRemoved.apply(this, arguments) : undefined;
            if (!isApplyingRemote && isEditingAllowed()) schedulePushWorkspace(60);
            return r;
        };

        const origConnChange = app.graph.onConnectionChange;
        app.graph.onConnectionChange = function () {
            const r = origConnChange ? origConnChange.apply(this, arguments) : undefined;
            if (!isApplyingRemote && isEditingAllowed()) schedulePushWorkspace(60);
            return r;
        };

        hookAllGraphNodes(app.graph);
    }

    function sendIdentifyToServer(includeCurrentGraph = false) {
        if (!myUsername) return;
        if (!viewingUser) viewingUser = myUsername;

        let initWf = null;
        if (includeCurrentGraph && app.graph && viewingUser === myUsername) {
            try {
                initWf = app.graph.serialize();
                userWorkflowCache[myUsername] = JSON.parse(JSON.stringify(initWf));
            } catch {}
        }

        if (api.socket && api.socket.readyState === WebSocket.OPEN) {
            api.socket.send(
                JSON.stringify({
                    type: "sync_identify",
                    data: {
                        username: myUsername,
                        viewing_user: viewingUser,
                        initial_workflow: initWf,
                    },
                })
            );
        }
    }

    async function switchViewingTab(targetUsername) {
        if (!targetUsername || targetUsername === viewingUser) return;

        // 1. Save our own current tab state before leaving so nothing is ever lost
        if (viewingUser === myUsername && app.graph) {
            try {
                const snap = app.graph.serialize();
                userWorkflowCache[myUsername] = JSON.parse(JSON.stringify(snap));
                pushWorkspaceNow(myUsername);
            } catch {}
        }

        // Clear remote cursors from previous tab
        for (const el of remoteCursors.values()) {
            el.remove();
        }
        remoteCursors.clear();

        viewingUser = targetUsername;
        collaborateOnOtherTab = false;
        lastAppliedTs = 0;
        lastSyncedFingerprint = "";
        clearCanvasPreviews();

        renderUserTabsBar();
        renderSpectatorBanner();

        // 2. If switching back to our own tab and we have a cached workflow, apply immediately for instant response
        if (viewingUser === myUsername && userWorkflowCache[myUsername]) {
            await applyRemoteWorkflow(userWorkflowCache[myUsername], Date.now(), true);
            applyNodeOutputsMap(getActiveOutputsMap(myUsername), false);
        }

        // 3. Tell server we switched tabs so it streams `viewingUser`'s live workspace & execution events to us
        if (api.socket && api.socket.readyState === WebSocket.OPEN) {
            api.socket.send(
                JSON.stringify({
                    type: "sync_switch_view",
                    data: {
                        username: myUsername,
                        viewing_user: viewingUser,
                    },
                })
            );
        }

        // HTTP fallback to fetch target user's workspace state immediately
        try {
            const resp = await fetch(
                api.apiURL(`/cluster/sync/workspace?user=${encodeURIComponent(viewingUser)}`)
            );
            if (resp.ok) {
                const stateData = await resp.json();
                await handleFullStatePayload(stateData, true);
            }
        } catch {}
    }

    function showUsernameModal(forceRename = false) {
        if (usernameModalEl) {
            usernameModalEl.remove();
            usernameModalEl = null;
        }

        usernameModalEl = document.createElement("div");
        usernameModalEl.id = "comfy-sync-username-modal";
        Object.assign(usernameModalEl.style, {
            position: "fixed",
            inset: "0",
            zIndex: "20000",
            background: "rgba(9, 13, 22, 0.78)",
            backdropFilter: "blur(8px)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontFamily: "system-ui, -apple-system, sans-serif",
        });

        const suggested = myUsername || localStorage.getItem("comfy_sync_last_username") || "";
        const activeOtherUsers = (knownUsers || []).filter((u) => u.username && u.username !== "default");

        const existingChipsHtml =
            activeOtherUsers.length > 0
                ? `
                <div style="margin-top:14px;font-size:12px;color:#9ca3af;">
                    Or select an active user workspace:
                    <div id="comfy-existing-user-chips" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:6px;">
                        ${activeOtherUsers
                            .map(
                                (u) => `
                            <button type="button" data-uname="${u.username.replace(/"/g, "&quot;")}"
                                style="padding:4px 10px;border-radius:999px;border:1px solid rgba(56,189,248,0.4);background:rgba(30,41,59,0.9);color:#e0f2fe;font-size:12px;cursor:pointer;">
                                ${u.online ? "🟢" : "⚪"} ${u.username}
                            </button>
                        `
                            )
                            .join("")}
                    </div>
                </div>
            `
                : "";

        usernameModalEl.innerHTML = `
            <div style="width:400px;max-width:92vw;padding:24px;border-radius:16px;background:#111827;border:1px solid rgba(255,255,255,0.16);box-shadow:0 20px 50px rgba(0,0,0,0.65);color:#f9fafb;">
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
                    <span style="font-size:22px;">👤</span>
                    <h2 style="margin:0;font-size:18px;font-weight:700;color:#fff;">
                        ${forceRename ? "Change Your Username" : "Welcome to ComfyUI Live Workspace"}
                    </h2>
                </div>
                <p style="margin:0 0 16px 0;font-size:13px;color:#9ca3af;line-height:1.45;">
                    Enter your name to work independently in your own tab. You can switch to any other user's tab at the top of the screen to watch what they are doing in real time.
                </p>
                <form id="comfy-sync-username-form" style="display:flex;gap:8px;">
                    <input id="comfy-sync-username-input" type="text" maxlength="32" required
                        placeholder="e.g. Alice, Bob, Designer-1..."
                        value="${suggested.replace(/"/g, "&quot;")}"
                        style="flex:1;padding:10px 12px;border-radius:8px;border:1px solid #374151;background:#1f2937;color:#fff;font-size:14px;outline:none;" />
                    <button type="submit"
                        style="padding:10px 18px;border-radius:8px;border:none;background:linear-gradient(135deg,#2563eb,#1d4ed8);color:#fff;font-weight:600;font-size:14px;cursor:pointer;white-space:nowrap;">
                        Start
                    </button>
                </form>
                ${existingChipsHtml}
            </div>
        `;

        document.body.appendChild(usernameModalEl);

        const inputEl = usernameModalEl.querySelector("#comfy-sync-username-input");
        const formEl = usernameModalEl.querySelector("#comfy-sync-username-form");

        const commitUsername = (chosenName) => {
            const cleaned = (chosenName || "").trim();
            if (!cleaned) return;
            myUsername = cleaned;
            viewingUser = cleaned;
            sessionStorage.setItem("comfy_sync_username", cleaned);
            localStorage.setItem("comfy_sync_last_username", cleaned);
            if (usernameModalEl) {
                usernameModalEl.remove();
                usernameModalEl = null;
            }
            sendIdentifyToServer(true);
            renderUserTabsBar();
            renderSpectatorBanner();
        };

        formEl?.addEventListener("submit", (e) => {
            e.preventDefault();
            commitUsername(inputEl?.value);
        });

        usernameModalEl.querySelectorAll("[data-uname]").forEach((btn) => {
            btn.addEventListener("click", () => {
                commitUsername(btn.getAttribute("data-uname"));
            });
        });

        setTimeout(() => {
            inputEl?.focus();
            inputEl?.select();
        }, 50);
    }

    function createUserTabsBar() {
        if (!document.getElementById("comfy-user-tabs-bar")) {
            userTabsBarEl = document.createElement("div");
            userTabsBarEl.id = "comfy-user-tabs-bar";
            Object.assign(userTabsBarEl.style, {
                position: "fixed",
                top: "8px",
                left: "50%",
                transform: "translateX(-50%)",
                zIndex: "10001",
                display: "flex",
                alignItems: "center",
                gap: "6px",
                padding: "4px 8px",
                borderRadius: "999px",
                background: "rgba(15, 23, 42, 0.92)",
                border: "1px solid rgba(255,255,255,0.16)",
                boxShadow: "0 6px 20px rgba(0,0,0,0.45)",
                backdropFilter: "blur(8px)",
                fontFamily: "system-ui, -apple-system, sans-serif",
                fontSize: "12px",
                color: "#f8fafc",
                maxWidth: "90vw",
                overflowX: "auto",
            });
            document.body.appendChild(userTabsBarEl);
        }

        if (!document.getElementById("comfy-spectator-banner")) {
            spectatorBannerEl = document.createElement("div");
            spectatorBannerEl.id = "comfy-spectator-banner";
            Object.assign(spectatorBannerEl.style, {
                position: "fixed",
                top: "46px",
                left: "50%",
                transform: "translateX(-50%)",
                zIndex: "10000",
                display: "none",
                alignItems: "center",
                gap: "10px",
                padding: "6px 14px",
                borderRadius: "999px",
                background: "rgba(30, 58, 138, 0.94)",
                border: "1px solid rgba(96, 165, 250, 0.5)",
                boxShadow: "0 4px 16px rgba(0,0,0,0.4)",
                fontFamily: "system-ui, -apple-system, sans-serif",
                fontSize: "12px",
                color: "#eff6ff",
            });
            document.body.appendChild(spectatorBannerEl);
        }

        renderUserTabsBar();
        renderSpectatorBanner();
    }

    function renderUserTabsBar() {
        if (!userTabsBarEl) return;

        const userMap = new Map();
        if (myUsername) {
            userMap.set(myUsername, {
                username: myUsername,
                online: true,
                executing: false,
                progress: null,
            });
        }
        for (const u of knownUsers) {
            if (!u || !u.username || u.username === "default") continue;
            userMap.set(u.username, u);
        }

        // Order: myUsername first, then online users, then offline saved workspaces
        const sortedUsers = Array.from(userMap.values()).sort((a, b) => {
            if (a.username === myUsername) return -1;
            if (b.username === myUsername) return 1;
            if (a.online !== b.online) return a.online ? -1 : 1;
            return a.username.localeCompare(b.username);
        });

        userTabsBarEl.innerHTML = "";

        const labelSpan = document.createElement("span");
        labelSpan.textContent = "Users:";
        Object.assign(labelSpan.style, {
            color: "#94a3b8",
            fontWeight: "600",
            padding: "0 4px 0 6px",
            fontSize: "11px",
            textTransform: "uppercase",
            letterSpacing: "0.04em",
        });
        userTabsBarEl.appendChild(labelSpan);

        for (const u of sortedUsers) {
            const isMe = u.username === myUsername;
            const isViewing = u.username === viewingUser;
            const btn = document.createElement("button");
            btn.type = "button";
            Object.assign(btn.style, {
                display: "flex",
                alignItems: "center",
                gap: "5px",
                padding: "4px 11px",
                borderRadius: "999px",
                border: isViewing
                    ? isMe
                        ? "1px solid #22c55e"
                        : "1px solid #38bdf8"
                    : "1px solid rgba(255,255,255,0.1)",
                background: isViewing
                    ? isMe
                        ? "rgba(22, 163, 74, 0.28)"
                        : "rgba(2, 132, 199, 0.35)"
                    : "rgba(30, 41, 59, 0.75)",
                color: isViewing ? "#ffffff" : "#cbd5e1",
                fontWeight: isViewing ? "600" : "500",
                fontSize: "12px",
                cursor: "pointer",
                whiteSpace: "nowrap",
                transition: "all 0.15s ease",
            });

            const dotColor = u.online ? "#22c55e" : "#64748b";
            let execBadge = "";
            if (u.executing) {
                let pctText = "";
                if (u.progress && u.progress.max > 0) {
                    const pct = Math.round((u.progress.value / u.progress.max) * 100);
                    pctText = ` ${pct}%`;
                }
                execBadge = `<span style="padding:1px 6px;border-radius:999px;background:#f59e0b;color:#111827;font-size:10px;font-weight:700;">⚡${pctText}</span>`;
            }

            btn.innerHTML = `
                <span style="width:7px;height:7px;border-radius:50%;background:${dotColor};display:inline-block;"></span>
                <span>${u.username}${isMe ? " (You)" : ""}</span>
                ${execBadge}
            `;
            btn.title = isMe
                ? "Switch to your own independent workspace tab"
                : `Click to watch ${u.username}'s workspace in real time`;
            btn.addEventListener("click", () => {
                switchViewingTab(u.username);
            });
            userTabsBarEl.appendChild(btn);
        }

        // Rename / Switch user button
        const renameBtn = document.createElement("button");
        renameBtn.type = "button";
        renameBtn.textContent = "✎ Name";
        renameBtn.title = "Change your username";
        Object.assign(renameBtn.style, {
            padding: "4px 8px",
            borderRadius: "999px",
            border: "1px solid rgba(255,255,255,0.12)",
            background: "transparent",
            color: "#94a3b8",
            fontSize: "11px",
            cursor: "pointer",
            marginLeft: "2px",
        });
        renameBtn.addEventListener("click", () => showUsernameModal(true));
        userTabsBarEl.appendChild(renameBtn);
    }

    function renderSpectatorBanner() {
        if (!spectatorBannerEl) return;
        if (!myUsername || !viewingUser || viewingUser === myUsername) {
            spectatorBannerEl.style.display = "none";
            return;
        }
        spectatorBannerEl.style.display = "flex";
        spectatorBannerEl.innerHTML = `
            <span>👁️ Watching <b>${viewingUser}</b> in real time</span>
            <button id="comfy-spectator-collab-btn" type="button"
                style="padding:3px 9px;border-radius:999px;border:1px solid rgba(255,255,255,0.25);background:${
                    collaborateOnOtherTab ? "#16a34a" : "rgba(15,23,42,0.6)"
                };color:#fff;font-size:11px;cursor:pointer;font-weight:600;">
                ${collaborateOnOtherTab ? "✏️ Collaborating (Live Edit ON)" : "🔒 View-Only (Click to Edit Together)"}
            </button>
            <button id="comfy-spectator-back-btn" type="button"
                style="padding:3px 10px;border-radius:999px;border:none;background:#f8fafc;color:#0f172a;font-size:11px;font-weight:700;cursor:pointer;">
                ↩ Back to My Tab (${myUsername})
            </button>
        `;
        spectatorBannerEl
            .querySelector("#comfy-spectator-collab-btn")
            ?.addEventListener("click", () => {
                collaborateOnOtherTab = !collaborateOnOtherTab;
                renderSpectatorBanner();
            });
        spectatorBannerEl
            .querySelector("#comfy-spectator-back-btn")
            ?.addEventListener("click", () => {
                switchViewingTab(myUsername);
            });
    }

    function renderRemoteCursor(data) {
        if (!data || !data.username || data.username === myUsername) return;
        if (data.workspace_user && data.workspace_user !== viewingUser) return;
        if (typeof data.x !== "number" || typeof data.y !== "number") return;

        let cursorEl = remoteCursors.get(data.username);
        if (!cursorEl) {
            cursorEl = document.createElement("div");
            Object.assign(cursorEl.style, {
                position: "fixed",
                pointerEvents: "none",
                zIndex: "9998",
                transition: "transform 0.06s linear",
                display: "flex",
                alignItems: "center",
                gap: "4px",
                left: "0",
                top: "0",
            });
            cursorEl.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="#38bdf8" stroke="#0f172a" stroke-width="2">
                    <path d="M3 3l7.07 16.97 2.51-7.39 7.39-2.51L3 3z"/>
                </svg>
                <span style="padding:2px 6px;border-radius:999px;background:#0284c7;color:#fff;font-size:11px;font-family:system-ui,sans-serif;font-weight:600;box-shadow:0 2px 6px rgba(0,0,0,0.4);">
                    ${data.username}
                </span>
            `;
            document.body.appendChild(cursorEl);
            remoteCursors.set(data.username, cursorEl);
        }

        const scale = app.canvas?.ds?.scale || 1;
        const offset = app.canvas?.ds?.offset || [0, 0];
        const canvasEl = app.canvas?.canvas;
        const rect = canvasEl ? canvasEl.getBoundingClientRect() : { left: 0, top: 0 };

        const screenX = rect.left + (data.x + offset[0]) * scale;
        const screenY = rect.top + (data.y + offset[1]) * scale;
        cursorEl.style.transform = `translate(${Math.round(screenX)}px, ${Math.round(screenY)}px)`;
        cursorEl._lastSeen = Date.now();
    }

    function setupPointerTracking() {
        window.addEventListener(
            "pointermove",
            (e) => {
                if (!myUsername || !isEditingAllowed()) return;
                const now = Date.now();
                if (now - lastCursorSentAt < 65) return;
                if (!api.socket || api.socket.readyState !== WebSocket.OPEN) return;

                const canvasEl = app.canvas?.canvas;
                if (!canvasEl) return;
                const rect = canvasEl.getBoundingClientRect();
                const scale = app.canvas?.ds?.scale || 1;
                const offset = app.canvas?.ds?.offset || [0, 0];

                const graphX = (e.clientX - rect.left) / scale - offset[0];
                const graphY = (e.clientY - rect.top) / scale - offset[1];

                lastCursorSentAt = now;
                api.socket.send(
                    JSON.stringify({
                        type: "sync_cursor",
                        data: {
                            username: myUsername,
                            workspace_user: viewingUser || myUsername,
                            x: Math.round(graphX),
                            y: Math.round(graphY),
                        },
                    })
                );
            },
            { passive: true }
        );

        // Clean up idle cursors after 10s
        setInterval(() => {
            const now = Date.now();
            for (const [uname, el] of remoteCursors.entries()) {
                if (now - (el._lastSeen || 0) > 10000) {
                    el.remove();
                    remoteCursors.delete(uname);
                }
            }
        }, 3000);
    }

    function updateStatusBadgeUI() {
        if (!statusBadgeEl) return;
        const peerOk = Boolean(latestStatus.peer_connected);
        const browsers = Number(latestStatus.connected_browsers || 1);
        const dotColor = peerOk ? "#22c55e" : browsers > 1 ? "#38bdf8" : "#a3e635";
        const label = peerOk
            ? `Cluster Synced • Peer + ${browsers} UI${browsers > 1 ? "s" : ""}`
            : `Live Sync • ${browsers} UI${browsers > 1 ? "s" : ""}`;
        statusBadgeEl.innerHTML = `
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${dotColor};box-shadow:0 0 6px ${dotColor};"></span>
            <span>${label}</span>
        `;
    }

    function createStatusWidget() {
        if (document.getElementById("comfy-cluster-sync-badge")) return;
        statusBadgeEl = document.createElement("div");
        statusBadgeEl.id = "comfy-cluster-sync-badge";
        Object.assign(statusBadgeEl.style, {
            position: "fixed",
            bottom: "12px",
            right: "14px",
            zIndex: "9999",
            display: "flex",
            alignItems: "center",
            gap: "6px",
            padding: "5px 10px",
            borderRadius: "999px",
            background: "rgba(17, 24, 39, 0.88)",
            color: "#f3f4f6",
            fontSize: "12px",
            fontFamily: "system-ui, -apple-system, sans-serif",
            border: "1px solid rgba(255,255,255,0.14)",
            cursor: "pointer",
            userSelect: "none",
            backdropFilter: "blur(6px)",
            boxShadow: "0 4px 12px rgba(0,0,0,0.35)",
        });
        statusBadgeEl.title = "Click to configure ComfyUI Cluster / Cloud Synchronization";
        updateStatusBadgeUI();

        statusPopoverEl = document.createElement("div");
        statusPopoverEl.id = "comfy-cluster-sync-popover";
        Object.assign(statusPopoverEl.style, {
            position: "fixed",
            bottom: "46px",
            right: "14px",
            zIndex: "10000",
            width: "320px",
            padding: "12px",
            borderRadius: "10px",
            background: "rgba(17, 24, 39, 0.96)",
            color: "#f9fafb",
            fontSize: "12px",
            fontFamily: "system-ui, -apple-system, sans-serif",
            border: "1px solid rgba(255,255,255,0.18)",
            boxShadow: "0 8px 24px rgba(0,0,0,0.5)",
            display: "none",
        });

        statusBadgeEl.addEventListener("click", async () => {
            if (statusPopoverEl.style.display === "none") {
                try {
                    const r = await fetch(api.apiURL("/cluster/sync/status"));
                    if (r.ok) {
                        latestStatus = await r.json();
                        if (Array.isArray(latestStatus.users)) {
                            knownUsers = latestStatus.users;
                            renderUserTabsBar();
                        }
                        updateStatusBadgeUI();
                    }
                } catch {}
                renderPopoverContent();
                statusPopoverEl.style.display = "block";
            } else {
                statusPopoverEl.style.display = "none";
            }
        });

        document.body.appendChild(statusBadgeEl);
        document.body.appendChild(statusPopoverEl);
    }

    function renderPopoverContent() {
        if (!statusPopoverEl) return;
        const tunnelUrl =
            latestStatus.self_tunnel_url ||
            (window.location.hostname.endsWith(".trycloudflare.com") ? window.location.origin : "");
        statusPopoverEl.innerHTML = `
            <div style="font-weight:600;font-size:13px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;">
                <span>ComfyUI Cluster Sync</span>
                <span style="font-size:11px;color:#9ca3af;">${latestStatus.node_id || "node_a"}</span>
            </div>
            <div style="margin-bottom:8px;color:#d1d5db;line-height:1.4;">
                <div>• Your Username: <b>${myUsername || "Not set"}</b></div>
                <div>• Viewing Tab: <b>${viewingUser || myUsername || "default"}</b></div>
                <div>• Connected Browsers: <b>${latestStatus.connected_browsers || 1}</b></div>
                <div>• Peer Node: <b>${latestStatus.peer_connected ? "Connected" : "Standalone / Waiting"}</b></div>
                ${tunnelUrl ? `<div style="margin-top:4px;word-break:break-all;color:#38bdf8;">• Tunnel: ${tunnelUrl}</div>` : ""}
            </div>
            <div style="margin-bottom:6px;color:#9ca3af;font-size:11px;">
                Link another ComfyUI server (e.g. Cloud Tunnel URL):
            </div>
            <div style="display:flex;gap:6px;">
                <input id="comfy-sync-peer-input" type="text" placeholder="https://xxxx.trycloudflare.com"
                    value="${latestStatus.peer_url || ""}"
                    style="flex:1;padding:5px 8px;border-radius:6px;border:1px solid #374151;background:#111827;color:#fff;font-size:12px;" />
                <button id="comfy-sync-peer-save"
                    style="padding:5px 10px;border-radius:6px;border:none;background:#2563eb;color:#fff;font-weight:600;cursor:pointer;font-size:12px;">
                    Sync
                </button>
            </div>
        `;
        const saveBtn = statusPopoverEl.querySelector("#comfy-sync-peer-save");
        const inputEl = statusPopoverEl.querySelector("#comfy-sync-peer-input");
        if (saveBtn && inputEl) {
            saveBtn.addEventListener("click", async () => {
                const peerUrl = inputEl.value.trim();
                saveBtn.textContent = "...";
                try {
                    const resp = await fetch(api.apiURL("/cluster/sync/config"), {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ peer_url: peerUrl }),
                    });
                    if (resp.ok) {
                        latestStatus = await resp.json();
                        updateStatusBadgeUI();
                        renderPopoverContent();
                    }
                } catch {}
            });
        }
    }

    function setupCrossTabAndLocalBridge() {
        const isCloudTunnel = window.location.hostname.endsWith(".trycloudflare.com");
        try {
            const bc = new BroadcastChannel("comfy_cluster_sync_bridge");
            bc.onmessage = async (ev) => {
                const msg = ev.data || {};
                if (msg.type === "cloud_tunnel_available" && !isCloudTunnel && msg.url) {
                    if (!latestStatus.peer_connected && !latestStatus.peer_url) {
                        try {
                            const r = await fetch(api.apiURL("/cluster/sync/config"), {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ peer_url: msg.url }),
                            });
                            if (r.ok) {
                                latestStatus = await r.json();
                                updateStatusBadgeUI();
                            }
                        } catch {}
                    }
                }
            };
            if (isCloudTunnel) {
                const tunnelOrigin = window.location.origin;
                bc.postMessage({ type: "cloud_tunnel_available", url: tunnelOrigin });
                fetch("http://127.0.0.1:8188/cluster/sync/config", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ peer_url: tunnelOrigin }),
                }).catch(() => {});
            }
        } catch {}
    }

    async function handleFullStatePayload(data, forceApply = false) {
        if (!data || typeof data !== "object") return;
        latestStatus = {
            ...latestStatus,
            node_id: data.node_id || latestStatus.node_id,
            peer_url: data.peer_url ?? latestStatus.peer_url,
            peer_connected: Boolean(data.peer_connected),
            self_tunnel_url: data.self_tunnel_url || latestStatus.self_tunnel_url,
            connected_browsers: data.connected_browsers || latestStatus.connected_browsers,
        };
        if (Array.isArray(data.users)) {
            knownUsers = data.users;
            renderUserTabsBar();
            if (usernameModalEl) {
                // Refresh user chips if modal is open
                showUsernameModal(false);
            }
        }
        updateStatusBadgeUI();

        // Do not overwrite local canvas until user has chosen their username
        if (!myUsername) {
            return;
        }

        const payloadUser = data.workspace_user || data.viewing_user || "default";
        if (payloadUser !== "default" && payloadUser !== viewingUser) {
            return;
        }

        if (data.workflow && Array.isArray(data.workflow.nodes) && data.workflow.nodes.length > 0) {
            await applyRemoteWorkflow(data.workflow, data.ts || 0, forceApply);
        } else if (viewingUser === myUsername && app.graph && Array.isArray(app.graph._nodes) && app.graph._nodes.length > 0) {
            schedulePushWorkspace(50);
        }

        if (data.node_outputs && typeof data.node_outputs === "object") {
            const targetMap = getActiveOutputsMap(viewingUser);
            Object.assign(targetMap, data.node_outputs);
            applyNodeOutputsMap(targetMap, true);
        }

        app.runningNodeId = data.executing_node ?? null;
        app.graph?.setDirtyCanvas?.(true, true);
    }

    function registerApiListeners() {
        api.addEventListener("sync_full_state", async (event) => {
            await handleFullStatePayload(event.detail || {});
        });

        api.addEventListener("sync_users_list", (event) => {
            const users = event.detail?.users;
            if (Array.isArray(users)) {
                knownUsers = users;
                renderUserTabsBar();
            }
        });

        api.addEventListener("sync_workspace", async (event) => {
            const payload = event.detail || {};
            if (!myUsername) return;
            if (payload.sender && payload.sender === api.clientId) {
                return;
            }
            const wsUser = payload.workspace_user || "default";
            if (wsUser !== viewingUser && wsUser !== "default") {
                return;
            }
            if (payload.workflow) {
                await applyRemoteWorkflow(payload.workflow, payload.ts || 0);
            }
        });

        api.addEventListener("sync_cursor", (event) => {
            renderRemoteCursor(event.detail || {});
        });

        api.addEventListener("sync_node_outputs", (event) => {
            const payload = event.detail || {};
            if (payload.sender && payload.sender === api.clientId) return;
            const wsUser = payload.workspace_user || viewingUser;
            if (wsUser && wsUser !== viewingUser && wsUser !== "default") return;
            if (payload.node_outputs) {
                applyNodeOutputsMap(payload.node_outputs, true);
            }
        });

        api.addEventListener("executed", (event) => {
            const detail = event.detail || {};
            if (detail._from_cluster_sync) return;
            const wsUser = detail.workspace_user || viewingUser;
            const nodeId = detail.display_node || detail.node;
            if (nodeId !== undefined && detail.output) {
                renderNodeOutputPreview(nodeId, detail.output, wsUser);
            }
        });

        api.addEventListener("executing", (event) => {
            const detail = event.detail;
            if (detail && typeof detail === "object" && detail.workspace_user && detail.workspace_user !== viewingUser) {
                return;
            }
            const nodeId = detail && typeof detail === "object" ? detail.display_node || detail.node : detail;
            app.runningNodeId = nodeId ?? null;
            app.graph?.setDirtyCanvas?.(true, true);
        });

        api.addEventListener("history_updated", async (event) => {
            const detail = event.detail || {};
            const wsUser = detail.workspace_user || viewingUser;
            if (detail.outputs && typeof detail.outputs === "object") {
                const targetMap = getActiveOutputsMap(wsUser);
                Object.assign(targetMap, detail.outputs);
                if (wsUser === viewingUser || wsUser === "default") {
                    applyNodeOutputsMap(detail.outputs, true);
                }
            }
            try {
                await app.ui?.queue?.update?.();
                await app.ui?.history?.update?.();
            } catch {}
        });

        api.addEventListener("userdata_updated", async (event) => {
            const path = event.detail?.path || "";
            if (path.includes("workflows")) {
                try {
                    await app.extensionManager?.workflow?.syncWorkflows?.();
                } catch {}
            } else if (path.includes("settings")) {
                try {
                    await app.ui?.settings?.load?.();
                } catch {}
            }
        });

        api.addEventListener("sync_conflict", (event) => {
            const info = event.detail || {};
            app.extensionManager?.toast?.add?.({
                severity: "warn",
                summary: "Workflow Sync Conflict",
                detail: `Concurrent edit saved to ${info.conflict_file || "conflict backup"}`,
                life: 6000,
            });
        });

        api.addEventListener("reconnected", () => {
            if (myUsername) {
                sendIdentifyToServer(false);
            }
        });
    }

    registerApiListeners();

    app.registerExtension({
        name: "Comfy.ClusterSync",
        async init() {
            registerApiListeners();
        },
        async setup() {
            attachGraphHooks();
            createUserTabsBar();
            createStatusWidget();
            setupCrossTabAndLocalBridge();
            setupPointerTracking();

            // Fetch initial user list
            try {
                const uResp = await fetch(api.apiURL("/cluster/sync/users"));
                if (uResp.ok) {
                    const uData = await uResp.json();
                    if (Array.isArray(uData.users)) {
                        knownUsers = uData.users;
                        renderUserTabsBar();
                    }
                }
            } catch {}

            // Prompt for username if not set in this tab session
            if (!myUsername) {
                showUsernameModal(false);
            } else {
                viewingUser = myUsername;
                sendIdentifyToServer(true);
                renderUserTabsBar();
                try {
                    const resp = await fetch(
                        api.apiURL(`/cluster/sync/workspace?user=${encodeURIComponent(viewingUser)}`)
                    );
                    if (resp.ok) {
                        const stateData = await resp.json();
                        await handleFullStatePayload(stateData);
                    }
                } catch {}
            }

            // Global interaction listeners + periodic dirty check for live canvas synchronization
            const triggerCheck = () => {
                attachGraphHooks();
                if (!isApplyingRemote && isEditingAllowed()) {
                    schedulePushWorkspace(90);
                }
            };
            window.addEventListener("pointerup", triggerCheck, { passive: true });
            window.addEventListener("keyup", triggerCheck, { passive: true });
            window.addEventListener("change", triggerCheck, { passive: true });
            window.addEventListener("paste", triggerCheck, { passive: true });
            window.addEventListener("drop", triggerCheck, { passive: true });

            setInterval(() => {
                attachGraphHooks();
                if (!isApplyingRemote && isEditingAllowed() && app.graph) {
                    const fp = computeWorkflowFingerprint(app.graph.serialize());
                    if (fp && fp !== lastSyncedFingerprint) {
                        pushWorkspaceNow();
                    }
                }
            }, 300);

            setInterval(async () => {
                try {
                    const r = await fetch(api.apiURL("/cluster/sync/status"));
                    if (r.ok) {
                        latestStatus = await r.json();
                        if (Array.isArray(latestStatus.users)) {
                            knownUsers = latestStatus.users;
                            renderUserTabsBar();
                        }
                        updateStatusBadgeUI();
                    }
                } catch {}
            }, 5000);
        },
        async afterConfigureGraph() {
            attachGraphHooks();
            applyNodeOutputsMap(getActiveOutputsMap(viewingUser), false);
            if (!isApplyingRemote && isEditingAllowed()) {
                schedulePushWorkspace(80);
            }
        },
        async nodeCreated(node) {
            hookNodeWidgets(node);
        },
    });
}
