/**
 * Metrolist Sync Relay Server (Node.js)
 *
 * A lightweight WebSocket server that pairs devices by Google Account hash
 * and relays playback commands between them.
 *
 * Security hardening:
 *  - Max message size: 64 KB (DoS prevention)
 *  - /stats protected by X-Admin-Key header
 *  - account_hash validated as SHA-256 hex (64 chars)
 *  - device_id sanitised (alphanumeric + dash/underscore, ≤128 chars)
 *  - device_name clamped to 64 chars
 *  - Max 512 concurrent connections
 */

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ADMIN_KEY = process.env.RELAY_ADMIN_KEY || "";
const MAX_MESSAGE_BYTES = 65536;   // 64 KB
const MAX_CONNECTIONS = 512;

// ── Validation helpers ────────────────────────────────────────────────────────

const ACCOUNT_HASH_RE = /^[0-9a-f]{64}$/;
const DEVICE_ID_RE = /^[A-Za-z0-9\-_]{1,128}$/;

function isValidAccountHash(h) { return typeof h === "string" && ACCOUNT_HASH_RE.test(h); }
function isValidDeviceId(id) { return typeof id === "string" && DEVICE_ID_RE.test(id); }
function sanitizeName(n) { return (typeof n === "string" ? n : "unknown").slice(0, 64); }

// ── Room Manager ──────────────────────────────────────────────────────────────

const rooms = new Map(); // accountHash -> Map<deviceId, device>
let totalConnections = 0;

function addDevice(device) {
    if (!rooms.has(device.accountHash)) {
        rooms.set(device.accountHash, new Map());
    }
    const room = rooms.get(device.accountHash);
    // Remove stale session with same deviceId
    if (room.has(device.deviceId)) totalConnections--;
    room.set(device.deviceId, device);
    totalConnections++;
}

function removeDevice(accountHash, deviceId) {
    const room = rooms.get(accountHash);
    if (room && room.has(deviceId)) {
        room.delete(deviceId);
        totalConnections--;
        if (room.size === 0) rooms.delete(accountHash);
    }
}

function getOtherDevices(accountHash, deviceId) {
    const room = rooms.get(accountHash);
    if (!room) return [];
    return [...room.values()].filter((d) => d.deviceId !== deviceId);
}

function getRoomDevices(accountHash) {
    const room = rooms.get(accountHash);
    return room ? [...room.values()] : [];
}

function getActiveDevice(accountHash) {
    const room = rooms.get(accountHash);
    if (!room) return null;
    return [...room.values()].find((d) => d.state.is_playing) || null;
}

function updateDeviceState(accountHash, deviceId, state) {
    const room = rooms.get(accountHash);
    if (room && room.has(deviceId)) {
        room.get(deviceId).state = state;
    }
}

// ── Message types ─────────────────────────────────────────────────────────────

const MSG = {
    REGISTER: "register",
    UNREGISTER: "unregister",
    STATE_UPDATE: "state_update",
    PLAYBACK_COMMAND: "playback_cmd",
    DEVICE_JOINED: "device_joined",
    DEVICE_LEFT: "device_left",
    REMOTE_STATE: "remote_state",
    REMOTE_COMMAND: "remote_cmd",
    CONFLICT: "conflict",
    ROOM_INFO: "room_info",
};

// ── Helper: send a typed message safely ──────────────────────────────────────

function sendMsg(ws, type, payload) {
    try {
        if (ws.readyState === 1 /* OPEN */) {
            ws.send(JSON.stringify({ type, payload: JSON.stringify(payload) }));
        }
    } catch (e) {
        console.error("[SEND ERROR]", e.message);
    }
}

// ── HTTP Server (health check + stats) ────────────────────────────────────────

const server = http.createServer((req, res) => {
    if (req.url === "/stats") {
        // Protect stats with admin key when configured
        if (ADMIN_KEY) {
            const provided = req.headers["x-admin-key"] || "";
            if (provided !== ADMIN_KEY) {
                res.writeHead(401, { "Content-Type": "text/plain" });
                res.end("Unauthorized");
                return;
            }
        }
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end(`Rooms: ${rooms.size}, Devices: ${totalConnections}`);
    } else {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("Metrolist Sync Relay Server is running");
    }
});

// ── WebSocket Server ──────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server, path: "/sync", maxPayload: MAX_MESSAGE_BYTES });

wss.on("connection", (ws) => {
    // Enforce connection limit
    if (totalConnections >= MAX_CONNECTIONS) {
        ws.close(1013, "Server at capacity");
        return;
    }

    let currentDevice = null;

    ws.on("message", (raw) => {
        try {
            // Re-check size (belt-and-suspenders; maxPayload handles this too)
            if (raw.length > MAX_MESSAGE_BYTES) {
                ws.close(1009, "Payload too large");
                return;
            }

            const message = JSON.parse(raw.toString());
            if (!message || typeof message.type !== "string") return;

            // Only parse inner payload when present
            let payload = {};
            if (message.payload && typeof message.payload === "string") {
                payload = JSON.parse(message.payload);
            }

            switch (message.type) {
                // --- REGISTER ---
                case MSG.REGISTER: {
                    const accountHash = payload.account_hash;
                    const deviceId = payload.device_id;
                    const deviceName = sanitizeName(payload.device_name);

                    if (!isValidAccountHash(accountHash)) {
                        ws.close(1008, "Invalid account_hash");
                        return;
                    }
                    if (!isValidDeviceId(deviceId)) {
                        ws.close(1008, "Invalid device_id");
                        return;
                    }

                    currentDevice = {
                        deviceId,
                        deviceName,
                        accountHash,
                        ws,
                        state: {
                            device_id: deviceId,
                            device_name: deviceName,
                            account_hash: accountHash,
                            song_id: null,
                            song_title: null,
                            song_artist: null,
                            position_ms: 0,
                            is_playing: false,
                            timestamp: Date.now(),
                        },
                    };
                    addDevice(currentDevice);

                    console.log(`[REGISTER] ${deviceName} (${deviceId.slice(0, 8)}…) joined room ${accountHash.slice(0, 8)}…`);

                    // Notify other devices in the room
                    const others = getOtherDevices(accountHash, deviceId);
                    others.forEach((other) => sendMsg(other.ws, MSG.DEVICE_JOINED, currentDevice.state));

                    // Send room info back to the new device
                    const roomDevices = getRoomDevices(accountHash).map((d) => d.state);
                    const active = getActiveDevice(accountHash);
                    sendMsg(ws, MSG.ROOM_INFO, {
                        devices: roomDevices,
                        active_device_id: active ? active.deviceId : null,
                    });
                    break;
                }

                // --- STATE_UPDATE ---
                case MSG.STATE_UPDATE: {
                    if (!currentDevice) break;

                    updateDeviceState(currentDevice.accountHash, currentDevice.deviceId, payload);

                    const otherPlaying = getOtherDevices(currentDevice.accountHash, currentDevice.deviceId)
                        .filter((d) => d.state.is_playing);

                    if (payload.is_playing && otherPlaying.length > 0) {
                        const otherDev = otherPlaying[0];
                        sendMsg(ws, MSG.CONFLICT, {
                            other_device_id: otherDev.deviceId,
                            other_device_name: otherDev.deviceName,
                            other_song_id: otherDev.state.song_id,
                            other_song_title: otherDev.state.song_title,
                        });
                    } else {
                        getOtherDevices(currentDevice.accountHash, currentDevice.deviceId)
                            .forEach((other) => sendMsg(other.ws, MSG.REMOTE_STATE, payload));
                    }
                    break;
                }

                // --- PLAYBACK_COMMAND ---
                case MSG.PLAYBACK_COMMAND: {
                    if (!currentDevice) break;
                    const targets = getOtherDevices(currentDevice.accountHash, currentDevice.deviceId);
                    targets.forEach((other) => sendMsg(other.ws, MSG.REMOTE_COMMAND, payload));
                    console.log(`[CMD] ${currentDevice.deviceName} → ${targets.length} device(s): ${payload.action || "?"}`);
                    break;
                }

                // --- UNREGISTER ---
                case MSG.UNREGISTER: {
                    if (currentDevice) {
                        handleDisconnect(currentDevice);
                        currentDevice = null;
                    }
                    break;
                }
            }
        } catch (e) {
            // Never crash the server on bad input
            console.error("[MSG ERROR]", e.message);
        }
    });

    ws.on("close", () => {
        if (currentDevice) {
            handleDisconnect(currentDevice);
            currentDevice = null;
        }
    });

    ws.on("error", (e) => console.error("[WS ERROR]", e.message));
});

function handleDisconnect(device) {
    removeDevice(device.accountHash, device.deviceId);
    console.log(`[DISCONNECT] ${device.deviceName} (${device.deviceId.slice(0, 8)}…)`);
    getRoomDevices(device.accountHash)
        .forEach((other) => sendMsg(other.ws, MSG.DEVICE_LEFT, device.state));
}

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Metrolist Sync Relay running on port ${PORT}`);
    console.log(`WebSocket: ws://0.0.0.0:${PORT}/sync`);
    console.log(`Stats endpoint protected: ${ADMIN_KEY ? "yes (X-Admin-Key required)" : "no (set RELAY_ADMIN_KEY env var)"}`);
});
