/**
 * Metrolist Sync Relay Server (Node.js)
 * 
 * A lightweight WebSocket server that pairs devices by Google Account hash
 * and relays playback commands between them.
 * 
 * Deploy for free on Back4App, Glitch.com, etc.
 */

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// ============================================================
// Room Manager — tracks all connected devices grouped by account
// ============================================================

const rooms = new Map(); // accountHash -> Map<deviceId, device>

function addDevice(device) {
    if (!rooms.has(device.accountHash)) {
        rooms.set(device.accountHash, new Map());
    }
    rooms.get(device.accountHash).set(device.deviceId, device);
}

function removeDevice(accountHash, deviceId) {
    const room = rooms.get(accountHash);
    if (room) {
        room.delete(deviceId);
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

// ============================================================
// Message Types (must match client SyncProtocol.kt)
// ============================================================

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

// ============================================================
// Helper: send a typed message over WebSocket
// ============================================================

function sendMsg(ws, type, payload) {
    try {
        if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type, payload: JSON.stringify(payload) }));
        }
    } catch (e) {
        console.error("[SEND ERROR]", e.message);
    }
}

// ============================================================
// HTTP Server (health check + stats)
// ============================================================

const server = http.createServer((req, res) => {
    if (req.url === "/stats") {
        let totalDevices = 0;
        rooms.forEach((room) => (totalDevices += room.size));
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end(`Rooms: ${rooms.size}, Devices: ${totalDevices}`);
    } else {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("Metrolist Sync Relay Server is running");
    }
});

// ============================================================
// WebSocket Server
// ============================================================

const wss = new WebSocketServer({ server, path: "/sync" });

wss.on("connection", (ws) => {
    let currentDevice = null;

    ws.on("message", (raw) => {
        try {
            const message = JSON.parse(raw.toString());
            const payload = message.payload ? JSON.parse(message.payload) : {};

            switch (message.type) {
                // --- REGISTER: Device comes online ---
                case MSG.REGISTER: {
                    currentDevice = {
                        deviceId: payload.device_id,
                        deviceName: payload.device_name,
                        accountHash: payload.account_hash,
                        ws,
                        state: {
                            device_id: payload.device_id,
                            device_name: payload.device_name,
                            account_hash: payload.account_hash,
                            song_id: null,
                            song_title: null,
                            song_artist: null,
                            position_ms: 0,
                            is_playing: false,
                            timestamp: Date.now(),
                        },
                    };
                    addDevice(currentDevice);

                    console.log(
                        `[REGISTER] ${payload.device_name} (${payload.device_id}) joined room ${payload.account_hash}`
                    );

                    // Notify other devices
                    const others = getOtherDevices(
                        payload.account_hash,
                        payload.device_id
                    );
                    others.forEach((other) => {
                        sendMsg(other.ws, MSG.DEVICE_JOINED, currentDevice.state);
                    });

                    // Send room info to the new device
                    const roomDevices = getRoomDevices(payload.account_hash).map(
                        (d) => d.state
                    );
                    const active = getActiveDevice(payload.account_hash);
                    sendMsg(ws, MSG.ROOM_INFO, {
                        devices: roomDevices,
                        active_device_id: active ? active.deviceId : null,
                    });
                    break;
                }

                // --- STATE_UPDATE: Playback state changed ---
                case MSG.STATE_UPDATE: {
                    if (!currentDevice) break;

                    updateDeviceState(
                        currentDevice.accountHash,
                        currentDevice.deviceId,
                        payload
                    );

                    // Check for conflict
                    const otherPlaying = getOtherDevices(
                        currentDevice.accountHash,
                        currentDevice.deviceId
                    ).filter((d) => d.state.is_playing);

                    if (payload.is_playing && otherPlaying.length > 0) {
                        // Conflict! Another device is also playing
                        const otherDev = otherPlaying[0];
                        sendMsg(ws, MSG.CONFLICT, {
                            other_device_id: otherDev.deviceId,
                            other_device_name: otherDev.deviceName,
                            other_song_id: otherDev.state.song_id,
                            other_song_title: otherDev.state.song_title,
                        });
                    } else {
                        // No conflict: broadcast to others
                        const others = getOtherDevices(
                            currentDevice.accountHash,
                            currentDevice.deviceId
                        );
                        others.forEach((other) => {
                            sendMsg(other.ws, MSG.REMOTE_STATE, payload);
                        });
                    }
                    break;
                }

                // --- PLAYBACK_COMMAND: Remote control ---
                case MSG.PLAYBACK_COMMAND: {
                    if (!currentDevice) break;

                    const others = getOtherDevices(
                        currentDevice.accountHash,
                        currentDevice.deviceId
                    );
                    others.forEach((other) => {
                        sendMsg(other.ws, MSG.REMOTE_COMMAND, payload);
                    });

                    console.log(
                        `[CMD] ${currentDevice.deviceName} sent command to ${others.length} device(s)`
                    );
                    break;
                }

                // --- UNREGISTER: Graceful disconnect ---
                case MSG.UNREGISTER: {
                    if (currentDevice) {
                        handleDisconnect(currentDevice);
                        currentDevice = null;
                    }
                    break;
                }
            }
        } catch (e) {
            console.error("[ERROR]", e.message);
        }
    });

    ws.on("close", () => {
        if (currentDevice) {
            handleDisconnect(currentDevice);
            currentDevice = null;
        }
    });

    ws.on("error", (e) => {
        console.error("[WS ERROR]", e.message);
    });
});

function handleDisconnect(device) {
    removeDevice(device.accountHash, device.deviceId);
    console.log(
        `[DISCONNECT] ${device.deviceName} (${device.deviceId}) left room ${device.accountHash}`
    );

    // Notify remaining devices
    const others = getRoomDevices(device.accountHash);
    others.forEach((other) => {
        sendMsg(other.ws, MSG.DEVICE_LEFT, device.state);
    });
}

// ============================================================
// Start
// ============================================================

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Metrolist Sync Relay Server running on port ${PORT}`);
    console.log(`WebSocket endpoint: ws://0.0.0.0:${PORT}/sync`);
    console.log(`Health check: http://0.0.0.0:${PORT}/`);
});
