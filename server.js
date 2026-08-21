const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const SIGNALING_HEARTBEAT_MS = Number(process.env.SIGNALING_HEARTBEAT_MS) || 25000;

// Ephemeral signaling for two-person P2P rooms. Nothing is persisted.
const rooms = new Map();
const ROOM_ID_PATTERN = /^[A-Z0-9]{3,8}$/;
const PEER_RECONNECT_GRACE_MS = Number(process.env.PEER_RECONNECT_GRACE_MS) || 5000;

const send = (ws, data) => {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
};

const leaveRoom = (ws, notifyPeer = true) => {
  if (!ws.roomId) return;

  if (ws.disconnectTimer) {
    clearTimeout(ws.disconnectTimer);
    ws.disconnectTimer = null;
  }

  const roomId = ws.roomId;
  const room = rooms.get(roomId);
  ws.roomId = null;
  if (!room) return;

  room.delete(ws);
  console.log(`Client left room ${roomId}. Total in room: ${room.size}`);

  if (notifyPeer) {
    for (const client of room) send(client, { type: 'peer-left' });
  }

  if (room.size === 0) rooms.delete(roomId);
};

wss.on('connection', (ws) => {
  ws.roomId = null;
  ws.clientId = null;
  ws.disconnectTimer = null;
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }

    if (data.type === 'join-room') {
      const roomId = typeof data.roomId === 'string' ? data.roomId.trim().toUpperCase() : '';
      const clientId = typeof data.clientId === 'string' ? data.clientId : '';
      if (!ROOM_ID_PATTERN.test(roomId) || !clientId) {
        send(ws, { type: 'room-error', message: 'Invalid room code.' });
        return;
      }
      
      if (ws.roomId && ws.roomId !== roomId) leaveRoom(ws);

      const room = rooms.get(roomId) || new Set();

      // A reconnect from the same browser takes over its reserved room seat.
      let replacedDisconnectedClient = false;
      for (const existingClient of room) {
        if (existingClient !== ws && existingClient.clientId === clientId) {
          replacedDisconnectedClient = Boolean(existingClient.disconnectTimer);
          if (existingClient.disconnectTimer) {
            clearTimeout(existingClient.disconnectTimer);
            existingClient.disconnectTimer = null;
          }
          room.delete(existingClient);
          existingClient.roomId = null;
          existingClient.close(4001, 'Replaced by reconnect');
        }
      }

      if (!room.has(ws) && room.size >= 2) {
        send(ws, { type: 'room-full' });
        return;
      }

      ws.roomId = roomId;
      ws.clientId = clientId;
      room.add(ws);
      rooms.set(roomId, room);
      
      console.log(`Client joined room ${roomId}. Total in room: ${room.size}`);
      send(ws, { type: 'room-joined', participantCount: room.size });
      
      // A quick signaling reconnect should not tear down healthy peer media. If the
      // media path also failed, the existing peer will start a fresh negotiation.
      if (room.size === 2) {
        for (const client of room) {
          if (client !== ws) {
            send(client, { type: replacedDisconnectedClient ? 'peer-reconnected' : 'peer-joined' });
          }
        }
      }
      
      return;
    }

    // Broadcast signaling message to all OTHER clients in the same room
    const room = ws.roomId ? rooms.get(ws.roomId) : null;
    if (room) {
      for (const client of room) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          // Send raw message string forward
          client.send(message.toString());
        }
      }
    }
  });

  ws.on('close', () => {
    if (!ws.roomId || ws.disconnectTimer) return;

    const room = rooms.get(ws.roomId);
    if (!room?.has(ws)) return;

    for (const client of room) {
      if (client !== ws) send(client, { type: 'peer-reconnecting' });
    }

    // WebSocket loss does not necessarily mean the WebRTC media path was lost.
    // Hold the ephemeral seat briefly so a reconnect can preserve a healthy call.
    ws.disconnectTimer = setTimeout(() => {
      ws.disconnectTimer = null;
      leaveRoom(ws);
    }, PEER_RECONNECT_GRACE_MS);
  });
});

// Keep idle signaling sockets alive through common proxy timeouts and detect
// half-open connections so the client can reconnect instead of hanging forever.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, SIGNALING_HEARTBEAT_MS);

wss.on('close', () => clearInterval(heartbeat));

const PORT = process.env.PORT || 9080;
server.listen(PORT, () => {
  console.log(`Signaling server running on port ${PORT}`);
});
