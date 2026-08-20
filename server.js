const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Ephemeral signaling for two-person P2P rooms. Nothing is persisted.
const rooms = new Map();
const ROOM_ID_PATTERN = /^[A-Z0-9]{3,8}$/;

const send = (ws, data) => {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
};

const leaveRoom = (ws, notifyPeer = true) => {
  if (!ws.roomId) return;

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

      // A reconnect from the same browser replaces its stale signaling socket.
      for (const existingClient of room) {
        if (existingClient !== ws && existingClient.clientId === clientId) {
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
      
      // When a second person joins, tell the FIRST person (Creator) to initiate the call.
      if (room.size === 2) {
        for (const client of room) {
          if (client !== ws) send(client, { type: 'peer-joined' });
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
    leaveRoom(ws);
  });
});

const PORT = process.env.PORT || 9080;
server.listen(PORT, () => {
  console.log(`Signaling server running on port ${PORT}`);
});
