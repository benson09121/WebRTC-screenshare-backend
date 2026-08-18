const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Simple signaling for room-based P2P.
const rooms = {};

wss.on('connection', (ws) => {
  ws.roomId = null;

  ws.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }

    if (data.type === 'join-room') {
      const roomId = data.roomId;
      if (!roomId) return;
      
      // Leave current room if any
      if (ws.roomId && rooms[ws.roomId]) {
        rooms[ws.roomId].delete(ws);
      }

      ws.roomId = roomId;
      if (!rooms[roomId]) rooms[roomId] = new Set();
      rooms[roomId].add(ws);
      
      console.log(`Client joined room ${roomId}. Total in room: ${rooms[roomId].size}`);
      return;
    }

    // Broadcast signaling message to all OTHER clients in the same room
    if (ws.roomId && rooms[ws.roomId]) {
      for (const client of rooms[ws.roomId]) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          // Send raw message string forward
          client.send(message.toString());
        }
      }
    }
  });

  ws.on('close', () => {
    if (ws.roomId && rooms[ws.roomId]) {
      rooms[ws.roomId].delete(ws);
      console.log(`Client left room ${ws.roomId}. Total in room: ${rooms[ws.roomId].size}`);
      if (rooms[ws.roomId].size === 0) {
        delete rooms[ws.roomId];
      }
    }
  });
});

const PORT = process.env.PORT || 9080;
server.listen(PORT, () => {
  console.log(`Signaling server running on port ${PORT}`);
});
