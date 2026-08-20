const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const path = require('node:path');
const WebSocket = require('ws');

const PORT = 19080 + (process.pid % 500);
const URL = `ws://127.0.0.1:${PORT}`;

const waitForServer = child => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('Signaling server did not start')), 5000);
  child.stdout.on('data', chunk => {
    if (chunk.toString().includes('Signaling server running')) {
      clearTimeout(timer);
      resolve();
    }
  });
  child.once('exit', code => reject(new Error(`Signaling server exited with ${code}`)));
});

const connect = () => new Promise((resolve, reject) => {
  const ws = new WebSocket(URL);
  ws.once('open', () => resolve(ws));
  ws.once('error', reject);
});

const waitForType = (ws, type) => new Promise((resolve, reject) => {
  const timer = setTimeout(() => {
    ws.off('message', onMessage);
    reject(new Error(`Timed out waiting for ${type}`));
  }, 3000);

  const onMessage = raw => {
    const message = JSON.parse(raw.toString());
    if (message.type !== type) return;
    clearTimeout(timer);
    ws.off('message', onMessage);
    resolve(message);
  };
  ws.on('message', onMessage);
});

const join = async (ws, clientId) => {
  const joined = waitForType(ws, 'room-joined');
  ws.send(JSON.stringify({ type: 'join-room', roomId: 'T3ST1', clientId }));
  return joined;
};

test('enforces room capacity and supports presence across reconnects', async t => {
  const server = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
    env: { ...process.env, PORT: String(PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(() => server.kill());
  await waitForServer(server);

  const first = await connect();
  const second = await connect();
  const third = await connect();
  t.after(() => {
    first.close();
    second.close();
    third.close();
  });

  const firstJoined = await join(first, 'client-first');
  assert.equal(firstJoined.participantCount, 1);

  const firstSawPeer = waitForType(first, 'peer-joined');
  const secondJoined = await join(second, 'client-second');
  assert.equal(secondJoined.participantCount, 2);
  await firstSawPeer;

  const roomFull = waitForType(third, 'room-full');
  third.send(JSON.stringify({ type: 'join-room', roomId: 'T3ST1', clientId: 'client-third' }));
  await roomFull;

  const replacement = await connect();
  t.after(() => replacement.close());
  const firstSawReconnect = waitForType(first, 'peer-joined');
  const replacementJoined = await join(replacement, 'client-second');
  assert.equal(replacementJoined.participantCount, 2);
  await firstSawReconnect;

  const peerLeft = waitForType(first, 'peer-left');
  replacement.close();
  await peerLeft;
});
