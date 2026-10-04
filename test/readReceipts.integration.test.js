/**
 * Read & delivery receipts (Haven iOS #4.x) — server emits
 * `message-delivered` to the sender when a DM message reaches a
 * recipient's socket, and `message-read` to the sender when the
 * recipient's client marks the DM read.
 *
 *   node --test test/readReceipts.integration.test.js
 */
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { spawn } = require('node:child_process');
const { io } = require('socket.io-client');
const Database = require('better-sqlite3');

const PORT = 3402;
const BASE = `http://127.0.0.1:${PORT}`;
const ROOT = path.join(__dirname, '..');
const DATA = path.join(os.tmpdir(), `haven-read-receipts-${Date.now()}`);

let server;

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const post = (p, body) => fetch(BASE + p, {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
}).then(async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) }));

function connect(token) {
  const s = io(BASE, { auth: { token }, transports: ['websocket'], forceNew: true });
  return new Promise((res, rej) => { s.on('connect', () => res(s)); s.on('connect_error', rej); });
}

/// Resolves with the payload of the next `event`, or rejects on timeout.
function once(sock, event, timeoutMs = 4000) {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error(`timeout waiting for ${event}`)), timeoutMs);
    sock.once(event, (payload) => { clearTimeout(t); res(payload); });
  });
}

/// Starts a listener that collects payloads for `event`.
function buffer(sock, event) {
  const seen = [];
  sock.on(event, (p) => seen.push(p));
  return seen;
}

/// Reads a row straight from the (isolated) database file.
function dbRow(sql, ...args) {
  const db = new Database(path.join(DATA, 'haven.db'), { readonly: true });
  try { return db.prepare(sql).get(...args); } finally { db.close(); }
}

test.before(async () => {
  fs.mkdirSync(DATA, { recursive: true });
  server = spawn(process.execPath, ['server.js'], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), HOST: '127.0.0.1', FORCE_HTTP: 'true', HAVEN_DATA_DIR: DATA, ADMIN_USERNAME: 'admin' },
    stdio: 'ignore',
  });
  for (let i = 0; i < 60; i++) {
    try {
      await new Promise((res, rej) => http.get(`${BASE}/api/health`, (r) => (r.statusCode === 200 ? res() : rej())).on('error', rej));
      return;
    } catch { await wait(500); }
  }
  throw new Error('server did not start');
});

test.after(() => {
  server?.kill();
  try { fs.rmSync(DATA, { recursive: true, force: true }); } catch { /* ignore */ }
});

test('DM: delivered fires when the recipient is online, read fires on mark-read', async () => {
  const alice = await post('/api/auth/register', { username: 'alice', password: 'receiptpw1', eulaVersion: '2.0', ageVerified: true });
  const bob = await post('/api/auth/register', { username: 'bob', password: 'receiptpw2', eulaVersion: '2.0', ageVerified: true });
  assert.ok(alice.body.token, 'alice registered');
  assert.ok(bob.body.token, 'bob registered');
  const bobId = bob.body.user.id;

  const sa = await connect(alice.body.token);
  const sb = await connect(bob.body.token);

  const dm = await (async () => {
    const opened = once(sa, 'dm-opened');
    sa.emit('start-dm', { targetUserId: bobId });
    return opened;
  })();
  const code = dm.code;
  // The server joins the target's socket into the room around the
  // dm-opened emits; give the adapter a beat before messaging.
  await wait(300);

  // 1. Deliver: bob (online) receives new-message, alice gets message-delivered.
  const incoming = once(sb, 'new-message');
  const delivered = once(sa, 'message-delivered');
  sa.emit('send-message', { code, content: 'hello bob' });
  const nm = await incoming;
  assert.equal(nm.channelCode, code);
  const msgId = nm.message.id;
  const d = await delivered;
  assert.equal(d.channelCode, code);
  assert.equal(d.messageId, msgId);

  // 2. Read: bob's mark-read makes alice see message-read, attributed to bob.
  const read = once(sa, 'message-read');
  sb.emit('mark-read', { code, messageId: msgId });
  const r = await read;
  assert.equal(r.channelCode, code);
  assert.equal(r.messageId, msgId);
  assert.equal(r.user_id, bobId);

  // 3. Idempotent: repeating mark-read does not re-fire the receipt.
  const again = buffer(sa, 'message-read');
  sb.emit('mark-read', { code, messageId: msgId });
  await wait(400);
  assert.equal(again.filter((p) => p.messageId === msgId).length, 0, 'no duplicate read receipt');

  // 4. A second message gets its own receipt (no crosstalk with the first).
  const incoming2 = once(sb, 'new-message');
  const delivered2 = once(sa, 'message-delivered');
  sa.emit('send-message', { code, content: 'second one' });
  const nm2 = await incoming2;
  const d2 = await delivered2;
  assert.equal(d2.messageId, nm2.message.id);
  assert.notEqual(nm2.message.id, msgId);
  const read2 = once(sa, 'message-read');
  sb.emit('mark-read', { code, messageId: nm2.message.id });
  const r2 = await read2;
  assert.equal(r2.messageId, nm2.message.id);

  sa.close();
  sb.close();
});

test('DM: no delivered event when the recipient has no socket', async () => {
  const dana = await post('/api/auth/register', { username: 'dana', password: 'receiptpw3', eulaVersion: '2.0', ageVerified: true });
  const erin = await post('/api/auth/register', { username: 'erin', password: 'receiptpw4', eulaVersion: '2.0', ageVerified: true });
  assert.ok(dana.body.token && erin.body.token);
  const danaId = dana.body.user.id;

  // Dana registers but never connects: her DM partner is offline.
  const se = await connect(erin.body.token);
  const dm = await (async () => {
    const opened = once(se, 'dm-opened');
    se.emit('start-dm', { targetUserId: danaId });
    return opened;
  })();
  await wait(300);

  const delivered = buffer(se, 'message-delivered');
  se.emit('send-message', { code: dm.code, content: 'anyone home?' });
  await wait(600);
  assert.equal(delivered.length, 0, 'no delivered receipt for an offline recipient');

  se.close();
});

test('group channel: no delivered, no read receipts', async () => {
  // First registered user on this server is the admin and can create channels.
  const admin = await post('/api/auth/register', { username: 'admin', password: 'rolecheckpw1', eulaVersion: '2.0', ageVerified: true });
  const hugo = await post('/api/auth/register', { username: 'hugo', password: 'receiptpw6', eulaVersion: '2.0', ageVerified: true });
  assert.ok(admin.body.token && hugo.body.token);
  const hugoId = hugo.body.user.id;

  const sg = await connect(admin.body.token);
  const sh = await connect(hugo.body.token);

  const created = once(sg, 'channel-created', 4000).catch(() => null);
  sg.emit('create-channel', { name: 'receipt-group' });
  const ev = await created;
  await wait(200);

  // The channel code is not guaranteed in the event; read it from the DB.
  const row = dbRow('SELECT id, code FROM channels WHERE name = ?', 'receipt-group');
  assert.ok(row, 'channel row exists');
  if (ev && ev.code) assert.equal(row.code, ev.code, 'event code matches DB');

  sg.emit('invite-to-channel', { targetUserId: hugoId, channelId: row.id });
  // A real client joins the live room when the user opens the channel.
  sh.emit('enter-channel', { code: row.code });
  await wait(300);

  const delivered = buffer(sg, 'message-delivered');
  const readReceipts = buffer(sg, 'message-read');
  const incoming = once(sh, 'new-message');
  sg.emit('send-message', { code: row.code, content: 'group hello' });
  const nm = await incoming;

  await wait(500);
  assert.equal(delivered.length, 0, 'group messages are not delivery-receipted');

  sh.emit('mark-read', { code: row.code, messageId: nm.message.id });
  await wait(500);
  assert.equal(readReceipts.filter((p) => p.messageId === nm.message.id).length, 0, 'group reads are not receipted');

  sg.close();
  sh.close();
});
