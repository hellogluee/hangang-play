// 한강점령기 — LAN 실시간 로비/릴레이 서버 (M1: 로비)
// 실행: node lobby.js   (같은 WiFi에서 팀원이 ws://<이맥IP>:8787 로 접속)
//
// 역할(M1): 접속·이름 등록, 나라(고구려/백제/신라) 선택 잠금, 로스터/입장 방송.
// M2에서 여기에 게임 상태 릴레이(host↔clients)를 얹는다.

const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT ? Number(process.env.PORT) : 8787;   // 클라우드는 PORT 주입
// WebGL 빌드 정적 서빙 디렉터리. 클라우드(server.js가 빌드와 같은 폴더)면 __dirname, 로컬 개발이면 ../webgl-deploy.
const STATIC_DIR = process.env.HG_STATIC ||
  (fs.existsSync(path.join(__dirname, "index.html")) ? __dirname : path.join(__dirname, "..", "webgl-deploy"));
const MIME = {
  ".html": "text/html; charset=utf-8", ".js": "application/javascript",
  ".wasm": "application/wasm", ".data": "application/octet-stream",
  ".json": "application/json", ".css": "text/css", ".png": "image/png",
  ".ico": "image/x-icon", ".ttf": "font/ttf",
};
const NATIONS = ["Goguryeo", "Baekje", "Silla"];
const NATION_KO = { Goguryeo: "고구려", Baekje: "백제", Silla: "신라" };

/** clientId -> { ws, name, nation|null, isHost } */
const clients = new Map();
let hostId = null;

// ── 최소 WebSocket(RFC6455) 구현: 외부 의존성 없이 순수 Node로 ──
function acceptKey(key) {
  return crypto
    .createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");
}
function encodeFrame(str) {
  const payload = Buffer.from(str, "utf8");
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x81, len]);
  } else if (len < 65536) {
    header = Buffer.from([0x81, 126, (len >> 8) & 255, len & 255]);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81; header[1] = 127;
    header.writeUInt32BE(Math.floor(len / 2 ** 32), 2);
    header.writeUInt32BE(len >>> 0, 6);
  }
  return Buffer.concat([header, payload]);
}
function send(ws, obj) {
  try { ws.write(encodeFrame(JSON.stringify(obj))); } catch (_) {}
}
function broadcast(obj) {
  for (const c of clients.values()) send(c.ws, obj);
}
function roster() {
  return {
    type: "roster",
    hostId,
    players: [...clients.entries()].map(([id, c]) => ({
      id, name: c.name, nation: c.nation, isHost: id === hostId,
    })),
    takenNations: [...clients.values()].map((c) => c.nation).filter(Boolean),
  };
}

const server = http.createServer((req, res) => {
  // 정적 파일 서빙 (WebGL 빌드). 경로 순회 방지.
  let rel = decodeURIComponent((req.url || "/").split("?")[0]);
  if (rel === "/" || rel === "") rel = "/index.html";
  const full = path.normalize(path.join(STATIC_DIR, rel));
  if (!full.startsWith(path.normalize(STATIC_DIR))) { res.writeHead(403); res.end(); return; }
  fs.readFile(full, (err, buf) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Not found. (WebGL 빌드가 " + STATIC_DIR + " 에 있어야 함)\n");
      return;
    }
    res.writeHead(200, {
      "Content-Type": MIME[path.extname(full)] || "application/octet-stream",
      // 재빌드 때 예전 framework.js + 새 wasm 캐시 불일치로 로딩이 멈추는 것 방지
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "Pragma": "no-cache",
      "Expires": "0",
    });
    res.end(buf);
  });
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return; }
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
    "Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n"
  );

  const id = crypto.randomBytes(4).toString("hex");
  clients.set(id, { ws: socket, name: "게스트", nation: null });
  if (!hostId) hostId = id;
  send(socket, { type: "welcome", id, host: id === hostId, nations: NATIONS });
  broadcast(roster());

  let buf = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    // 마스킹된 텍스트 프레임 파싱 (클라이언트→서버는 항상 마스킹됨)
    while (buf.length >= 2) {
      const len0 = buf[1] & 127;
      let off = 2, len = len0;
      if (len0 === 126) { if (buf.length < 4) break; len = buf.readUInt16BE(2); off = 4; }
      else if (len0 === 127) { if (buf.length < 10) break; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const masked = (buf[1] & 128) !== 0;
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) break;
      const opcode = buf[0] & 0x0f;
      let payload;
      if (masked) {
        const mask = buf.slice(off, off + 4);
        payload = buf.slice(off + 4, off + 4 + len);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      } else {
        payload = buf.slice(off, off + len);
      }
      buf = buf.slice(need);
      if (opcode === 0x8) { socket.end(); return; }        // close
      if (opcode === 0x1) handleMessage(id, payload.toString("utf8"));
    }
  });
  const cleanup = () => {
    const c = clients.get(id);
    clients.delete(id);
    if (c && c.nation) broadcast({ type: "log", text: `${NATION_KO[c.nation]} 플레이어 퇴장` });
    if (hostId === id) hostId = clients.keys().next().value || null;
    broadcast(roster());
  };
  socket.on("close", cleanup);
  socket.on("error", cleanup);
});

function handleMessage(id, text) {
  let msg; try { msg = JSON.parse(text); } catch { return; }
  const c = clients.get(id);
  if (!c) return;

  if (msg.type === "join") {
    c.name = (msg.name || "게스트").slice(0, 16);
    broadcast(roster());
  } else if (msg.type === "claim") {
    const n = msg.nation;
    if (!NATIONS.includes(n)) return;
    const taken = [...clients.values()].some((x) => x !== c && x.nation === n);
    if (taken) { send(c.ws, { type: "denied", nation: n }); return; }  // 이미 선택됨 → 잠금
    c.nation = n;
    broadcast({ type: "log", text: `${NATION_KO[n]} 플레이어 입장` });
    broadcast(roster());
  } else if (msg.type === "release") {
    c.nation = null;
    broadcast(roster());
  } else if (msg.type === "start" && id === hostId) {
    broadcast({ type: "start", roster: roster().players });
  } else if (msg.type === "relay") {
    // M2용: 임의 게임 메시지 릴레이 (host↔clients)
    broadcast({ type: "relay", from: id, data: msg.data });
  }
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[lobby] listening on 0.0.0.0:${PORT}`);
});
