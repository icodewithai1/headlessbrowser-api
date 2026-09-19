/*  ==============================================================
    /api/browse  —  THE WHOLE BROWSER, as one file.
    Paste into GitHub as:  api/browse.js

    A real headless Chrome runs inside the Vercel function. Every byte
    it touches goes through a wisp relay, so sites see a normal browser
    on a residential egress instead of a datacenter bot.

      GET /api/browse?url=https://example.com          -> the site, rendered,
                                                          every link/asset
                                                          rewritten through here
      GET /api/browse?url=...&raw=1                    -> one asset (image/css/js/…)
      GET /api/browse?url=...&shot=1                   -> PNG screenshot
      GET /api/browse?url=...&json=1                   -> {title, text, links, html}
      GET /api/browse?url=<youtube>&secs=12            -> captures the video
      GET /api/browse?url=<youtube>&format=player      -> plays what it captured

    No package.json needed — dependencies install themselves at runtime.
    ============================================================== */
import http from "http";
import fs from "fs";
import { createRequire } from "module";
import { spawnSync } from "child_process";

export const config = { maxDuration: 60, memory: 1024 };
export const maxDuration = 60;

/* ---------- deps: build-time if present, otherwise install at runtime ---- */
async function deps() {
  try {
    const c = (await import("@sparticuz/chromium")).default;
    const p = (await import("puppeteer-core")).default;
    return { chromium: c.default || c, puppeteer: p.default || p, via: "build" };
  } catch {}
  const dir = "/tmp/pptr";
  if (!fs.existsSync(dir + "/node_modules/puppeteer-core")) {
    spawnSync("npm", ["i", "--prefix", dir, "@sparticuz/chromium", "puppeteer-core", "ws",
      "--no-audit", "--no-fund", "--loglevel=error"], { stdio: "ignore", timeout: 240000 });
  }
  const req = createRequire(dir + "/node_modules/");
  const c = req("@sparticuz/chromium"), p = req("puppeteer-core");
  return { chromium: c.default || c, puppeteer: p.default || p, via: "runtime" };
}

const RELAY = "wss://w2.qwq.sh/ws/";
let _WS = null;
async function getWS() {
  if (_WS) return _WS;
  if (typeof WebSocket !== "undefined") return (_WS = WebSocket);
  try { return (_WS = (await import("ws")).default); } catch {}
  return (_WS = createRequire("/tmp/pptr/node_modules/")("ws"));
}

/* ---------- wisp raw-TCP tunnel (the browser does its own TLS) ----------- */
class RawTunnel {
  constructor(ws, host, port) {
    this.ws = ws; this.chunks = []; this.pend = null; this.done = false; this.acc = Buffer.alloc(0);
    const fin = () => { if (!this.done) { this.done = true; this.chunks.push(null); this.pend && this.pend(); } };
    ws.onclose = fin; ws.onerror = fin;
    ws.onmessage = (ev) => {
      this.acc = Buffer.concat([this.acc, Buffer.from(ev.data)]);
      while (this.acc.length >= 5) {
        const type = this.acc[0], id = this.acc.readUInt32LE(1);
        if (type === 2) { const rest = this.acc.subarray(5); this.acc = Buffer.alloc(0); if (id === 1) { this.chunks.push(rest); this.pend && this.pend(); } return; }
        const need = type === 3 ? 9 : type === 4 ? 6 : 5;
        if (this.acc.length < need) return;
        if (type === 4) fin();
        this.acc = this.acc.subarray(need);
      }
    };
    const c = Buffer.alloc(5 + 3 + Buffer.byteLength(host));
    c.writeUInt8(1, 0); c.writeUInt32LE(1, 1); c.writeUInt8(1, 5); c.writeUInt16LE(port, 6); c.write(host, 8, "utf8");
    ws.send(c);
  }
  async read() { while (!this.chunks.length) { if (this.done) return null; await new Promise((r) => { this.pend = r; }); this.pend = null; } return this.chunks.shift(); }
  write(buf) { const d = Buffer.from(buf); const c = Buffer.alloc(5 + d.length); c.writeUInt8(2, 0); c.writeUInt32LE(1, 1); d.copy(c, 5); try { this.ws.send(c); } catch {} }
  end() { const c = Buffer.alloc(6); c.writeUInt8(4, 0); c.writeUInt32LE(1, 1); c.writeUInt8(0, 5); try { this.ws.send(c); } catch {} }
}

function startProxy() {
  return new Promise((resolve) => {
    const srv = http.createServer((rq, rs) => rs.end());
    srv.on("connect", async (rq, clientSocket) => {
      const [host, p] = (rq.url || "").split(":");
      const port = parseInt(p || "443", 10);
      let ws = null, t = null;
      try {
        const WSImpl = await getWS();
        ws = new WSImpl(RELAY);
        await new Promise((ok, no) => { ws.onopen = ok; ws.onerror = no; setTimeout(() => no(new Error("relay timeout")), 15000); });
        ws.binaryType = "arraybuffer";
        t = new RawTunnel(ws, host, port);
        clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        (async () => { for (;;) { const d = await t.read(); if (d === null) break; try { clientSocket.write(d); } catch { break; } } try { clientSocket.end(); } catch {} })();
      } catch {
        try { clientSocket.end("HTTP/1.1 502 Bad Gateway\r\n\r\n"); } catch {}
        try { t && t.end(); } catch {} try { ws && ws.close(); } catch {}
        return;
      }
      clientSocket.on("data", (d) => { try { t.write(d); } catch {} });
      clientSocket.on("close", () => { try { t.end(); } catch {} try { ws.close(); } catch {} });
      clientSocket.on("error", () => { try { t.end(); } catch {} try { ws.close(); } catch {} });
    });
    srv.listen(0, "127.0.0.1", () => resolve(srv));
  });
}

/* ---------- one warm browser per instance (reused across requests) ------- */
let BROWSER = null, PROXY = null, PORT = 0;

async function ensureBrowser() {
  if (BROWSER) {
    const alive = await Promise.race([
      (async () => { try { const p = await BROWSER.newPage(); await p.close(); return true; } catch { return false; } })(),
      new Promise((r) => setTimeout(() => r(false), 5000)),
    ]);
    if (alive) return { browser: BROWSER, port: PORT };
    try { await BROWSER.close(); } catch {}
    BROWSER = null;
  }
  if (!PROXY) { PROXY = await startProxy(); PORT = PROXY.address().port; }
  const { chromium, puppeteer } = await deps();
  const exePath = process.env.CHROME_PATH || (await chromium.executablePath());
  const own = ["--no-sandbox", "--disable-setuid-sandbox",
    "--autoplay-policy=no-user-gesture-required",
    "--disable-blink-features=AutomationControlled",
    "--proxy-server=127.0.0.1:" + PORT,
    "--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE 127.0.0.1"];
  const tuned = (chromium.args || []).filter((a) => !/^--headless/.test(a)).concat(own);
  const plain = ["--disable-dev-shm-usage", "--disable-gpu"].concat(own);
  let lastErr = null;
  for (const args of [tuned, plain]) {
    try {
      const b = await puppeteer.launch({ args, executablePath: exePath, headless: true,
        defaultViewport: { width: 1280, height: 800 }, protocolTimeout: 180000 });
      const probe = await b.newPage(); await probe.evaluate(() => 1); await probe.close();
      BROWSER = b;
      return { browser: b, port: PORT };
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error("browser failed to start");
}

/* ---------- injectables ------------------------------------------------- */
// lift decoded media straight out of the MediaSource buffers
const CAPTURE_SHIM = `
window.__cap = { v: [], a: [] };
(function () {
  const btoaChunk = (buf) => {
    const bytes = new Uint8Array(buf); let s = "";
    for (let i = 0; i < bytes.length; i += 8192) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192));
    return btoa(s);
  };
  const MS = window.MediaSource || window.WebKitMediaSource;
  if (MS) {
    const add = MS.prototype.addSourceBuffer;
    MS.prototype.addSourceBuffer = function (mime) {
      const sb = add.call(this, mime);
      try { sb.__t = /audio/i.test(mime) ? "a" : "v"; } catch { sb.__t = "v"; }
      return sb;
    };
  }
  const SB = window.SourceBuffer;
  if (SB) {
    const app = SB.prototype.appendBuffer;
    SB.prototype.appendBuffer = function (buf) {
      try { window.__cap[this.__t === "a" ? "a" : "v"].push(btoaChunk(buf)); } catch {}
      return app.call(this, buf);
    };
  }
})();
`;

// rewrite every URL in a rendered page so it all flows back through here
function REWRITE_SHIM(prefix) {
  const ABS = (u) => { try { return new URL(u, location.href).href; } catch { return null; } };
  const skip = (u) => !u || /^(data|blob|javascript|about|mailto|tel|ws|wss):/i.test(u);
  // asset = bytes we need as-is (img/css/js) ; otherwise it's a page to render
  const prox = (u, asset) => { if (skip(u)) return u; const a = ABS(u); return a ? prefix + encodeURIComponent(a) + (asset ? "&raw=1" : "") : u; };
  const cssUrls = (t) => String(t).replace(/url\((['"]?)([^'")]+)\1\)/gi, (m, q, u) => "url(" + q + prox(u, true) + q + ")");
  const ASSET_TAGS = { IMG: 1, SCRIPT: 1, SOURCE: 1, VIDEO: 1, AUDIO: 1, TRACK: 1, INPUT: 1, EMBED: 1, OBJECT: 1 };
  const isAssetEl = (el) =>
    !!ASSET_TAGS[el.tagName] ||
    (el.tagName === "LINK" && /icon|stylesheet|preload|apple-touch/i.test(el.getAttribute("rel") || ""));
  const attrs = ["src", "href", "action", "poster", "data-src", "data-href", "data-original", "srcset"];
  document.querySelectorAll("*").forEach((el) => {
    if (!el.tagName) return;
    const asset = isAssetEl(el);
    for (const a of attrs) {
      if (!el.hasAttribute || !el.hasAttribute(a)) continue;
      const v = el.getAttribute(a);
      if (!v) continue;
      if (a === "srcset") {
        el.setAttribute(a, v.split(",").map((part) => {
          const m = part.trim().match(/^(\S+)(\s+.*)?$/);
          return m ? prox(m[1], true) + (m[2] || "") : part;
        }).join(", "));
      } else if (a === "href" && el.tagName === "LINK" && !asset) continue;
      else el.setAttribute(a, prox(v, asset));
    }
    const st = el.getAttribute && el.getAttribute("style");
    if (st && /url\(/i.test(st)) el.setAttribute("style", cssUrls(st));
  });
  document.querySelectorAll("style").forEach((s) => { s.textContent = cssUrls(s.textContent); });
  document.querySelectorAll("template").forEach((tm) => {
    if (tm.content && tm.content.querySelectorAll) {
      tm.content.querySelectorAll("*").forEach((el) => {
        const asset = isAssetEl(el);
        for (const a of attrs) if (el.hasAttribute && el.hasAttribute(a)) el.setAttribute(a, prox(el.getAttribute(a), asset));
      });
    }
  });
}

// keep JS-driven traffic inside the tunnel too
function RUNTIME_SHIM(prefix) {
  const ABS = (u) => { try { return new URL(u, location.href).href; } catch { return null; } };
  const fix = (u) => {
    if (typeof u !== "string") return u;
    if (!/^(https?:)?\/\//i.test(u)) {
      if (/^\//.test(u)) { try { u = new URL(u, location.href).href; } catch { return u; } }
      else return u;
    }
    const a = ABS(u);
    if (!a) return u;
    if (a.indexOf(prefix) === 0) return u;
    return prefix + encodeURIComponent(a) + "&raw=1";
  };
  const of = window.fetch;
  if (of) window.fetch = function (i, o) {
    try {
      if (typeof i === "string") i = fix(i);
      else if (i && i.url) { try { i = new Request(fix(i.url), i); } catch { i.url = fix(i.url); } }
    } catch {}
    return of.call(window, i, o);
  };
  const XO = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (m, u) {
    try { u = fix(u); } catch {}
    return XO.apply(this, arguments);
  };
}

/* ---------- helpers ----------------------------------------------------- */
function isYT(u) { try { const h = new URL(u).hostname.replace(/^www\./, ""); return /(^|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)$/.test(h); } catch { return false; } }

function toWatchUrl(u) {
  let s = String(u).trim();
  if (!/^https?:/i.test(s)) s = "https://" + s;
  const url = new URL(s);
  const host = url.hostname.replace(/^www\./, "");
  let id = null;
  if (host === "youtu.be") id = url.pathname.slice(1);
  else if (/(^|\.)youtube(-nocookie)?\.com$/.test(host)) {
    id = url.searchParams.get("v");
    if (!id) { const m = url.pathname.match(/\/(?:embed|v|shorts|live)\/([\w-]{6,})/); if (m) id = m[1]; }
  }
  if (!id) throw new Error("not a youtube url");
  return "https://www.youtube.com/watch?v=" + id;
}

// grab one asset with its real content-type, binary safe
async function asset(browser, url) {
  const page = await browser.newPage();
  const client = await page.createCDPSession();
  await client.send("Network.enable");
  let meta = null, reqId = null;
  client.on("Network.responseReceived", (e) => { if (!meta || e.type === "Document") { meta = e.response; reqId = e.requestId; } });
  try { await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 }); } catch {}
  await new Promise((r) => setTimeout(r, 1200));
  let body = Buffer.alloc(0);
  if (reqId) {
    try {
      const r = await client.send("Network.getResponseBody", { requestId: reqId });
      body = r.base64 ? Buffer.from(r.body, "base64") : Buffer.from(r.body, "utf8");
    } catch {}
  }
  await page.close().catch(() => {});
  return { status: (meta && meta.status) || 502, headers: (meta && meta.headers) || {}, body };
}

const withTimeout = (p, ms) => Promise.race([p, new Promise((_, no) => setTimeout(() => no(new Error("timeout")), ms))]);

/* ------------------------------- endpoint ------------------------------- */
export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "*");
  if (req.method === "OPTIONS") return res.status(204).end();

  const q = req.query || {};
  const raw = String(q.url || "");
  if (!raw) return res.status(400).json({ error: "missing ?url= parameter" });
  let target;
  try { target = /^https?:/i.test(raw) ? raw : "https://" + raw; new URL(target); } catch { return res.status(400).json({ error: "bad url" }); }

  const proto = (req.headers["x-forwarded-proto"] || "https").split(",")[0];
  const host = req.headers.host;
  const self = proto + "://" + host + (req.url && req.url.split("?")[0].startsWith("/api/") ? req.url.split("?")[0] : "/api/browse");
  const prefix = self + "?url=";
  const here = (u, extra) => prefix + encodeURIComponent(u) + (extra || "");

  try {
    const { browser } = await ensureBrowser();

    /* ---- one asset ---- */
    if (q.raw) {
      const a = await withTimeout(asset(browser, target), 45000);
      if (!a.body.length) return res.status(a.status || 502).json({ error: "asset empty" });
      let ct = a.headers["content-type"] || a.headers["Content-Type"] || "application/octet-stream";
      let body = a.body;
      if (/text\/css/i.test(ct)) {
        const base = new URL(target);
        const txt = body.toString("utf8").replace(/url\((['"]?)([^'")]+)\1\)/gi, (m, q1, u) => {
          if (/^(data|blob):/i.test(u)) return m;
          let abs; try { abs = new URL(u, base).href; } catch { return m; }
          return "url(" + q1 + here(abs, "&raw=1") + q1 + ")";
        });
        body = Buffer.from(txt, "utf8");
      }
      res.setHeader("Content-Type", ct.split(";")[0]);
      res.setHeader("Cache-Control", "public, max-age=300");
      return res.end(body);
    }

    /* ---- youtube: take the video ---- */
    const wantVideo = q.video || q.format || (isYT(target) && !q.shot && !q.json && !q.html);
    if (wantVideo) {
      let watch;
      try { watch = toWatchUrl(target); } catch { watch = target; }
      const secs = Math.max(4, Math.min(30, parseInt(q.secs || "12", 10) || 12));
      const page = await browser.newPage();
      await page.evaluateOnNewDocument(CAPTURE_SHIM);
      await page.goto(watch, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
      await page.waitForFunction(() => { const v = document.querySelector("video"); return v && v.currentTime > 0.5; }, { timeout: 25000 }).catch(() => {});
      await new Promise((r) => setTimeout(r, secs * 1000));
      const out = await page.evaluate(() => {
        const v = document.querySelector("video");
        return { v: window.__cap.v, a: window.__cap.a, title: document.title, t: v ? v.currentTime : 0, ready: v ? v.readyState : 0 };
      });
      await page.close().catch(() => {});
      const join = (arr) => Buffer.concat((arr || []).map((x) => Buffer.from(x, "base64")));
      const video = join(out.v), audio = join(out.a);
      if (!video.length) return res.status(502).json({ error: "no media decoded", title: out.title, readyState: out.ready });
      const base = here(raw);
      const player = "<!doctype html><html><head><meta charset=utf8><meta name=viewport content='width=device-width,initial-scale=1'><title>" +
        (out.title || "captured").replace(/[<>&]/g, "") + "</title></head>" +
        "<body style='margin:0;background:#000;display:flex;align-items:center;justify-content:center;min-height:100vh'><div style='width:min(96vw,960px)'>" +
        "<video id=v src='" + base + "&format=mp4' controls autoplay playsinline style='width:100%'></video>" +
        "<audio id=a src='" + base + "&format=webm'></audio>" +
        "<script>var v=document.getElementById('v'),a=document.getElementById('a');" +
        "v.addEventListener('play',function(){a.currentTime=v.currentTime;a.play().catch(function(){})});" +
        "v.addEventListener('seeked',function(){a.currentTime=v.currentTime});" +
        "v.addEventListener('pause',function(){a.pause()});" +
        "v.addEventListener('volumechange',function(){a.muted=v.muted});</script></div></body></html>";
      if (q.format === "mp4") { res.setHeader("Content-Type", "video/mp4"); return res.end(video); }
      if (q.format === "webm") { res.setHeader("Content-Type", "audio/webm"); return res.end(audio); }
      if (q.format === "player") { res.setHeader("Content-Type", "text/html"); return res.end(player); }
      return res.json({
        ok: true, title: out.title, seconds: Math.round(out.t), from: watch,
        video: { mime: "video/mp4", bytes: video.length, base64: video.toString("base64") },
        audio: audio.length ? { mime: "audio/webm", bytes: audio.length, base64: audio.toString("base64") } : null,
        urls: { mp4: base + "&format=mp4", webm: base + "&format=webm", player: base + "&format=player" },
      });
    }

    /* ---- render a page ---- */
    const page = await browser.newPage();
    await page.setExtraHTTPHeaders({ "Accept-Language": "en-US,en;q=0.9" });
    await page.goto(target, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
    await page.evaluate(() => 1).catch(() => {});
    await new Promise((r) => setTimeout(r, 2200));
    const info = await page.evaluate(() => ({ title: document.title, url: location.href,
      links: Array.from(document.links || []).slice(0, 60).map((a) => a.href),
      text: (document.body ? document.body.innerText : "").slice(0, 20000) }));

    if (q.json) {
      const html = await page.content();
      await page.close().catch(() => {});
      return res.json({ ok: true, title: info.title, url: info.url, text: info.text, links: info.links, html });
    }

    if (q.shot) {
      const buf = await page.screenshot({ type: "png", fullPage: !!q.full });
      await page.close().catch(() => {});
      res.setHeader("Content-Type", "image/png");
      return res.end(buf);
    }

    await page.evaluate(REWRITE_SHIM, prefix);
    let html = await page.content();
    await page.close().catch(() => {});

    const shim = "<script>(" + RUNTIME_SHIM.toString() + ")(" + JSON.stringify(prefix) + ");</script>";
    html = html.replace(/<head([^>]*)>/i, "<head$1><base href=\"" + info.url.replace(/"/g, "&quot;") + "\">" + shim);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.end(html);
  } catch (e) {
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
}
