require("dotenv").config();
const express = require("express");
const axios = require("axios");
const https = require("https");
const fs = require("fs");
const path = require("path");
const session = require("express-session");

const app = express();
app.use(express.json());

// -------------------- ENV --------------------
const SAP_BASE = process.env.SAP_BASE; // e.g. https://host:50000/b1s/v2
const SAP_COMPANY = process.env.SAP_COMPANY;
const SAP_USER = process.env.SAP_USER;
const SAP_PASS = process.env.SAP_PASS;

const POLL_MS = parseInt(process.env.POLL_MS || "3000", 10);
const PORT = parseInt(process.env.PORT || "8000", 10);
const VERIFY_TLS = String(process.env.VERIFY_TLS || "true").toLowerCase() === "true";
const SESSION_SECRET = process.env.SESSION_SECRET || "dev_secret_change_me";

if (!SAP_BASE || !SAP_COMPANY || !SAP_USER || !SAP_PASS) {
  console.error("Missing env vars: SAP_BASE, SAP_COMPANY, SAP_USER, SAP_PASS");
  process.exit(1);
}

// -------------------- SESSION (user-level memory) --------------------
// This keeps selection/history per user during browsing.
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: true,
    cookie: {
      httpOnly: true,
      // set secure:true if behind HTTPS
      secure: false,
      maxAge: 2 * 60 * 60 * 1000, // 2 hours
    },
  })
);

// -------------------- HTTP CLIENT --------------------
const httpsAgent = new https.Agent({ rejectUnauthorized: VERIFY_TLS });

const http = axios.create({
  httpsAgent,
  timeout: 30000,
  validateStatus: () => true,
});

// -------------------- STATE --------------------
const STATE_FILE = path.join(__dirname, "state.json");
function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, "utf-8");
    const json = JSON.parse(raw);
    return { lastDocEntry: Number(json.lastDocEntry || 0) };
  } catch {
    return { lastDocEntry: 0 };
  }
}
function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}
const state = loadState();

// -------------------- SAP SESSION --------------------
let B1SESSION = null;
let ROUTEID = null;

function resetSession() {
  B1SESSION = null;
  ROUTEID = null;
}

function cookieHeader() {
  const parts = [];
  if (B1SESSION) parts.push(`B1SESSION=${B1SESSION}`);
  if (ROUTEID) parts.push(`ROUTEID=${ROUTEID}`);
  return parts.join("; ");
}

async function sapLogin() {
  const url = `${SAP_BASE}/Login`;
  const resp = await http.post(url, {
    CompanyDB: SAP_COMPANY,
    UserName: SAP_USER,
    Password: SAP_PASS,
  });

  if (resp.status < 200 || resp.status >= 300) {
    throw new Error(`Login failed: HTTP ${resp.status} ${JSON.stringify(resp.data)}`);
  }

  const setCookie = resp.headers["set-cookie"] || [];
  const b1 = setCookie.find((c) => c.startsWith("B1SESSION="));
  const route = setCookie.find((c) => c.startsWith("ROUTEID="));

  if (!b1) throw new Error("Login succeeded but B1SESSION cookie missing");

  B1SESSION = b1.split(";")[0].split("=")[1];
  ROUTEID = route ? route.split(";")[0].split("=")[1] : null;

  sendToAll({ type: "INFO", message: "Logged in to SAP Service Layer" });
}

async function sapGet(relativeUrl) {
  if (!B1SESSION) await sapLogin();

  const url = `${SAP_BASE}${relativeUrl}`;
  const resp = await http.get(url, { headers: { Cookie: cookieHeader() } });

  if (resp.status === 401 || resp.status === 403) {
    resetSession();
    await sapLogin();
    const resp2 = await http.get(url, { headers: { Cookie: cookieHeader() } });

    if (resp2.status < 200 || resp2.status >= 300) {
      throw new Error(`SAP GET failed: HTTP ${resp2.status} ${JSON.stringify(resp2.data)}`);
    }
    return resp2.data;
  }

  if (resp.status < 200 || resp.status >= 300) {
    throw new Error(`SAP GET failed: HTTP ${resp.status} ${JSON.stringify(resp.data)}`);
  }
  return resp.data;
}

// -------------------- SSE --------------------
const clients = new Set();
function sendToAll(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of clients) res.write(data);
}

app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  res.write(`data: ${JSON.stringify({ type: "INFO", message: "Connected" })}\n\n`);
  clients.add(res);
  req.on("close", () => clients.delete(res));
});

// -------------------- CACHE (full invoices) --------------------
const fullInvoiceCache = new Map(); // docEntry -> { data, cachedAt }
const CACHE_TTL_MS = 30 * 60 * 1000; // 30 mins

function cacheSet(docEntry, data) {
  fullInvoiceCache.set(Number(docEntry), { data, cachedAt: Date.now() });
}
function cacheGet(docEntry) {
  const rec = fullInvoiceCache.get(Number(docEntry));
  if (!rec) return null;
  if (Date.now() - rec.cachedAt > CACHE_TTL_MS) {
    fullInvoiceCache.delete(Number(docEntry));
    return null;
  }
  return rec.data;
}

// -------------------- POLLING --------------------
let pollRunning = false;

async function pollInvoices() {
  if (pollRunning) return;
  pollRunning = true;

  try {
    const latest = await sapGet(`/Invoices?$select=DocEntry&$orderby=DocEntry desc&$top=30`);
    const list = (latest.value || []).map((x) => Number(x.DocEntry)).filter(Number.isFinite);

    const newDocEntries = list.filter((d) => d > state.lastDocEntry).sort((a, b) => a - b);
    if (!newDocEntries.length) return;

    for (const docEntry of newDocEntries) {
      const full = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, full);

      sendToAll({
        type: "NEW_INVOICE",
        docEntry,
        docNum: full.DocNum ?? null,
        cardName: full.CardName ?? null,
        docTotal: full.DocTotal ?? null,
      });
    }

    state.lastDocEntry = Math.max(...newDocEntries);
    saveState(state);
  } catch (err) {
    sendToAll({ type: "ERROR", message: err?.message || String(err) });
  } finally {
    pollRunning = false;
  }
}

setInterval(pollInvoices, POLL_MS);
pollInvoices();

// -------------------- API: INVOICES --------------------
app.get("/api/invoices/summary", async (req, res) => {
  try {
    const page = Math.max(parseInt(req.query.page || "1", 10), 1);
    const pageSize = Math.min(Math.max(parseInt(req.query.pageSize || "10", 10), 1), 50);
    const skip = (page - 1) * pageSize;

    const data = await sapGet(
      `/Invoices?$select=DocEntry,DocNum,CardCode,CardName,DocTotal,DocCurrency,DocDate,DocTime,CreationDate,UpdateDate,UpdateTime,DocumentStatus,Cancelled` +
        `&$orderby=DocEntry desc&$top=${pageSize}&$skip=${skip}`
    );

    res.json({ page, pageSize, value: data.value || [] });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

app.get("/api/invoice/:docEntry/raw", async (req, res) => {
  try {
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry)) return res.status(400).json({ error: "Invalid DocEntry" });

    const cached = cacheGet(docEntry);
    if (cached) return res.json({ source: "cache", data: cached });

    const full = await sapGet(`/Invoices(${docEntry})`);
    cacheSet(docEntry, full);
    res.json({ source: "sap", data: full });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- API: GRA (SESSION + single + batch) --------------------
// For now: MOCK submit. We store the submission record in the user's session.
// Later we will replace mockSubmitToGRA() with real HTTP POST to GRA.

function ensureGraSession(req) {
  if (!req.session.gra) {
    req.session.gra = {
      history: [], // {type, docEntry(s), status, ts, message}
      lastSelection: [],
    };
  }
}

async function mockSubmitToGRA(invoiceJson) {
  // placeholder for real GRA call later
  // returning a "fake" response
  return {
    status: "MOCK_OK",
    receivedDocEntry: invoiceJson.DocEntry,
    receivedDocNum: invoiceJson.DocNum,
    timestamp: new Date().toISOString(),
  };
}

app.get("/api/gra/history", (req, res) => {
  ensureGraSession(req);
  res.json(req.session.gra);
});

app.post("/api/gra/select", (req, res) => {
  ensureGraSession(req);
  const docEntries = Array.isArray(req.body.docEntries) ? req.body.docEntries : [];
  const cleaned = docEntries.map(Number).filter(Number.isFinite);
  req.session.gra.lastSelection = cleaned;
  res.json({ ok: true, lastSelection: cleaned });
});

// Single post
app.post("/api/gra/post/single", async (req, res) => {
  ensureGraSession(req);
  try {
    const docEntry = Number(req.body.docEntry);
    if (!Number.isFinite(docEntry)) return res.status(400).json({ error: "docEntry is required" });

    // Get full invoice (cache first)
    let inv = cacheGet(docEntry);
    if (!inv) {
      inv = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, inv);
    }

    const graResp = await mockSubmitToGRA(inv);

    req.session.gra.history.unshift({
      type: "single",
      docEntries: [docEntry],
      status: "MOCK_OK",
      ts: new Date().toISOString(),
      message: `Mock posted invoice DocEntry ${docEntry}`,
      graResp,
    });

    res.json({ ok: true, mode: "single", docEntry, graResp });
  } catch (err) {
    req.session.gra.history.unshift({
      type: "single",
      docEntries: [req.body.docEntry],
      status: "FAILED",
      ts: new Date().toISOString(),
      message: err?.message || String(err),
    });
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// Batch post
app.post("/api/gra/post/batch", async (req, res) => {
  ensureGraSession(req);
  try {
    const docEntries = Array.isArray(req.body.docEntries) ? req.body.docEntries : [];
    const cleaned = docEntries.map(Number).filter(Number.isFinite);
    if (!cleaned.length) return res.status(400).json({ error: "docEntries[] is required" });

    const results = [];
    for (const docEntry of cleaned) {
      let inv = cacheGet(docEntry);
      if (!inv) {
        inv = await sapGet(`/Invoices(${docEntry})`);
        cacheSet(docEntry, inv);
      }
      const graResp = await mockSubmitToGRA(inv);
      results.push({ docEntry, graResp });
    }

    req.session.gra.history.unshift({
      type: "batch",
      docEntries: cleaned,
      status: "MOCK_OK",
      ts: new Date().toISOString(),
      message: `Mock posted ${cleaned.length} invoices`,
      results,
    });

    res.json({ ok: true, mode: "batch", count: cleaned.length, results });
  } catch (err) {
    req.session.gra.history.unshift({
      type: "batch",
      docEntries: req.body.docEntries || [],
      status: "FAILED",
      ts: new Date().toISOString(),
      message: err?.message || String(err),
    });
    res.status(500).json({ error: err?.message || String(err) });
  }
});

// -------------------- UI --------------------

// Live
app.get("/", (req, res) => {
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Live Invoice Monitor</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    .nav a { margin-right: 12px; }
    .muted { color:#666; font-size:12px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 12px; margin: 10px 0; }
    .err { color:#b00020; margin: 10px 0; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/">Live</a>
    <a href="/invoices">Invoices</a>
    <a href="/gra">GRA Posting</a>
  </div>

  <h2>Live Invoice Monitor (Polling)</h2>
  <div class="muted">Polling every ${POLL_MS}ms — detects new DocEntry and caches FULL JSON.</div>

  <div id="log"></div>

  <script>
    const log = document.getElementById("log");
    const es = new EventSource("/events");

    function addError(msg){
      const el = document.createElement("div");
      el.className = "err";
      el.textContent = "⚠ " + msg;
      log.prepend(el);
    }

    es.onmessage = (evt) => {
      const msg = JSON.parse(evt.data);

      if (msg.type === "NEW_INVOICE") {
        const el = document.createElement("div");
        el.className = "card";
        el.innerHTML = \`
          <div><b>New Invoice Detected</b></div>
          <div class="muted">DocEntry: \${msg.docEntry} • DocNum: \${msg.docNum ?? ""}</div>
          <div>Customer: \${msg.cardName ?? ""}</div>
          <div>Total: \${msg.docTotal ?? ""}</div>
          <div style="margin-top:8px;">
            <a href="/invoices#docEntry=\${msg.docEntry}">Open in invoices</a> •
            <a href="/gra#docEntry=\${msg.docEntry}">Post to GRA</a>
          </div>
        \`;
        log.prepend(el);
      }

      if (msg.type === "ERROR") addError(msg.message);
    };
  </script>
</body>
</html>`);
});

// Invoices (list + click full JSON + selection for GRA)
app.get("/invoices", (req, res) => {
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Invoices</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    .nav a { margin-right: 12px; }
    .row { display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin: 14px 0; }
    button, select { padding: 10px 12px; border:1px solid #ddd; border-radius: 10px; background:#fff; cursor:pointer; }
    .muted { color:#666; font-size:12px; }
    .err { color:#b00020; margin: 10px 0; }
    table { width:100%; border-collapse: collapse; margin-top: 12px; }
    th, td { border-bottom:1px solid #eee; padding: 10px 8px; text-align:left; vertical-align:top; }
    th { font-size:12px; color:#666; }
    tr:hover { background:#fafafa; }
    .pill { border:1px solid #eee; padding:6px 10px; border-radius:999px; font-size:12px; background:#fafafa; }
    input[type="checkbox"] { transform: scale(1.1); }

    /* modal */
    .backdrop { position:fixed; inset:0; background:rgba(0,0,0,.5); display:none; align-items:center; justify-content:center; padding:20px; }
    .modal { background:#fff; width:min(1100px, 95vw); max-height:90vh; overflow:auto; border-radius:16px; padding:16px; }
    .modalHead { display:flex; justify-content:space-between; gap:10px; align-items:center; }
    pre { background:#0b1020; color:#e6edf3; padding:12px; border-radius:12px; overflow:auto; }
    .close { border:1px solid #ddd; border-radius:10px; padding:8px 10px; background:#fff; cursor:pointer; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/">Live</a>
    <a href="/invoices">Invoices</a>
    <a href="/gra">GRA Posting</a>
  </div>

  <h2>Invoices</h2>
  <div class="muted">Select invoices (checkbox) then click “Send selection to GRA page”. Click a row to view FULL RAW JSON.</div>

  <div class="row">
    <button id="reload">Reload</button>
    <span class="muted">Page</span>
    <button id="prev">Prev</button>
    <span id="pageNum" class="pill">1</span>
    <button id="next">Next</button>

    <span class="muted">Page size</span>
    <select id="pageSize">
      <option>5</option>
      <option selected>10</option>
      <option>15</option>
      <option>20</option>
      <option>50</option>
    </select>

    <button id="sendToGra">Send selection to GRA page</button>
  </div>

  <div id="error" class="err" style="display:none;"></div>
  <div id="meta" class="muted"></div>
  <div id="tableWrap"></div>

  <!-- modal -->
  <div id="backdrop" class="backdrop">
    <div class="modal">
      <div class="modalHead">
        <div>
          <div id="mTitle" style="font-weight:700;">Invoice</div>
          <div id="mSub" class="muted"></div>
        </div>
        <button class="close" id="closeBtn">Close</button>
      </div>
      <div style="margin-top:12px;">
        <pre id="jsonPre">Loading...</pre>
      </div>
    </div>
  </div>

<script>
  let page = 1;
  let pageSize = 10;
  const selected = new Set();

  const errEl = document.getElementById("error");
  const metaEl = document.getElementById("meta");
  const tableWrap = document.getElementById("tableWrap");
  const pageNumEl = document.getElementById("pageNum");
  const pageSizeEl = document.getElementById("pageSize");

  const backdrop = document.getElementById("backdrop");
  const jsonPre = document.getElementById("jsonPre");
  const mTitle = document.getElementById("mTitle");
  const mSub = document.getElementById("mSub");

  function setErr(msg){
    if (!msg){ errEl.style.display="none"; errEl.textContent=""; return; }
    errEl.style.display="block";
    errEl.textContent = "⚠ " + msg;
  }

  function safe(v){ return (v === null || v === undefined) ? "" : String(v); }
  function d10(x){ return x ? String(x).slice(0,10) : ""; }

  async function fetchJson(url, opts){
    const res = await fetch(url, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Request failed");
    return data;
  }

  async function loadSummary(){
    setErr("");
    pageNumEl.textContent = String(page);

    const data = await fetchJson(\`/api/invoices/summary?page=\${page}&pageSize=\${pageSize}\`);
    const rows = data.value || [];

    metaEl.textContent = rows.length ? \`Loaded \${rows.length} invoices • Selected: \${selected.size}\` : "No invoices";

    const html = \`
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>DocEntry</th>
            <th>DocNum</th>
            <th>Customer</th>
            <th>Total</th>
            <th>Date/Time</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          \${rows.map(r => {
            const checked = selected.has(Number(r.DocEntry)) ? "checked" : "";
            return \`
              <tr data-docentry="\${r.DocEntry}">
                <td><input type="checkbox" data-check="\${r.DocEntry}" \${checked}/></td>
                <td>\${r.DocEntry}</td>
                <td>\${safe(r.DocNum)}</td>
                <td>\${safe(r.CardCode)} - \${safe(r.CardName)}</td>
                <td>\${safe(r.DocTotal)} \${safe(r.DocCurrency)}</td>
                <td>
                  \${d10(r.DocDate)} \${safe(r.DocTime)}<br>
                  <span class="muted">Created: \${d10(r.CreationDate)} • Updated: \${safe(r.UpdateTime)}</span>
                </td>
                <td>\${safe(r.DocumentStatus)} \${safe(r.Cancelled)}</td>
              </tr>
            \`;
          }).join("")}
        </tbody>
      </table>
    \`;

    tableWrap.innerHTML = html;

    // Jump to docEntry if provided in hash
    const hash = new URLSearchParams((location.hash || "").replace("#",""));
    const docEntry = hash.get("docEntry");
    if (docEntry){
      openInvoice(Number(docEntry));
      location.hash = "";
    }
  }

  async function openInvoice(docEntry){
    backdrop.style.display = "flex";
    jsonPre.textContent = "Loading full invoice JSON...";
    mTitle.textContent = "Invoice DocEntry: " + docEntry;
    mSub.textContent = "";

    try{
      const result = await fetchJson(\`/api/invoice/\${docEntry}/raw\`);
      const inv = result.data;
      mSub.textContent = \`Source: \${result.source} • DocNum: \${safe(inv.DocNum)} • Customer: \${safe(inv.CardName)}\`;
      jsonPre.textContent = JSON.stringify(inv, null, 2);
    }catch(e){
      jsonPre.textContent = "Failed: " + String(e.message || e);
    }
  }

  // checkbox handling
  tableWrap.addEventListener("change", (e) => {
    const cb = e.target.closest("input[data-check]");
    if (!cb) return;
    const docEntry = Number(cb.getAttribute("data-check"));
    if (!Number.isFinite(docEntry)) return;
    if (cb.checked) selected.add(docEntry);
    else selected.delete(docEntry);
    metaEl.textContent = \`Selected: \${selected.size}\`;
    e.stopPropagation();
  });

  // click row (but ignore checkbox column) -> open modal
  tableWrap.addEventListener("click", (e) => {
    if (e.target.closest("input[data-check]")) return;
    const tr = e.target.closest("tr[data-docentry]");
    if (!tr) return;
    openInvoice(Number(tr.getAttribute("data-docentry")));
  });

  document.getElementById("closeBtn").addEventListener("click", () => backdrop.style.display = "none");
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) backdrop.style.display = "none"; });

  document.getElementById("reload").addEventListener("click", loadSummary);
  document.getElementById("prev").addEventListener("click", () => { if (page > 1){ page--; loadSummary(); } });
  document.getElementById("next").addEventListener("click", () => { page++; loadSummary(); });

  pageSizeEl.addEventListener("change", () => {
    pageSize = parseInt(pageSizeEl.value, 10);
    page = 1;
    loadSummary();
  });

  document.getElementById("sendToGra").addEventListener("click", async () => {
    try{
      const docEntries = Array.from(selected);
      await fetchJson("/api/gra/select", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ docEntries })
      });
      location.href = "/gra";
    }catch(e){
      setErr(e.message || String(e));
    }
  });

  loadSummary();
</script>
</body>
</html>`);
});

// GRA PAGE (single + batch) using session selection/history
app.get("/gra", (req, res) => {
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>GRA Posting</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    .nav a { margin-right: 12px; }
    .grid { display:grid; grid-template-columns: 1fr 1fr; gap:16px; margin-top: 14px; }
    @media (max-width: 900px){ .grid { grid-template-columns: 1fr; } }
    .card { border:1px solid #ddd; border-radius: 14px; padding: 14px; }
    input, button { padding: 10px 12px; border:1px solid #ddd; border-radius: 10px; }
    button { background:#fff; cursor:pointer; }
    .muted { color:#666; font-size:12px; }
    .err { color:#b00020; margin: 10px 0; }
    pre { background:#0b1020; color:#e6edf3; padding:12px; border-radius:12px; overflow:auto; }
    .pill { border:1px solid #eee; padding:6px 10px; border-radius:999px; font-size:12px; background:#fafafa; display:inline-block; }
  </style>
</head>
<body>
  <div class="nav">
    <a href="/">Live</a>
    <a href="/invoices">Invoices</a>
    <a href="/gra">GRA Posting</a>
  </div>

  <h2>GRA Posting (Session-based)</h2>
  <div class="muted">This is ready for real GRA integration later. For now it does MOCK post and stores history in your session.</div>

  <div id="error" class="err" style="display:none;"></div>

  <div class="grid">
    <div class="card">
      <h3>Single Post</h3>
      <div class="muted">Post one invoice by DocEntry.</div>
      <div style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap;">
        <input id="singleDocEntry" placeholder="DocEntry e.g. 111" />
        <button id="singleBtn">Mock Post Single</button>
      </div>
      <div style="margin-top:10px;" class="muted">
        Tip: you can click an invoice on the invoices page and copy the DocEntry.
      </div>
      <pre id="singleOut">Waiting...</pre>
    </div>

    <div class="card">
      <h3>Multi Post (Batch)</h3>
      <div class="muted">Uses your selection from the invoices page.</div>
      <div style="margin-top:10px;">
        <div class="pill">Selected DocEntries:</div>
        <div id="selList" class="muted" style="margin-top:6px;"></div>
      </div>
      <div style="margin-top:10px;">
        <button id="batchBtn">Mock Post Selected (Batch)</button>
      </div>
      <pre id="batchOut">Waiting...</pre>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <h3>Session Posting History</h3>
    <div class="muted">Shows what you posted in this browser session.</div>
    <pre id="histOut">Loading...</pre>
  </div>

<script>
  const errEl = document.getElementById("error");
  const singleOut = document.getElementById("singleOut");
  const batchOut = document.getElementById("batchOut");
  const histOut = document.getElementById("histOut");
  const selList = document.getElementById("selList");
  const singleDocEntry = document.getElementById("singleDocEntry");

  function setErr(msg){
    if (!msg){ errEl.style.display="none"; errEl.textContent=""; return; }
    errEl.style.display="block";
    errEl.textContent = "⚠ " + msg;
  }

  async function fetchJson(url, opts){
    const res = await fetch(url, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Request failed");
    return data;
  }

  async function loadSession(){
    const s = await fetchJson("/api/gra/history");
    const sel = (s.lastSelection || []);
    selList.textContent = sel.length ? sel.join(", ") : "(none — go to Invoices and select some)";
    histOut.textContent = JSON.stringify(s.history || [], null, 2);

    // if URL hash has docEntry, auto fill single
    const hash = new URLSearchParams((location.hash || "").replace("#",""));
    const de = hash.get("docEntry");
    if (de){
      singleDocEntry.value = de;
      location.hash = "";
    }
  }

  document.getElementById("singleBtn").addEventListener("click", async () => {
    try{
      setErr("");
      singleOut.textContent = "Posting...";
      const docEntry = Number(singleDocEntry.value);
      const resp = await fetchJson("/api/gra/post/single", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ docEntry })
      });
      singleOut.textContent = JSON.stringify(resp, null, 2);
      await loadSession();
    }catch(e){
      setErr(e.message || String(e));
      singleOut.textContent = "Failed.";
      await loadSession();
    }
  });

  document.getElementById("batchBtn").addEventListener("click", async () => {
    try{
      setErr("");
      batchOut.textContent = "Posting batch...";
      const s = await fetchJson("/api/gra/history");
      const docEntries = s.lastSelection || [];
      const resp = await fetchJson("/api/gra/post/batch", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ docEntries })
      });
      batchOut.textContent = JSON.stringify(resp, null, 2);
      await loadSession();
    }catch(e){
      setErr(e.message || String(e));
      batchOut.textContent = "Failed.";
      await loadSession();
    }
  });

  loadSession();
</script>
</body>
</html>`);
});

// -------------------- START --------------------
app.listen(PORT, () => console.log(`Running on http://localhost:${PORT}`));