/**
 * server.js
 * -------------------------------------------------------------------
 * SAP B1 Service Layer:
 *  - Polls new invoices (by DocEntry) and caches FULL JSON
 *  - Invoices list page + Full invoice page (shows EVERYTHING)
 *
 * GRA E-VAT (VER 8.2 style):
 *  - Maps SAP invoice JSON -> GRA invoice payload
 *  - Can POST to staging/real endpoint (when enabled) OR mock
 *
 * UI:
 *  - Professional, clean admin UI (no gradients, no “AI gimmicks”)
 *  - Nice, user-friendly error messages
 *  - Invoice “GRA template” view + collapsible Raw JSON
 *  - Remove selected invoices on GRA page before posting
 * -------------------------------------------------------------------
 */

require("dotenv").config();
const express = require("express");
const axios = require("axios");
const https = require("https");
const fs = require("fs");
const path = require("path");
const session = require("express-session");

const app = express();
app.use(express.json({ limit: "2mb" }));

// -------------------- ENV (SAP) --------------------
const SAP_BASE = process.env.SAP_BASE; // e.g. https://host:50000/b1s/v2
const SAP_COMPANY = process.env.SAP_COMPANY;
const SAP_USER = process.env.SAP_USER;
const SAP_PASS = process.env.SAP_PASS;

const POLL_MS = parseInt(process.env.POLL_MS || "5000", 10);
const PORT = parseInt(process.env.PORT || "8000", 10);
const VERIFY_TLS = String(process.env.VERIFY_TLS || "true").toLowerCase() === "true";
const SESSION_SECRET = process.env.SESSION_SECRET || "dev_secret_change_me";

// -------------------- ENV (GRA / E-VAT) --------------------
const GRA_ENABLED = String(process.env.GRA_ENABLED || "false").toLowerCase() === "true";
const GRA_BASE = process.env.GRA_BASE || "https://vsdcstaging.vat-gh.com";
const GRA_INVOICE_URL = process.env.GRA_INVOICE_URL || "";
const GRA_TAXPAYER_CODE = process.env.GRA_TAXPAYER_CODE || ""; // e.g. CXX000000YY-001
const GRA_SECURITY_KEY = process.env.GRA_SECURITY_KEY || "";

// Defaults used in mapping when SAP fields are missing
const GRA_USER_NAME = process.env.GRA_USER_NAME || "SAP User";
const GRA_FLAG = process.env.GRA_FLAG || "INVOICE";
const GRA_CALCULATION_TYPE = process.env.GRA_CALCULATION_TYPE || "INCLUSIVE";
const GRA_SALE_TYPE = process.env.GRA_SALE_TYPE || "NORMAL";
const GRA_DISCOUNT_TYPE = process.env.GRA_DISCOUNT_TYPE || "GENERAL";
const GRA_TAX_TYPE = process.env.GRA_TAX_TYPE || "FLAT";
const GRA_DEFAULT_BP_TIN = process.env.GRA_DEFAULT_BP_TIN || "C0000000000";

// Basic checks
if (!SAP_BASE || !SAP_COMPANY || !SAP_USER || !SAP_PASS) {
  console.error("Missing env vars: SAP_BASE, SAP_COMPANY, SAP_USER, SAP_PASS");
  process.exit(1);
}
if (GRA_ENABLED) {
  const okUrl = !!GRA_INVOICE_URL || (!!GRA_BASE && !!GRA_TAXPAYER_CODE);
  if (!okUrl) {
    console.error("GRA_ENABLED=true but missing GRA_INVOICE_URL or (GRA_BASE + GRA_TAXPAYER_CODE)");
    process.exit(1);
  }
  if (!GRA_SECURITY_KEY) {
    console.error("GRA_ENABLED=true but missing GRA_SECURITY_KEY");
    process.exit(1);
  }
}

// -------------------- SESSION --------------------
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: true,
    cookie: {
      httpOnly: true,
      secure: false,
      maxAge: 2 * 60 * 60 * 1000,
    },
  })
);

// -------------------- HTTP CLIENTS --------------------
const httpsAgent = new https.Agent({ rejectUnauthorized: VERIFY_TLS });

const httpSAP = axios.create({
  httpsAgent,
  timeout: 45000,
  validateStatus: () => true,
});

const httpGRA = axios.create({
  httpsAgent,
  timeout: 60000,
  validateStatus: () => true,
});

// -------------------- USER-FRIENDLY ERROR HELPERS --------------------
function normalizeError(err) {
  // Default
  const out = {
    title: "Something went wrong",
    message: "We couldn’t complete the request. Please try again.",
    detail: "",
    hint: "",
    code: "",
  };

  const raw = err?.message ? String(err.message) : String(err || "");
  out.detail = raw;

  // SAP common
  if (raw.includes("Login failed")) {
    out.title = "SAP login failed";
    out.message = "We couldn’t sign in to SAP Service Layer.";
    out.hint = "Check SAP_COMPANY, SAP_USER, SAP_PASS, and SAP_BASE in your .env file.";
    return out;
  }
  if (raw.includes("SAP GET failed")) {
    out.title = "SAP request failed";
    out.message = "SAP didn’t return the data we requested.";
    if (raw.includes("HTTP 404")) {
      out.hint = "That invoice may not exist (wrong DocEntry), or your user has no permission.";
    } else if (raw.includes("HTTP 401") || raw.includes("HTTP 403")) {
      out.hint = "Your SAP session expired or permission was denied. Try again.";
    } else {
      out.hint = "Check SAP Service Layer is reachable and your account has access.";
    }
    return out;
  }

  // GRA common
  if (raw.includes("GRA invoice URL not configured")) {
    out.title = "GRA endpoint not configured";
    out.message = "Posting is not configured yet.";
    out.hint = "Set GRA_TAXPAYER_CODE (or GRA_INVOICE_URL) and GRA_SECURITY_KEY in .env.";
    return out;
  }
  if (raw.includes("GRA POST failed")) {
    out.title = "GRA posting failed";
    out.message = "The invoice could not be sent to GRA E-VAT.";
    out.hint = "Confirm Security Key, Taxpayer code, and that your payload mapping matches GRA fields.";
    return out;
  }

  return out;
}

function sendApiError(res, err, status = 500) {
  const nice = normalizeError(err);
  res.status(status).json({
    error: nice.message,
    userMessage: nice.message,
    userTitle: nice.title,
    hint: nice.hint,
    code: nice.code,
    detail: nice.detail,
  });
}

// -------------------- STATE (global) --------------------
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

function resetSapSession() {
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
  const resp = await httpSAP.post(url, {
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

  sendToAll({ type: "INFO", message: "Connected to SAP Service Layer." });
}

async function sapGet(relativeUrl) {
  if (!B1SESSION) await sapLogin();

  const url = `${SAP_BASE}${relativeUrl}`;
  const resp = await httpSAP.get(url, { headers: { Cookie: cookieHeader() } });

  if (resp.status === 401 || resp.status === 403) {
    resetSapSession();
    await sapLogin();
    const resp2 = await httpSAP.get(url, { headers: { Cookie: cookieHeader() } });
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
  res.write(`data: ${JSON.stringify({ type: "INFO", message: "Live feed connected." })}\n\n`);

  clients.add(res);
  req.on("close", () => clients.delete(res));
});

// -------------------- CACHE (full invoices) --------------------
const fullInvoiceCache = new Map();
const CACHE_TTL_MS = 30 * 60 * 1000;

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
    const latest = await sapGet(`/Invoices?$select=DocEntry&$orderby=DocEntry desc&$top=50`);
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
        docCurrency: full.DocCurrency ?? null,
      });
    }

    state.lastDocEntry = Math.max(...newDocEntries);
    saveState(state);
  } catch (err) {
    const nice = normalizeError(err);
    sendToAll({ type: "ERROR", message: `${nice.title}: ${nice.message}` });
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
      `/Invoices?$select=DocEntry,DocNum,CardCode,CardName,DocTotal,DocCurrency,DocRate,DocDate,DocTime,TaxDate,CreationDate,UpdateDate,UpdateTime,DocumentStatus,Cancelled,NumAtCard,Comments,Reference1,VatSum` +
        `&$orderby=DocEntry desc&$top=${pageSize}&$skip=${skip}`
    );

    res.json({ page, pageSize, value: data.value || [] });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

app.get("/api/invoice/:docEntry/raw", async (req, res) => {
  try {
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);

    const cached = cacheGet(docEntry);
    if (cached) return res.json({ source: "cache", data: cached });

    const full = await sapGet(`/Invoices(${docEntry})`);
    cacheSet(docEntry, full);
    res.json({ source: "sap", data: full });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

// -------------------- GRA SESSION (selection/history) --------------------
function ensureGraSession(req) {
  if (!req.session.gra) {
    req.session.gra = { history: [], lastSelection: [] };
  }
}

app.get("/api/gra/history", (req, res) => {
  ensureGraSession(req);
  res.json({
    enabled: GRA_ENABLED,
    invoiceUrl: getGraInvoiceUrl(),
    lastSelection: req.session.gra.lastSelection || [],
    history: req.session.gra.history || [],
  });
});

app.post("/api/gra/select", (req, res) => {
  ensureGraSession(req);
  const docEntries = Array.isArray(req.body.docEntries) ? req.body.docEntries : [];
  const cleaned = docEntries.map(Number).filter((n) => Number.isFinite(n) && n > 0);
  req.session.gra.lastSelection = cleaned;
  res.json({ ok: true, lastSelection: cleaned });
});

app.post("/api/gra/remove", (req, res) => {
  ensureGraSession(req);
  const docEntry = Number(req.body.docEntry);
  if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);
  req.session.gra.lastSelection = (req.session.gra.lastSelection || []).filter((d) => Number(d) !== docEntry);
  res.json({ ok: true, lastSelection: req.session.gra.lastSelection });
});

// -------------------- MAPPING: SAP -> GRA E-VAT payload --------------------
function round2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round((x + Number.EPSILON) * 100) / 100;
}
function safeStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}
function getLineLevy(line, keyCandidates) {
  for (const k of keyCandidates) {
    if (line && Object.prototype.hasOwnProperty.call(line, k) && Number.isFinite(Number(line[k]))) return round2(line[k]);
  }
  return 0;
}

function mapSapInvoiceToGra(inv) {
  const currency = inv.DocCurrency || "GHS";
  const exchangeRate = Number.isFinite(Number(inv.DocRate)) ? Number(inv.DocRate) : 1.0;
  const invoiceNumber = safeStr(inv.DocNum || inv.DocEntry);
  const transactionDate = safeStr(inv.DocDate || inv.TaxDate || inv.CreationDate) || new Date().toISOString();

  const businessPartnerName = safeStr(inv.CardName) || "cash customer";
  const businessPartnerTin = safeStr(inv.U_TIN || inv.U_BP_TIN || inv.BusinessPartnerTin) || GRA_DEFAULT_BP_TIN;

  const totalAmount = Number.isFinite(Number(inv.DocTotal)) ? Number(inv.DocTotal) : 0.0;
  const totalVat = Number.isFinite(Number(inv.VatSum)) ? Number(inv.VatSum) : 0.0;

  const lines = Array.isArray(inv.DocumentLines) ? inv.DocumentLines : [];
  const items = lines.map((ln) => {
    const unitPrice = Number.isFinite(Number(ln.UnitPrice)) ? Number(ln.UnitPrice) : Number(ln.Price) || 0.0;

    const levyAmountA = getLineLevy(ln, ["U_LEVY_A", "U_NHIL", "U_NHIL_LEVY", "U_LevyA"]);
    const levyAmountB = getLineLevy(ln, ["U_LEVY_B", "U_GETFUND", "U_GETFUND_LEVY", "U_LevyB"]);
    const levyAmountC = getLineLevy(ln, ["U_LEVY_C", "U_COVID", "U_COVID_LEVY", "U_LevyC"]);
    const levyAmountD = getLineLevy(ln, ["U_LEVY_D", "U_CST", "U_CST_LEVY", "U_LevyD"]);
    const levyAmountE = getLineLevy(ln, ["U_LEVY_E", "U_TOURISM", "U_TOURISM_LEVY", "U_LevyE"]);

    return {
      itemCode: safeStr(ln.ItemCode) || safeStr(ln.U_ItemCode) || "",
      itemCategory: safeStr(ln.U_ItemCategory) || "",
      expireDate: ln.U_ExpireDate ? safeStr(ln.U_ExpireDate) : null,
      description: safeStr(ln.ItemDescription) || "",
      quantity: Number.isFinite(Number(ln.Quantity)) ? Number(ln.Quantity) : 0.0,
      levyAmountA,
      levyAmountB,
      levyAmountC,
      levyAmountD,
      levyAmountE,
      discountAmount: 0.0,
      exciseAmount: Number.isFinite(Number(ln.U_ExciseAmount)) ? Number(ln.U_ExciseAmount) : 0.0,
      batchCode: safeStr(ln.BatchCode || ln.U_BatchCode || ""),
      unitPrice,
    };
  });

  const totalLevy = round2(
    items.reduce((sum, it) => {
      return (
        sum +
        Number(it.levyAmountA || 0) +
        Number(it.levyAmountB || 0) +
        Number(it.levyAmountC || 0) +
        Number(it.levyAmountD || 0) +
        Number(it.levyAmountE || 0)
      );
    }, 0)
  );

  const totalExciseAmount = round2(items.reduce((sum, it) => sum + Number(it.exciseAmount || 0), 0));
  const purchaseOrderReference = safeStr(inv.NumAtCard || "");

  return {
    currency,
    exchangeRate,
    invoiceNumber,
    totalLevy,
    userName: GRA_USER_NAME,
    flag: GRA_FLAG,
    calculationType: GRA_CALCULATION_TYPE,
    totalVat: round2(totalVat),
    transactionDate,
    totalAmount: round2(totalAmount),
    totalExciseAmount: round2(totalExciseAmount),
    voucherAmount: 0.0,
    businessPartnerName,
    businessPartnerTin,
    saleType: GRA_SALE_TYPE,
    discountType: GRA_DISCOUNT_TYPE,
    discountAmount: 0.0,
    reference: "",
    groupReferenceId: "",
    purchaseOrderReference,
    taxType: GRA_TAX_TYPE,
    items,
  };
}

function getGraInvoiceUrl() {
  if (GRA_INVOICE_URL) return GRA_INVOICE_URL;
  if (!GRA_BASE || !GRA_TAXPAYER_CODE) return "";
  return `${GRA_BASE.replace(/\/+$/, "")}/vsdc/api/v1/taxpayer/${encodeURIComponent(GRA_TAXPAYER_CODE)}/invoice`;
}

app.get("/api/gra/payload/:docEntry", async (req, res) => {
  try {
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);

    let inv = cacheGet(docEntry);
    if (!inv) {
      inv = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, inv);
    }

    const payload = mapSapInvoiceToGra(inv);
    res.json({ ok: true, docEntry, payload });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

// -------------------- REAL (or mock) POST TO GRA --------------------
async function submitToGRA(payload) {
  const url = getGraInvoiceUrl();
  if (!url) throw new Error("GRA invoice URL not configured");

  if (!GRA_ENABLED) {
    return {
      mode: "MOCK",
      status: "MOCK_OK",
      invoiceUrl: url,
      timestamp: new Date().toISOString(),
      note:
        "Posting is currently in mock mode. To post for real, set GRA_ENABLED=true and configure GRA_SECURITY_KEY and GRA_TAXPAYER_CODE (or GRA_INVOICE_URL).",
    };
  }

  const resp = await httpGRA.post(url, payload, {
    headers: {
      security_key: GRA_SECURITY_KEY,
      "Content-Type": "application/json",
    },
  });

  if (resp.status < 200 || resp.status >= 300) {
    throw new Error(`GRA POST failed: HTTP ${resp.status} ${JSON.stringify(resp.data)}`);
  }

  return { mode: "REAL", httpStatus: resp.status, data: resp.data };
}

app.post("/api/gra/post/single", async (req, res) => {
  ensureGraSession(req);
  try {
    const docEntry = Number(req.body.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("DocEntry must be a number"), 400);

    let inv = cacheGet(docEntry);
    if (!inv) {
      inv = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, inv);
    }

    const payload = mapSapInvoiceToGra(inv);
    const graResp = await submitToGRA(payload);

    req.session.gra.history.unshift({
      type: "single",
      docEntries: [docEntry],
      status: "SUCCESS",
      ts: new Date().toISOString(),
      invoiceUrl: getGraInvoiceUrl(),
      mode: graResp.mode,
      payloadPreview: {
        invoiceNumber: payload.invoiceNumber,
        businessPartnerName: payload.businessPartnerName,
        totalAmount: payload.totalAmount,
        totalVat: payload.totalVat,
        totalLevy: payload.totalLevy,
        currency: payload.currency,
        itemsCount: payload.items?.length || 0,
      },
      graResp,
    });

    res.json({ ok: true, mode: "single", docEntry, payload, graResp });
  } catch (err) {
    req.session.gra.history.unshift({
      type: "single",
      docEntries: [req.body.docEntry],
      status: "FAILED",
      ts: new Date().toISOString(),
      message: err?.message || String(err),
    });
    sendApiError(res, err, 500);
  }
});

app.post("/api/gra/post/batch", async (req, res) => {
  ensureGraSession(req);
  try {
    const docEntries = Array.isArray(req.body.docEntries) ? req.body.docEntries : [];
    const cleaned = docEntries.map(Number).filter((n) => Number.isFinite(n) && n > 0);
    if (!cleaned.length) return sendApiError(res, new Error("Select at least one invoice to post."), 400);

    const results = [];
    for (const docEntry of cleaned) {
      let inv = cacheGet(docEntry);
      if (!inv) {
        inv = await sapGet(`/Invoices(${docEntry})`);
        cacheSet(docEntry, inv);
      }
      const payload = mapSapInvoiceToGra(inv);
      const graResp = await submitToGRA(payload);
      results.push({ docEntry, payload, graResp });
    }

    req.session.gra.history.unshift({
      type: "batch",
      docEntries: cleaned,
      status: "SUCCESS",
      ts: new Date().toISOString(),
      invoiceUrl: getGraInvoiceUrl(),
      mode: results[0]?.graResp?.mode || (GRA_ENABLED ? "REAL" : "MOCK"),
      message: `${GRA_ENABLED ? "Posted" : "Mock posted"} ${cleaned.length} invoices`,
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
    sendApiError(res, err, 500);
  }
});

// -------------------- UI (Professional Admin Shell) --------------------
function uiCss() {
  return `
  :root{
    --bg:#f5f7fb;
    --card:#ffffff;
    --text:#0f172a;
    --muted:#64748b;
    --border:#e2e8f0;
    --shadow: 0 10px 25px rgba(15, 23, 42, 0.06);
    --radius:14px;

    --primary:#1d4ed8;
    --primaryHover:#1e40af;
    --success:#16a34a;
    --warning:#d97706;
    --danger:#dc2626;

    --rowHover:#f8fafc;
    --chipBg:#f1f5f9;
    --chipText:#334155;

    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
  }
  *{ box-sizing:border-box; }
  html, body{ height:100%; }
  body{ margin:0; font-family: var(--sans); background: var(--bg); color: var(--text); }
  a{ color: var(--primary); text-decoration:none; }
  a:hover{ text-decoration:underline; }

  .layout{ min-height:100vh; display:grid; grid-template-columns: 260px 1fr; }
  @media (max-width: 980px){
    .layout{ grid-template-columns: 1fr; }
    .sidebar{ position: sticky; top:0; z-index:20; border-right:none; border-bottom:1px solid var(--border); }
    .navList{ display:flex; gap:10px; flex-wrap:wrap; }
    .navItem{ flex: 0 0 auto; }
  }

  .sidebar{
    background:#0b1220;
    color:#e5e7eb;
    border-right: 1px solid rgba(226,232,240,.08);
    padding: 18px 16px;
  }
  .brand{ display:flex; align-items:center; gap:10px; padding: 10px 10px 14px; border-bottom: 1px solid rgba(226,232,240,.10); margin-bottom: 14px; }
  .mark{ width:34px; height:34px; border-radius: 10px; background: rgba(255,255,255,.10); display:flex; align-items:center; justify-content:center; font-weight:800; letter-spacing:.3px; }
  .brandName{ font-weight:800; line-height:1.1; }
  .brandSub{ font-size:12px; color: rgba(229,231,235,.70); margin-top:2px; }

  .navList{ display:flex; flex-direction:column; gap:8px; }
  .navItem{
    display:flex; align-items:center; justify-content:space-between;
    padding: 10px 12px; border-radius: 12px; color:#e5e7eb;
    background: transparent; border: 1px solid rgba(226,232,240,.10);
  }
  .navItem:hover{ background: rgba(255,255,255,.06); text-decoration:none; }
  .navItem.active{ background: rgba(29,78,216,.25); border-color: rgba(59,130,246,.35); }
  .navHint{ font-size:12px; color: rgba(229,231,235,.65); }

  .content{ padding: 22px 22px 50px; }
  .topbar{
    background: var(--card); border: 1px solid var(--border); border-radius: var(--radius);
    box-shadow: var(--shadow); padding: 14px 16px;
    display:flex; align-items:center; justify-content:space-between; gap:12px;
    position: sticky; top: 12px; z-index: 10;
  }
  .topTitle{ font-size: 16px; font-weight: 800; }
  .topMeta{ color: var(--muted); font-size: 12px; }
  .page{ margin-top: 14px; }

  .card{ background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: var(--shadow); padding: 16px; }
  .row{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .grid{ display:grid; gap:14px; }
  .grid2{ grid-template-columns: 1fr 1fr; }
  @media (max-width: 1100px){ .grid2{ grid-template-columns: 1fr; } }

  .chip{
    display:inline-flex; align-items:center; gap:8px;
    padding: 7px 10px; border-radius: 999px;
    background: var(--chipBg); color: var(--chipText);
    font-size: 12px; border: 1px solid var(--border);
  }

  button, select, input{
    padding: 10px 12px; border: 1px solid var(--border);
    border-radius: 12px; background: #fff; color: var(--text);
    outline: none; font-family: var(--sans);
  }
  input::placeholder{ color:#94a3b8; }
  button{ cursor:pointer; }
  button:hover{ background:#f8fafc; }
  button:active{ transform: translateY(1px); }

  .btnPrimary{ background: var(--primary); color:#fff; border-color: var(--primary); }
  .btnPrimary:hover{ background: var(--primaryHover); }

  .btnSuccess{ background: var(--success); color:#fff; border-color: var(--success); }
  .btnDanger{ background: var(--danger); color:#fff; border-color: var(--danger); }

  .btnWarn{ background: #fffbeb; color: #92400e; border-color: #fde68a; }
  .btnWarn:hover{ background:#fff7ed; }

  /* Friendly alert box */
  .alert{
    display:flex; gap:12px; align-items:flex-start;
    padding: 12px 12px; border-radius: 12px;
    border:1px solid var(--border); background:#fff;
  }
  .alertIcon{
    width:34px; height:34px; border-radius: 10px;
    display:flex; align-items:center; justify-content:center;
    font-weight:900;
  }
  .alertError{ border-color:#fecaca; background:#fff; }
  .alertError .alertIcon{ background:#fee2e2; color:#7f1d1d; }
  .alertTitle{ font-weight:900; }
  .alertMsg{ color: var(--text); margin-top:3px; }
  .alertHint{ color: var(--muted); font-size:12px; margin-top:6px; }

  table{
    width:100%; border-collapse: separate; border-spacing: 0;
    border: 1px solid var(--border); border-radius: 12px;
    overflow:hidden; background: #fff;
  }
  thead th{
    text-align:left; font-size:12px; color:#64748b;
    padding: 12px 12px; background: #f8fafc;
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 1;
  }
  tbody td{ padding: 12px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }
  tbody tr:hover{ background: var(--rowHover); cursor:pointer; }
  tbody tr:last-child td{ border-bottom:none; }

  .badge{ display:inline-flex; align-items:center; padding: 6px 10px; border-radius: 999px; font-size: 12px; border:1px solid var(--border); background:#fff; }
  .bOpen{ color:#065f46; background:#dcfce7; border-color:#bbf7d0; }
  .bClosed{ color:#1f2937; background:#f1f5f9; border-color:#e2e8f0; }
  .bCancelled{ color:#7f1d1d; background:#fee2e2; border-color:#fecaca; }

  pre{
    background:#0b1020; color:#e6edf3; padding: 12px; border-radius: 12px;
    overflow:auto; max-height: 75vh; font-family: var(--mono); font-size: 12px; line-height: 1.45;
  }
  .noClamp{ white-space: pre-wrap; word-break: break-word; }

  details{ border:1px solid var(--border); border-radius: 12px; padding: 10px 12px; background:#fff; margin: 8px 0; }
  summary{ cursor:pointer; font-weight: 700; }

  .kv{ display:grid; grid-template-columns: 280px 1fr; gap:10px; padding: 8px 0; border-bottom: 1px dashed #e2e8f0; }
  .kv:last-child{ border-bottom:none; }
  .k{ color:#64748b; font-size: 12px; word-break: break-all; }
  .v{ color:#0f172a; word-break: break-word; }

  /* GRA-like Invoice Template */
  .paper{
    background:#fff;
    border:1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
  }
  .paperHeader{
    text-align:center;
    font-weight:900;
    letter-spacing:.3px;
  }
  .paperSub{
    text-align:center;
    margin-top:6px;
    font-weight:800;
    text-decoration: underline;
  }
  .twoCols{
    display:grid;
    grid-template-columns: 1fr 1fr;
    gap: 22px;
    margin-top: 18px;
  }
  @media (max-width: 900px){
    .twoCols{ grid-template-columns: 1fr; }
  }
  .fieldRow{ display:grid; grid-template-columns: 140px 1fr; gap:10px; margin: 8px 0; }
  .fieldKey{ font-weight:800; }
  .fieldVal{ color: var(--text); }

  .totalsGrid{
    display:grid;
    grid-template-columns: 1fr 240px;
    gap: 16px;
    margin-top: 14px;
  }
  @media (max-width: 900px){
    .totalsGrid{ grid-template-columns: 1fr; }
  }
  .totalsBox{
    border:1px solid var(--border);
    border-radius:12px;
    padding: 12px;
    background:#fff;
  }
  .totLine{
    display:flex;
    justify-content:space-between;
    gap:12px;
    padding: 6px 0;
    border-bottom:1px dashed var(--border);
    font-size: 13px;
  }
  .totLine:last-child{ border-bottom:none; }
  .totLabel{ color: var(--muted); font-weight:700; }
  .totValue{ font-weight:900; }
  .bigTotal{
    margin-top: 12px;
    border-top: 2px solid #111827;
    padding-top: 12px;
    display:flex;
    justify-content:space-between;
    font-weight: 900;
  }
  `;
}

function renderShell({ title, active, topTitle, topMeta, bodyHtml }) {
  const isActive = (k) => (k === active ? "navItem active" : "navItem");
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>${title}</title>
  <style>${uiCss()}</style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">
        <div class="mark">SL</div>
        <div>
          <div class="brandName">SAP → GRA E-VAT</div>
          <div class="brandSub">Invoice Monitor</div>
        </div>
      </div>

      <nav class="navList">
        <a class="${isActive("live")}" href="/">
          <span>Live Monitor</span>
          <span class="navHint">SSE</span>
        </a>
        <a class="${isActive("invoices")}" href="/invoices">
          <span>Invoices</span>
          <span class="navHint">List</span>
        </a>
        <a class="${isActive("gra")}" href="/gra">
          <span>GRA / E-VAT</span>
          <span class="navHint">${GRA_ENABLED ? "REAL" : "MOCK"}</span>
        </a>
      </nav>

      <div style="margin-top:14px; padding:12px; border:1px solid rgba(226,232,240,.10); border-radius:12px;">
        <div class="navHint">Polling</div>
        <div style="font-weight:800; margin-top:4px;">${POLL_MS} ms</div>
        <div class="navHint" style="margin-top:8px;">Cache TTL</div>
        <div style="font-weight:800; margin-top:4px;">30 mins</div>
      </div>
    </aside>

    <main class="content">
      <div class="topbar">
        <div>
          <div class="topTitle">${topTitle}</div>
          <div class="topMeta">${topMeta}</div>
        </div>
        <div class="row">
          <span class="chip">Mode: <b>${GRA_ENABLED ? "REAL POST" : "MOCK"}</b></span>
          <span class="chip">TLS Verify: <b>${VERIFY_TLS ? "ON" : "OFF"}</b></span>
        </div>
      </div>

      <div class="page">
        ${bodyHtml}
      </div>

      <div class="topMeta" style="margin-top:12px;">
        Tip: If SAP has a self-signed certificate in dev, set VERIFY_TLS=false (testing only).
      </div>
    </main>
  </div>
</body>
</html>`;
}

function clientErrorBox(errObj) {
  const title = errObj?.userTitle || "Something went wrong";
  const msg = errObj?.userMessage || errObj?.error || "We couldn’t complete the request.";
  const hint = errObj?.hint || "";
  return `
    <div class="alert alertError">
      <div class="alertIcon">!</div>
      <div>
        <div class="alertTitle">${escapeHtml(title)}</div>
        <div class="alertMsg">${escapeHtml(msg)}</div>
        ${hint ? `<div class="alertHint">Tip: ${escapeHtml(hint)}</div>` : ""}
      </div>
    </div>
  `;
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// -------------------- UI: LIVE --------------------
app.get("/", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <div>
          <div style="font-weight:900; font-size:16px;">Live invoice feed</div>
          <div class="topMeta" style="margin-top:6px;">New invoices detected by polling and pushed here.</div>
        </div>
        <div class="row">
          <span class="chip">Polling: <b>${POLL_MS}ms</b></span>
          <span class="chip">Auto-cache full JSON</span>
        </div>
      </div>

      <div id="notice" style="margin-top:12px;"></div>
      <div id="log" class="grid" style="margin-top:12px;"></div>
    </div>

    <script>
      const log = document.getElementById("log");
      const notice = document.getElementById("notice");
      const es = new EventSource("/events");

      function showErrorBox(title, message, hint){
        notice.innerHTML = \`
          <div class="alert alertError">
            <div class="alertIcon">!</div>
            <div>
              <div class="alertTitle">\${title}</div>
              <div class="alertMsg">\${message}</div>
              \${hint ? '<div class="alertHint">Tip: ' + hint + '</div>' : ''}
            </div>
          </div>
        \`;
      }

      function clearNotice(){ notice.innerHTML = ""; }
      function fmt(v){ return (v === null || v === undefined) ? "" : String(v); }

      es.onmessage = (evt) => {
        const msg = JSON.parse(evt.data);

        if (msg.type === "INFO") {
          clearNotice();
        }

        if (msg.type === "NEW_INVOICE") {
          clearNotice();
          const el = document.createElement("div");
          el.className = "card";
          el.innerHTML = \`
            <div class="row" style="justify-content:space-between;">
              <div class="chip">New invoice</div>
              <div class="topMeta">DocEntry: <b>\${msg.docEntry}</b> • DocNum: <b>\${fmt(msg.docNum)}</b></div>
            </div>
            <div style="margin-top:10px;">
              <div style="font-size:16px; font-weight:900;">\${fmt(msg.cardName) || "—"}</div>
              <div class="topMeta">Total: <b>\${fmt(msg.docTotal)}</b> \${fmt(msg.docCurrency)}</div>
            </div>
            <div class="row" style="margin-top:12px;">
              <a class="chip" href="/invoice/\${msg.docEntry}">Open invoice</a>
              <a class="chip" href="/gra#docEntry=\${msg.docEntry}">GRA mapping</a>
            </div>
          \`;
          log.prepend(el);

          // Browser notification (if allowed)
          if ("Notification" in window) {
            if (Notification.permission === "granted") {
              new Notification("New invoice detected", { body: "DocEntry " + msg.docEntry + " • " + (msg.cardName || "") });
            }
          }
        }

        if (msg.type === "ERROR") {
          showErrorBox("Live feed error", msg.message || "Something went wrong.", "Check SAP connection and try again.");
        }
      };

      // Ask notification permission once (optional)
      (async function(){
        try{
          if ("Notification" in window && Notification.permission === "default") {
            await Notification.requestPermission();
          }
        }catch(e){}
      })();
    </script>
  `;

  res.send(
    renderShell({
      title: "Live Monitor",
      active: "live",
      topTitle: "Live Monitor",
      topMeta: "Real-time feed of newly detected SAP invoices.",
      bodyHtml,
    })
  );
});

// -------------------- UI: FULL INVOICE (GRA-like template + collapsible JSON) --------------------
app.get("/invoice/:docEntry", (req, res) => {
  const docEntry = Number(req.params.docEntry || 0);

  const bodyHtml = `
    <div class="row" style="margin-bottom:12px;">
      <span class="chip">DocEntry: <b id="docEntry">${docEntry}</b></span>
      <a class="chip" href="/gra#docEntry=${docEntry}">Open in GRA mapping</a>
    </div>

    <div id="errorWrap" style="display:none; margin-bottom:12px;"></div>

    <div class="grid grid2">
      <div class="card">
        <div style="font-weight:900; font-size:16px;">GRA-style invoice preview</div>
        <div class="topMeta" style="margin-top:6px;">This is a template view built from the invoice JSON. Raw JSON is still available.</div>

        <div id="paper" class="paper" style="margin-top:14px;">
          <div class="paperHeader">GRA</div>
          <div class="paperSub">VAT INVOICE</div>

          <div class="twoCols">
            <div>
              <div class="fieldRow"><div class="fieldKey">CUSTOMER:</div><div class="fieldVal" id="custName">—</div></div>
              <div class="fieldRow"><div class="fieldKey">CUSTOMER TIN:</div><div class="fieldVal" id="custTin">—</div></div>
              <div class="fieldRow"><div class="fieldKey">INVOICE NO:</div><div class="fieldVal" id="invNo">—</div></div>
              <div class="fieldRow"><div class="fieldKey">DATE:</div><div class="fieldVal" id="invDate">—</div></div>
              <div class="fieldRow"><div class="fieldKey">SERVED BY:</div><div class="fieldVal" id="servedBy">—</div></div>
              <div class="fieldRow"><div class="fieldKey">BRANCH NAME:</div><div class="fieldVal" id="branchName">—</div></div>
            </div>
            <div>
              <div class="fieldRow"><div class="fieldKey">VENDOR:</div><div class="fieldVal" id="vendorName">—</div></div>
              <div class="fieldRow"><div class="fieldKey">VENDOR TIN:</div><div class="fieldVal" id="vendorTin">—</div></div>
              <div class="fieldRow"><div class="fieldKey">PHONE #:</div><div class="fieldVal" id="phone">—</div></div>
              <div class="fieldRow"><div class="fieldKey">DUE DATE:</div><div class="fieldVal" id="dueDate">—</div></div>
              <div class="fieldRow"><div class="fieldKey">CURRENCY:</div><div class="fieldVal" id="currency">—</div></div>
            </div>
          </div>

          <div style="margin-top:16px;">
            <table>
              <thead>
                <tr>
                  <th>Item Code</th>
                  <th>Item Description</th>
                  <th style="width:120px;">Item Price</th>
                  <th style="width:110px;">Quantity</th>
                  <th style="width:140px;">Amount</th>
                </tr>
              </thead>
              <tbody id="linesBody"></tbody>
            </table>
          </div>

          <div class="totalsGrid">
            <div>
              <div class="fieldRow"><div class="fieldKey">GROUP REFERENCE:</div><div class="fieldVal" id="groupRef">—</div></div>
              <div class="fieldRow"><div class="fieldKey">REMARKS:</div><div class="fieldVal" id="remarks">—</div></div>
            </div>
            <div class="totalsBox">
              <div class="totLine"><div class="totLabel">TOTAL (EXCL TAXES):</div><div class="totValue" id="totExcl">—</div></div>
              <div class="totLine"><div class="totLabel">DISCOUNT:</div><div class="totValue" id="totDisc">—</div></div>
              <div class="totLine"><div class="totLabel">NHIL (2.5%):</div><div class="totValue" id="totNhil">—</div></div>
              <div class="totLine"><div class="totLabel">GETFUND (2.5%):</div><div class="totValue" id="totGetfund">—</div></div>
              <div class="totLine"><div class="totLabel">CST (5%):</div><div class="totValue" id="totCst">—</div></div>
              <div class="totLine"><div class="totLabel">TOURISM (1%):</div><div class="totValue" id="totTourism">—</div></div>
              <div class="totLine"><div class="totLabel">COVID (1%):</div><div class="totValue" id="totCovid">—</div></div>
              <div class="totLine"><div class="totLabel">VAT:</div><div class="totValue" id="totVat">—</div></div>
              <div class="totLine"><div class="totLabel">TOTAL TAXES:</div><div class="totValue" id="totTaxes">—</div></div>

              <div class="bigTotal">
                <div>INVOICE TOTAL:</div>
                <div id="invoiceTotal">—</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="row" style="justify-content:space-between;">
          <div>
            <div style="font-weight:900; font-size:16px;">Invoice JSON</div>
            <div class="topMeta" style="margin-top:6px;">Exact SAP response. You can collapse it.</div>
          </div>
          <div class="row">
            <button id="reload">Reload</button>
            <button class="btnPrimary" id="copy">Copy</button>
            <button id="toggleRaw">Collapse</button>
          </div>
        </div>

        <div id="rawWrap" style="margin-top:12px;">
          <pre id="raw" class="noClamp">Loading...</pre>
        </div>

        <div style="margin-top:14px; font-weight:900;">Structured view (expand sections)</div>
        <div class="row" style="margin-top:10px;">
          <button id="collapseAll">Collapse all</button>
          <button class="btnPrimary" id="expandAll">Expand all</button>
        </div>
        <div id="tree" style="margin-top:12px;"></div>
      </div>
    </div>

    <script>
      const docEntry = Number(document.getElementById("docEntry").textContent || "0");
      const errorWrap = document.getElementById("errorWrap");
      const rawEl = document.getElementById("raw");
      const treeEl = document.getElementById("tree");

      // Collapse raw JSON
      const rawWrap = document.getElementById("rawWrap");
      const toggleRawBtn = document.getElementById("toggleRaw");
      let rawCollapsed = false;
      function setRawCollapsed(collapsed){
        rawCollapsed = !!collapsed;
        rawWrap.style.display = rawCollapsed ? "none" : "block";
        toggleRawBtn.textContent = rawCollapsed ? "Expand" : "Collapse";
      }
      toggleRawBtn.addEventListener("click", () => setRawCollapsed(!rawCollapsed));

      function showNiceError(errObj){
        const title = errObj?.userTitle || "Something went wrong";
        const msg = errObj?.userMessage || errObj?.error || "We couldn’t complete the request.";
        const hint = errObj?.hint || "";
        errorWrap.style.display = "block";
        errorWrap.innerHTML = \`
          <div class="alert alertError">
            <div class="alertIcon">!</div>
            <div>
              <div class="alertTitle">\${escapeHtml(title)}</div>
              <div class="alertMsg">\${escapeHtml(msg)}</div>
              \${hint ? '<div class="alertHint">Tip: ' + escapeHtml(hint) + '</div>' : ''}
            </div>
          </div>
        \`;
      }
      function clearError(){ errorWrap.style.display="none"; errorWrap.innerHTML=""; }

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      function escapeHtml(s){
        return String(s ?? "")
          .replaceAll("&","&amp;")
          .replaceAll("<","&lt;")
          .replaceAll(">","&gt;")
          .replaceAll('"',"&quot;")
          .replaceAll("'","&#039;");
      }

      function safe(v){
        if (v === null) return "null";
        if (v === undefined) return "undefined";
        if (typeof v === "string") return v;
        return JSON.stringify(v);
      }

      function renderKV(obj){
        const keys = Object.keys(obj || {});
        const wrap = document.createElement("div");
        keys.forEach(k => {
          const row = document.createElement("div");
          row.className = "kv";
          row.innerHTML = \`<div class="k">\${escapeHtml(k)}</div><div class="v">\${escapeHtml(safe(obj[k]))}</div>\`;
          wrap.appendChild(row);
        });
        return wrap;
      }

      function renderTree(value, label){
        const t = typeof value;

        if (value === null || t !== "object"){
          const d = document.createElement("details");
          d.open = false;
          const sum = document.createElement("summary");
          sum.textContent = label;
          d.appendChild(sum);
          const div = document.createElement("div");
          div.style.marginTop = "10px";
          div.innerHTML = \`<div class="kv"><div class="k">value</div><div class="v">\${escapeHtml(String(value))}</div></div>\`;
          d.appendChild(div);
          return d;
        }

        if (Array.isArray(value)){
          const d = document.createElement("details");
          d.open = false;
          const sum = document.createElement("summary");
          sum.textContent = \`\${label} (array • \${value.length})\`;
          d.appendChild(sum);

          const inner = document.createElement("div");
          inner.style.marginTop = "10px";
          value.forEach((item, idx) => inner.appendChild(renderTree(item, \`[\${idx}]\`)));
          d.appendChild(inner);
          return d;
        }

        const d = document.createElement("details");
        d.open = false;
        const sum = document.createElement("summary");
        sum.textContent = \`\${label} (object)\`;
        d.appendChild(sum);

        const inner = document.createElement("div");
        inner.style.marginTop = "10px";

        const keys = Object.keys(value);
        const primitive = {};
        const complex = [];

        keys.forEach(k => {
          const v = value[k];
          if (v === null || typeof v !== "object") primitive[k] = v;
          else complex.push([k, v]);
        });

        if (Object.keys(primitive).length){
          inner.appendChild(renderKV(primitive));
        }
        complex.forEach(([k, v]) => inner.appendChild(renderTree(v, k)));
        d.appendChild(inner);
        return d;
      }

      function setAllDetails(open){
        const nodes = treeEl.querySelectorAll("details");
        nodes.forEach(d => { d.open = !!open; });
      }

      function money(n){
        const x = Number(n);
        if (!Number.isFinite(x)) return "0.00";
        return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      }
      function d10(s){
        if (!s) return "";
        return String(s).slice(0,10);
      }

      function fillInvoiceTemplate(inv){
        // Header basics
        document.getElementById("custName").textContent = (inv.CardName || "cash customer");
        document.getElementById("custTin").textContent = (inv.U_TIN || inv.U_BP_TIN || "${GRA_DEFAULT_BP_TIN}");
        document.getElementById("invNo").textContent = String(inv.DocNum ?? inv.DocEntry ?? "");
        document.getElementById("invDate").textContent = d10(inv.DocDate || inv.TaxDate || inv.CreationDate);
        document.getElementById("servedBy").textContent = (inv.SalesPersonName || inv.U_SERVED_BY || "—");
        document.getElementById("branchName").textContent = (inv.BPLName || inv.U_BRANCH || "—");

        document.getElementById("vendorName").textContent = (inv.U_VENDOR || "—");
        document.getElementById("vendorTin").textContent = (inv.U_VENDOR_TIN || "—");
        document.getElementById("phone").textContent = (inv.U_PHONE || "—");
        document.getElementById("dueDate").textContent = d10(inv.DocDueDate || "");
        document.getElementById("currency").textContent = (inv.DocCurrency || "GHS");

        document.getElementById("groupRef").textContent = (inv.U_GROUP_REFERENCE || inv.Reference1 || "—");
        document.getElementById("remarks").textContent = (inv.Comments || "—");

        // Lines
        const lines = Array.isArray(inv.DocumentLines) ? inv.DocumentLines : [];
        const tbody = document.getElementById("linesBody");
        tbody.innerHTML = lines.map(ln => {
          const qty = Number(ln.Quantity || 0);
          const price = Number(ln.UnitPrice ?? ln.Price ?? 0);
          const amount = qty * price;
          return \`
            <tr>
              <td>\${escapeHtml(String(ln.ItemCode || ""))}</td>
              <td>\${escapeHtml(String(ln.ItemDescription || ""))}</td>
              <td>\${money(price)}</td>
              <td>\${money(qty)}</td>
              <td>\${money(amount)}</td>
            </tr>
          \`;
        }).join("");

        // Taxes (best-effort)
        const vat = Number(inv.VatSum || 0);
        const docTotal = Number(inv.DocTotal || 0);
        const totalExcl = docTotal - vat;

        // Levy totals (from line UDFs if present)
        function sumUdf(keys){
          let s = 0;
          for (const ln of lines){
            for (const k of keys){
              const v = Number(ln?.[k]);
              if (Number.isFinite(v)) { s += v; break; }
            }
          }
          return s;
        }
        const nhil = sumUdf(["U_LEVY_A","U_NHIL","U_NHIL_LEVY","U_LevyA"]);
        const getfund = sumUdf(["U_LEVY_B","U_GETFUND","U_GETFUND_LEVY","U_LevyB"]);
        const covid = sumUdf(["U_LEVY_C","U_COVID","U_COVID_LEVY","U_LevyC"]);
        const cst = sumUdf(["U_LEVY_D","U_CST","U_CST_LEVY","U_LevyD"]);
        const tourism = sumUdf(["U_LEVY_E","U_TOURISM","U_TOURISM_LEVY","U_LevyE"]);

        const discount = 0;

        const totalTaxes = vat + nhil + getfund + covid + cst + tourism;

        document.getElementById("totExcl").textContent = money(totalExcl);
        document.getElementById("totDisc").textContent = money(discount);
        document.getElementById("totNhil").textContent = money(nhil);
        document.getElementById("totGetfund").textContent = money(getfund);
        document.getElementById("totCst").textContent = money(cst);
        document.getElementById("totTourism").textContent = money(tourism);
        document.getElementById("totCovid").textContent = money(covid);
        document.getElementById("totVat").textContent = money(vat);
        document.getElementById("totTaxes").textContent = money(totalTaxes);
        document.getElementById("invoiceTotal").textContent = money(docTotal);
      }

      async function load(){
        clearError();
        if (!Number.isFinite(docEntry) || docEntry <= 0){
          showNiceError({ userTitle: "Invalid invoice", userMessage: "This invoice number is not valid.", hint: "Open from the invoices list and try again." });
          rawEl.textContent = "Invalid DocEntry";
          treeEl.textContent = "";
          return;
        }

        rawEl.textContent = "Loading...";
        treeEl.textContent = "";

        try{
          const result = await fetchJson(\`/api/invoice/\${docEntry}/raw\`);
          const inv = result.data;

          rawEl.textContent = JSON.stringify(inv, null, 2);
          treeEl.appendChild(renderTree(inv, "Invoice"));
          fillInvoiceTemplate(inv);
        }catch(e){
          showNiceError(e);
          rawEl.textContent = "Failed to load invoice.";
        }
      }

      document.getElementById("reload").addEventListener("click", load);
      document.getElementById("copy").addEventListener("click", async () => {
        try{
          await navigator.clipboard.writeText(rawEl.textContent);
          alert("Copied");
        }catch(e){
          alert("Copy failed.");
        }
      });

      document.getElementById("expandAll").addEventListener("click", () => setAllDetails(true));
      document.getElementById("collapseAll").addEventListener("click", () => setAllDetails(false));

      // Start with Raw JSON expanded (change to true if you want default collapsed)
      setRawCollapsed(false);

      load();
    </script>
  `;

  res.send(
    renderShell({
      title: `Invoice ${docEntry}`,
      active: "invoices",
      topTitle: "Invoice details",
      topMeta: "GRA-style preview + raw JSON + structured view.",
      bodyHtml,
    })
  );
});

// -------------------- UI: INVOICES LIST --------------------
app.get("/invoices", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <div>
          <div style="font-weight:900; font-size:16px;">Invoices</div>
          <div class="topMeta" style="margin-top:6px;">Select invoices for batch posting, or click a row to open full invoice.</div>
        </div>
        <div class="row">
          <span class="chip">Polling: <b>${POLL_MS}ms</b></span>
          <span class="chip">Cache: <b>30 mins</b></span>
        </div>
      </div>

      <div class="row" style="margin-top:14px;">
        <button class="btnPrimary" id="reload">Reload</button>

        <span class="topMeta">Page</span>
        <button id="prev">Prev</button>
        <span id="pageNum" class="chip">1</span>
        <button id="next">Next</button>

        <span class="topMeta">Page size</span>
        <select id="pageSize">
          <option>5</option>
          <option selected>10</option>
          <option>15</option>
          <option>20</option>
          <option>50</option>
        </select>

        <button class="btnSuccess" id="selectAll">Select all</button>
        <button class="btnDanger" id="clearSel">Clear</button>
        <button class="btnWarn" id="sendToGra">Go to GRA</button>
      </div>

      <div id="errorWrap" style="display:none; margin-top:12px;"></div>
      <div id="meta" class="topMeta" style="margin-top:12px;"></div>

      <div style="margin-top:12px; max-height: 70vh; overflow:auto; border-radius:12px;">
        <div id="tableWrap"></div>
      </div>
    </div>

    <script>
      let page = 1;
      let pageSize = 10;
      const selected = new Set();

      const errorWrap = document.getElementById("errorWrap");
      const metaEl = document.getElementById("meta");
      const tableWrap = document.getElementById("tableWrap");
      const pageNumEl = document.getElementById("pageNum");
      const pageSizeEl = document.getElementById("pageSize");

      function showNiceError(errObj){
        errorWrap.style.display="block";
        errorWrap.innerHTML = ${JSON.stringify(clientErrorBox({}))}.replace("Something went wrong", errObj?.userTitle || "Something went wrong")
          .replace("We couldn’t complete the request.", errObj?.userMessage || errObj?.error || "We couldn’t complete the request.")
          .replace("Tip:", errObj?.hint ? ("Tip: " + errObj.hint) : "");
        // If no hint, remove line
        if (!errObj?.hint) {
          errorWrap.innerHTML = errorWrap.innerHTML.replace(/<div class="alertHint">.*?<\\/div>/, "");
        }
      }
      function clearError(){ errorWrap.style.display="none"; errorWrap.innerHTML=""; }

      function safe(v){ return (v === null || v === undefined) ? "" : String(v); }
      function d10(x){ return x ? String(x).slice(0,10) : ""; }

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      async function loadServerSelection(){
        const s = await fetchJson("/api/gra/history");
        (s.lastSelection || []).forEach(d => selected.add(Number(d)));
      }

      async function syncSelectionToServer(){
        const docEntries = Array.from(selected);
        await fetchJson("/api/gra/select", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ docEntries })
        });
      }

      function statusBadge(row){
        const st = safe(row.DocumentStatus).toLowerCase();
        const cancelled = safe(row.Cancelled);
        if (String(cancelled).includes("tYES")) return '<span class="badge bCancelled">Cancelled</span>';
        if (st.includes("open")) return '<span class="badge bOpen">Open</span>';
        return '<span class="badge bClosed">' + (safe(row.DocumentStatus) || "—") + '</span>';
      }

      async function loadSummary(){
        clearError();
        pageNumEl.textContent = String(page);

        const data = await fetchJson(\`/api/invoices/summary?page=\${page}&pageSize=\${pageSize}\`);
        const rows = data.value || [];

        metaEl.textContent = rows.length
          ? \`Loaded \${rows.length} invoices • Selected: \${selected.size}\`
          : "No invoices found";

        const html = \`
          <table>
            <thead>
              <tr>
                <th style="width:70px;">Select</th>
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
                const de = Number(r.DocEntry);
                const checked = selected.has(de) ? "checked" : "";
                return \`
                  <tr data-docentry="\${de}">
                    <td><input type="checkbox" data-check="\${de}" \${checked}/></td>
                    <td><b>\${de}</b></td>
                    <td>\${safe(r.DocNum)}</td>
                    <td>
                      <div><b>\${safe(r.CardCode)}</b></div>
                      <div class="topMeta">\${safe(r.CardName)}</div>
                    </td>
                    <td><b>\${safe(r.DocTotal)}</b> \${safe(r.DocCurrency)}</td>
                    <td>
                      <div><b>\${d10(r.DocDate)}</b> \${safe(r.DocTime)}</div>
                      <div class="topMeta">Created: \${d10(r.CreationDate)} • Updated: \${safe(r.UpdateTime)}</div>
                    </td>
                    <td>\${statusBadge(r)}</td>
                  </tr>
                \`;
              }).join("")}
            </tbody>
          </table>
        \`;

        tableWrap.innerHTML = html;
      }

      tableWrap.addEventListener("change", async (e) => {
        const cb = e.target.closest("input[data-check]");
        if (!cb) return;
        const docEntry = Number(cb.getAttribute("data-check"));
        if (!Number.isFinite(docEntry) || docEntry <= 0) return;

        if (cb.checked) selected.add(docEntry);
        else selected.delete(docEntry);

        metaEl.textContent = \`Selected: \${selected.size}\`;
        try { await syncSelectionToServer(); } catch {}
        e.stopPropagation();
      });

      tableWrap.addEventListener("click", (e) => {
        if (e.target.closest("input[data-check]")) return;
        const tr = e.target.closest("tr[data-docentry]");
        if (!tr) return;
        const docEntry = Number(tr.getAttribute("data-docentry"));
        if (!Number.isFinite(docEntry) || docEntry <= 0) return;
        location.href = "/invoice/" + docEntry;
      });

      document.getElementById("reload").addEventListener("click", () => loadSummary());
      document.getElementById("prev").addEventListener("click", () => { if (page > 1){ page--; loadSummary(); } });
      document.getElementById("next").addEventListener("click", () => { page++; loadSummary(); });

      pageSizeEl.addEventListener("change", () => {
        pageSize = parseInt(pageSizeEl.value, 10);
        page = 1;
        loadSummary();
      });

      document.getElementById("selectAll").addEventListener("click", async () => {
        const cbs = tableWrap.querySelectorAll("input[data-check]");
        cbs.forEach(cb => {
          cb.checked = true;
          const docEntry = Number(cb.getAttribute("data-check"));
          if (Number.isFinite(docEntry) && docEntry > 0) selected.add(docEntry);
        });
        metaEl.textContent = \`Selected: \${selected.size}\`;
        try { await syncSelectionToServer(); } catch {}
      });

      document.getElementById("clearSel").addEventListener("click", async () => {
        selected.clear();
        const cbs = tableWrap.querySelectorAll("input[data-check]");
        cbs.forEach(cb => cb.checked = false);
        metaEl.textContent = \`Selected: \${selected.size}\`;
        try { await syncSelectionToServer(); } catch {}
      });

      document.getElementById("sendToGra").addEventListener("click", async () => {
        if (!selected.size){
          showNiceError({ userTitle:"Nothing selected", userMessage:"Please tick at least one invoice before going to GRA.", hint:"Select invoices using the checkbox on the left." });
          return;
        }
        try{
          await syncSelectionToServer();
          location.href = "/gra";
        }catch(e){
          showNiceError(e);
        }
      });

      (async function init(){
        try{ await loadServerSelection(); }catch(e){}
        await loadSummary();
      })();
    </script>
  `;

  res.send(
    renderShell({
      title: "Invoices",
      active: "invoices",
      topTitle: "Invoices",
      topMeta: "Browse, select, and open full invoice details.",
      bodyHtml,
    })
  );
});

// -------------------- UI: GRA / E-VAT (remove selection + preview + post) --------------------
app.get("/gra", (req, res) => {
  const bodyHtml = `
    <div class="row" style="margin-bottom:12px;">
      <span class="chip">Mode: <b id="modeChip">Loading…</b></span>
      <span class="chip">Endpoint: <b id="urlChip">Loading…</b></span>
      <span class="chip">Posting: <b>${GRA_ENABLED ? "Enabled" : "Mock only"}</b></span>
    </div>

    <div id="errorWrap" style="display:none; margin-bottom:12px;"></div>

    <div class="grid grid2">
      <div class="card">
        <div style="font-weight:900; font-size:16px;">Single invoice</div>
        <div class="topMeta" style="margin-top:6px;">Preview the mapped payload, then post.</div>

        <div class="row" style="margin-top:12px;">
          <input id="docEntry" placeholder="DocEntry e.g. 111" />
          <button id="previewBtn">Preview mapping</button>
          <button class="btnPrimary" id="postBtn">${GRA_ENABLED ? "POST to GRA" : "Mock post"}</button>
        </div>

        <div style="margin-top:14px; font-weight:900;">Mapped payload</div>
        <pre id="payloadOut" style="margin-top:10px;" class="noClamp">Waiting…</pre>

        <div style="margin-top:14px; font-weight:900;">Post response</div>
        <pre id="respOut" style="margin-top:10px;" class="noClamp">Waiting…</pre>
      </div>

      <div class="card">
        <div style="font-weight:900; font-size:16px;">Batch selection</div>
        <div class="topMeta" style="margin-top:6px;">Remove items from the list before posting.</div>

        <div style="margin-top:12px;">
          <div class="chip">Selected DocEntries</div>
          <div id="selList" style="margin-top:10px;"></div>
        </div>

        <div class="row" style="margin-top:12px;">
          <button class="btnWarn" id="batchPostBtn">${GRA_ENABLED ? "POST selected" : "Mock post selected"}</button>
          <a class="chip" href="/invoices">Go to invoices</a>
        </div>

        <div style="margin-top:14px; font-weight:900;">Batch result</div>
        <pre id="batchOut" style="margin-top:10px;" class="noClamp">Waiting…</pre>

        <div style="margin-top:14px; font-weight:900;">Session history</div>
        <pre id="histOut" style="margin-top:10px;" class="noClamp">Loading…</pre>
      </div>
    </div>

    <script>
      const errorWrap = document.getElementById("errorWrap");
      const payloadOut = document.getElementById("payloadOut");
      const respOut = document.getElementById("respOut");
      const batchOut = document.getElementById("batchOut");
      const histOut = document.getElementById("histOut");
      const selList = document.getElementById("selList");
      const docEntryEl = document.getElementById("docEntry");
      const modeChip = document.getElementById("modeChip");
      const urlChip = document.getElementById("urlChip");

      function showNiceError(errObj){
        const title = errObj?.userTitle || "Something went wrong";
        const msg = errObj?.userMessage || errObj?.error || "We couldn’t complete the request.";
        const hint = errObj?.hint || "";
        errorWrap.style.display = "block";
        errorWrap.innerHTML = \`
          <div class="alert alertError">
            <div class="alertIcon">!</div>
            <div>
              <div class="alertTitle">\${escapeHtml(title)}</div>
              <div class="alertMsg">\${escapeHtml(msg)}</div>
              \${hint ? '<div class="alertHint">Tip: ' + escapeHtml(hint) + '</div>' : ''}
            </div>
          </div>
        \`;
      }
      function clearError(){ errorWrap.style.display="none"; errorWrap.innerHTML=""; }

      function escapeHtml(s){
        return String(s ?? "")
          .replaceAll("&","&amp;")
          .replaceAll("<","&lt;")
          .replaceAll(">","&gt;")
          .replaceAll('"',"&quot;")
          .replaceAll("'","&#039;");
      }

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      async function loadSession(){
        const s = await fetchJson("/api/gra/history");
        modeChip.textContent = s.enabled ? "REAL POST" : "MOCK";
        urlChip.textContent = s.invoiceUrl || "(not set)";

        const sel = (s.lastSelection || []);
        renderSelection(sel);
        histOut.textContent = JSON.stringify(s.history || [], null, 2);

        const hash = new URLSearchParams((location.hash || "").replace("#",""));
        const de = hash.get("docEntry");
        if (de){
          docEntryEl.value = de;
          location.hash = "";
        }
      }

      function renderSelection(sel){
        if (!sel.length){
          selList.innerHTML = '<div class="topMeta">(none — go to Invoices and select some)</div>';
          return;
        }

        selList.innerHTML = sel.map(d => \`
          <div class="row" style="justify-content:space-between; padding:10px 12px; border:1px solid var(--border); border-radius:12px; background:#fff; margin-bottom:8px;">
            <div>
              <div style="font-weight:900;">DocEntry: \${d}</div>
              <div class="topMeta"><a href="/invoice/\${d}">Open invoice</a> • <a href="#docEntry=\${d}" onclick="document.getElementById('docEntry').value=\${d}">Use as single</a></div>
            </div>
            <button class="btnDanger" data-remove="\${d}">Remove</button>
          </div>
        \`).join("");

        // remove handlers
        selList.querySelectorAll("button[data-remove]").forEach(btn => {
          btn.addEventListener("click", async () => {
            try{
              clearError();
              const docEntry = Number(btn.getAttribute("data-remove"));
              await fetchJson("/api/gra/remove", {
                method:"POST",
                headers:{ "Content-Type":"application/json" },
                body: JSON.stringify({ docEntry })
              });
              await loadSession();
            }catch(e){
              showNiceError(e);
            }
          });
        });
      }

      document.getElementById("previewBtn").addEventListener("click", async () => {
        try{
          clearError();
          payloadOut.textContent = "Loading mapping...";
          respOut.textContent = "Waiting…";

          const docEntry = Number(docEntryEl.value);
          if (!Number.isFinite(docEntry) || docEntry <= 0){
            showNiceError({ userTitle:"Invalid DocEntry", userMessage:"Please enter a valid DocEntry (e.g. 111).", hint:"You can copy it from the Invoices page." });
            payloadOut.textContent = "Waiting…";
            return;
          }

          const data = await fetchJson("/api/gra/payload/" + docEntry);
          payloadOut.textContent = JSON.stringify(data.payload, null, 2);
        }catch(e){
          showNiceError(e);
          payloadOut.textContent = "Failed.";
        }
      });

      document.getElementById("postBtn").addEventListener("click", async () => {
        try{
          clearError();
          payloadOut.textContent = "Preparing...";
          respOut.textContent = "Posting...";

          const docEntry = Number(docEntryEl.value);
          if (!Number.isFinite(docEntry) || docEntry <= 0){
            showNiceError({ userTitle:"Invalid DocEntry", userMessage:"Please enter a valid DocEntry (e.g. 111).", hint:"Open an invoice page and copy the DocEntry." });
            respOut.textContent = "Waiting…";
            return;
          }

          const preview = await fetchJson("/api/gra/payload/" + docEntry);
          payloadOut.textContent = JSON.stringify(preview.payload, null, 2);

          const resp = await fetchJson("/api/gra/post/single", {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ docEntry })
          });

          respOut.textContent = JSON.stringify(resp.graResp, null, 2);
          await loadSession();
        }catch(e){
          showNiceError(e);
          respOut.textContent = "Failed.";
          await loadSession();
        }
      });

      document.getElementById("batchPostBtn").addEventListener("click", async () => {
        try{
          clearError();
          batchOut.textContent = "Posting batch...";

          const s = await fetchJson("/api/gra/history");
          const docEntries = s.lastSelection || [];
          if (!docEntries.length){
            showNiceError({ userTitle:"Nothing selected", userMessage:"No invoices selected for batch posting.", hint:"Go to Invoices and tick invoices to select them." });
            batchOut.textContent = "Failed.";
            return;
          }

          const resp = await fetchJson("/api/gra/post/batch", {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ docEntries })
          });

          batchOut.textContent = JSON.stringify({
            ok: resp.ok,
            mode: resp.mode,
            count: resp.count,
            results: (resp.results || []).map(r => ({
              docEntry: r.docEntry,
              mode: r.graResp?.mode,
              httpStatus: r.graResp?.httpStatus,
              status: r.graResp?.data?.response?.status || r.graResp?.status || "OK"
            }))
          }, null, 2);

          await loadSession();
        }catch(e){
          showNiceError(e);
          batchOut.textContent = "Failed.";
          await loadSession();
        }
      });

      loadSession();
    </script>
  `;

  res.send(
    renderShell({
      title: "GRA / E-VAT",
      active: "gra",
      topTitle: "GRA / E-VAT",
      topMeta: "Preview mapped payloads and post (or mock post) to the E-VAT endpoint.",
      bodyHtml,
    })
  );
});

// -------------------- START --------------------
app.listen(process.env.PORT || PORT, () => {
  console.log(`Running on http://localhost:${process.env.PORT || PORT}`);
  console.log(`Polling every ${POLL_MS}ms`);
  console.log(`GRA posting mode: ${GRA_ENABLED ? "REAL" : "MOCK"}`);
});