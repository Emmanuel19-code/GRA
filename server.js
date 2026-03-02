/**
 * server.js
 * -------------------------------------------------------------------
 * SAP B1 Service Layer:
 *  - Polls new invoices (by DocEntry) and caches FULL JSON
 *  - Invoices list page + Full invoice page (shows EVERYTHING)
 *
 * GRA E-VAT:
 *  - STRICT payload mapping
 *  - UI toggle: INCLUSIVE vs EXCLUSIVE
 *  - Pre-post checks (blocks hard errors)
 *  - Posting screen (/post-gra) to POST + display SUCCESS response
 *  - On failure: shows the real GRA error (HTTP + body)
 *  - Export SUCCESS response as PDF: /api/gra/post-result/:docEntry/pdf
 *
 * ItemCode mapping SAP->GRA via env:
 *   GRA_ITEM_CODE_MAP_JSON={"ITM-00002":"TXC00389165855"}
 *
 * Default tax model used:
 *  - Levies A..E computed from BASE using rates
 *  - VAT = VAT_RATE * (BASE + TOTAL_LEVY)
 *  - Inclusive factor:
 *      TOTAL = BASE * (1 + levySumRate + VAT_RATE*(1+levySumRate))
 *
 * Levy C date rule:
 *  - C applies BEFORE GRA_LEVY_C_END_DATE (default 2026-01-01), else 0
 *
 * Required ENV:
 *  - SAP_BASE, SAP_COMPANY, SAP_USER, SAP_PASS
 * Optional ENV:
 *  - VERIFY_TLS=true|false
 *  - GRA_POST_ENABLED=true|false
 *  - GRA_POST_URL=...
 *  - GRA_SECURITY_KEY=...
 *  - GRA_VAT_RATE=0.15
 *  - GRA_LEVY_RATES_JSON={"A":0.025,"B":0.025,"C":0.01,"D":0,"E":0}
 *  - GRA_LEVY_C_END_DATE=2026-01-01
 *  - GRA_ITEM_CODE_MAP_JSON=...
 * -------------------------------------------------------------------
 */

require("dotenv").config();
const express = require("express");
const axios = require("axios");
const https = require("https");
const fs = require("fs");
const path = require("path");
const session = require("express-session");
const os = require("os");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "3mb" }));

// -------------------- ENV (SAP) --------------------
const SAP_BASE = process.env.SAP_BASE; // e.g. https://host:50000/b1s/v2
const SAP_COMPANY = process.env.SAP_COMPANY;
const SAP_USER = process.env.SAP_USER;
const SAP_PASS = process.env.SAP_PASS;

const POLL_MS = parseInt(process.env.POLL_MS || "5000", 10);
const PORT = parseInt(process.env.PORT || "8000", 10);
const VERIFY_TLS = String(process.env.VERIFY_TLS || "true").toLowerCase() === "true";
const SESSION_SECRET = process.env.SESSION_SECRET || "dev_secret_change_me";

// -------------------- ENV (GRA) --------------------
const GRA_USER_NAME = process.env.GRA_USER_NAME || "Kofi Ghana";
const GRA_FLAG = process.env.GRA_FLAG || "INVOICE";
const GRA_SALE_TYPE = process.env.GRA_SALE_TYPE || "NORMAL";
const GRA_DISCOUNT_TYPE = process.env.GRA_DISCOUNT_TYPE || "GENERAL";
const GRA_DEFAULT_BP_TIN = process.env.GRA_DEFAULT_BP_TIN || "C0000000000";

const GRA_SEND_PO_REFERENCE = String(process.env.GRA_SEND_PO_REFERENCE || "false").toLowerCase() === "true";
const GRA_INVOICE_PREFIX = process.env.GRA_INVOICE_PREFIX || "SAP";
const GRA_LEVY_MODE = String(process.env.GRA_LEVY_MODE || "rates").toLowerCase(); // rates | override

const GRA_VAT_RATE = Number.isFinite(Number(process.env.GRA_VAT_RATE)) ? Number(process.env.GRA_VAT_RATE) : 0.15;
const GRA_LEVY_C_END_DATE = (process.env.GRA_LEVY_C_END_DATE || "2026-01-01").slice(0, 10);

// Posting ENV
const GRA_POST_ENABLED = String(process.env.GRA_POST_ENABLED || "false").toLowerCase() === "true";
const GRA_POST_URL = process.env.GRA_POST_URL || "";
const GRA_SECURITY_KEY = process.env.GRA_SECURITY_KEY || "";

// -------------------- Helpers --------------------
function parseJsonEnv(s, fallback) {
  try {
    return s ? JSON.parse(s) : fallback;
  } catch {
    return fallback;
  }
}

const GRA_ITEM_CODE_MAP = parseJsonEnv(process.env.GRA_ITEM_CODE_MAP_JSON, {});
const GRA_LEVY_RATES_BASE = parseJsonEnv(process.env.GRA_LEVY_RATES_JSON, {
  A: 0.025,
  B: 0.025,
  C: 0.01, // default pre-2026
  D: 0,
  E: 0,
});
const GRA_ITEM_LEVY_OVERRIDE = parseJsonEnv(process.env.GRA_ITEM_LEVY_OVERRIDE_JSON, {});

// Basic checks
if (!SAP_BASE || !SAP_COMPANY || !SAP_USER || !SAP_PASS) {
  console.error("Missing env vars: SAP_BASE, SAP_COMPANY, SAP_USER, SAP_PASS");
  process.exit(1);
}

// -------------------- SESSION --------------------
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: true,
    cookie: { httpOnly: true, secure: false, maxAge: 2 * 60 * 60 * 1000 },
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

// -------------------- FRIENDLY ERRORS --------------------
function normalizeError(err) {
  const out = {
    title: "Something went wrong",
    message: "We couldn’t complete the request. Please try again.",
    detail: "",
    hint: "",
    code: "",
  };
  const raw = err?.message ? String(err.message) : String(err || "");
  out.detail = raw;

  if (raw.includes("Login failed")) {
    out.title = "SAP login failed";
    out.message = "We couldn’t sign in to SAP Service Layer.";
    out.hint = "Check SAP_COMPANY, SAP_USER, SAP_PASS, and SAP_BASE in your .env file.";
    return out;
  }
  if (raw.includes("SAP GET failed")) {
    out.title = "SAP request failed";
    out.message = "SAP didn’t return the data we requested.";
    out.hint = "Check SAP Service Layer reachability, permissions, and DocEntry.";
    return out;
  }
  if (raw.includes("PDF generation failed")) {
    out.title = "PDF export failed";
    out.message = "We couldn’t generate the PDF.";
    out.hint = "Check server logs (Python/reportlab).";
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

const clients = new Set();
function sendToAll(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of clients) res.write(data);
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
app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.write(`data: ${JSON.stringify({ type: "INFO", message: "Live feed connected." })}\n\n`);
  clients.add(res);
  req.on("close", () => clients.delete(res));
});

// -------------------- CACHE --------------------
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

// -------------------- GRA POST RESULT STORE --------------------
const GRA_RESULTS_FILE = path.join(__dirname, "gra_results.json");
const graResults = new Map(); // docEntry -> { savedAt, requestPayload, responsePayload }

function loadGraResults() {
  try {
    const raw = fs.readFileSync(GRA_RESULTS_FILE, "utf-8");
    const json = JSON.parse(raw || "{}");
    for (const [k, v] of Object.entries(json || {})) graResults.set(Number(k), v);
  } catch {}
}
function saveGraResults() {
  const obj = {};
  for (const [k, v] of graResults.entries()) obj[String(k)] = v;
  fs.writeFileSync(GRA_RESULTS_FILE, JSON.stringify(obj, null, 2));
}
loadGraResults();

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

// -------------------- TAX ENGINE HELPERS --------------------
function round2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round((x + Number.EPSILON) * 100) / 100;
}
function money(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || x < 0) return 0.0;
  return round2(x);
}
function safeStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}
function isIsoDate10(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || "").slice(0, 10));
}
function date10FromIsoZ(isoZ) {
  const s = String(isoZ || "");
  return s.slice(0, 10);
}
function isBeforeDate(date10, cutoff10) {
  if (!isIsoDate10(date10) || !isIsoDate10(cutoff10)) return false;
  return date10 < cutoff10;
}

function toIsoZFromSap(inv) {
  const d = safeStr(inv.DocDate || inv.TaxDate || inv.CreationDate);
  const dt = d ? String(d).slice(0, 10) : new Date().toISOString().slice(0, 10);

  const rawTime = inv.DocTime ?? inv.UpdateTime ?? "";
  const t = String(rawTime);

  if (t.includes(":")) {
    const hh = (t.split(":")[0] || "00").padStart(2, "0");
    const mm = (t.split(":")[1] || "00").padStart(2, "0");
    const ss = (t.split(":")[2] || "00").padStart(2, "0");
    return `${dt}T${hh}:${mm}:${ss}Z`;
  }
  return `${dt}T00:00:00Z`;
}

function buildGraInvoiceNumber(inv) {
  const raw = safeStr(inv.DocNum || inv.DocEntry);
  if (!raw) return `${GRA_INVOICE_PREFIX}-0000`;
  if (raw.length >= 4) return raw;
  return `${GRA_INVOICE_PREFIX}-${raw.padStart(4, "0")}`;
}

// Mode-aware line amount (critical for inclusive correctness)
function getLineAmountForMode(ln, calculationType) {
  const mode = String(calculationType || "").toUpperCase();

  if (mode === "INCLUSIVE") {
    const inclusiveCandidates = [ln.GrossTotal, ln.TotalInclTax, ln.U_TotalInclTax, ln.U_GrossTotal];
    for (const c of inclusiveCandidates) {
      const x = Number(c);
      if (Number.isFinite(x) && x >= 0) return money(x);
    }
  }

  const exclusiveCandidates = [ln.LineTotal, ln.RowTotal, ln.GrossTotal, ln.TotalInclTax];
  for (const c of exclusiveCandidates) {
    const x = Number(c);
    if (Number.isFinite(x) && x >= 0) return money(x);
  }

  const qty = Number(ln.Quantity);
  const price = Number(ln.UnitPrice ?? ln.Price);
  if (Number.isFinite(qty) && Number.isFinite(price)) return money(qty * price);

  return 0.0;
}

function resolveGraItemCode(ln) {
  const sapItem = safeStr(ln.ItemCode || ln.U_ItemCode || "");
  const direct = safeStr(ln.U_GRA_ITEM_CODE || "");
  if (direct) return direct;

  const mapped = GRA_ITEM_CODE_MAP[sapItem];
  if (mapped) return String(mapped);

  return sapItem; // fallback (validator warns)
}

function levyOverrideFor(sapItemCode, graItemCode) {
  return GRA_ITEM_LEVY_OVERRIDE[sapItemCode] || GRA_ITEM_LEVY_OVERRIDE[graItemCode] || null;
}

function getLevyRatesForDate(transactionDateIsoZ) {
  const d10 = date10FromIsoZ(transactionDateIsoZ);
  const base = { ...GRA_LEVY_RATES_BASE };
  if (!isBeforeDate(d10, GRA_LEVY_C_END_DATE)) base.C = 0;
  return base;
}

function inclusiveFactor(levySumRate, vatRate) {
  return 1 + levySumRate + vatRate * (1 + levySumRate);
}

function computeLeviesFromBase(base, sapItemCode, graItemCode, rates) {
  const ov = levyOverrideFor(sapItemCode, graItemCode);

  if (GRA_LEVY_MODE === "override" && ov) {
    return {
      A: money(ov.A ?? 0),
      B: money(ov.B ?? 0),
      C: money(ov.C ?? 0),
      D: money(ov.D ?? 0),
      E: money(ov.E ?? 0),
    };
  }

  const A = money(base * Number(rates.A ?? 0));
  const B = money(base * Number(rates.B ?? 0));
  const C = money(base * Number(rates.C ?? 0));
  const D = money(base * Number(rates.D ?? 0));
  const E = money(base * Number(rates.E ?? 0));

  if (ov && typeof ov === "object") {
    return {
      A: ov.A !== undefined ? money(ov.A) : A,
      B: ov.B !== undefined ? money(ov.B) : B,
      C: ov.C !== undefined ? money(ov.C) : C,
      D: ov.D !== undefined ? money(ov.D) : D,
      E: ov.E !== undefined ? money(ov.E) : E,
    };
  }
  return { A, B, C, D, E };
}

function mapSapInvoiceToGra(inv, calculationType) {
  const currency = safeStr(inv.DocCurrency) || "GHS";
  const exchangeRate = Number.isFinite(Number(inv.DocRate)) ? Number(inv.DocRate) : 1.0;

  const invoiceNumber = buildGraInvoiceNumber(inv);
  const transactionDate = toIsoZFromSap(inv);

  const businessPartnerName = safeStr(inv.CardName) || "cash customer";
  const businessPartnerTin = safeStr(inv.U_TIN || inv.U_BP_TIN || inv.BusinessPartnerTin) || GRA_DEFAULT_BP_TIN;
  const purchaseOrderReference = GRA_SEND_PO_REFERENCE ? safeStr(inv.NumAtCard || "") : "";

  const rates = getLevyRatesForDate(transactionDate);
  const levySumRate =
    Number(rates.A || 0) + Number(rates.B || 0) + Number(rates.C || 0) + Number(rates.D || 0) + Number(rates.E || 0);

  const lines = Array.isArray(inv.DocumentLines) ? inv.DocumentLines : [];
  const ct = String(calculationType || "EXCLUSIVE").toUpperCase();

  const items = lines.map((ln) => {
    const sapItemCode = safeStr(ln.ItemCode || ln.U_ItemCode || "");
    const graItemCode = resolveGraItemCode(ln);

    const qty = money(ln.Quantity);
    const unitPrice = money(ln.UnitPrice ?? ln.Price);

    const lineAmount = getLineAmountForMode(ln, ct);

    let base = 0;
    if (ct === "INCLUSIVE") {
      const factor = inclusiveFactor(levySumRate, GRA_VAT_RATE);
      base = factor > 0 ? money(lineAmount / factor) : 0;
    } else {
      base = money(lineAmount);
    }

    const lev = computeLeviesFromBase(base, sapItemCode, graItemCode, rates);

    return {
      itemCode: graItemCode,
      itemCategory: safeStr(ln.U_ItemCategory || ""),
      expireDate: safeStr(ln.U_ExpireDate || ""),
      description: safeStr(ln.ItemDescription || ""),
      quantity: qty,

      levyAmountA: money(lev.A),
      levyAmountB: money(lev.B),
      levyAmountC: money(lev.C),
      levyAmountD: money(lev.D),
      levyAmountE: money(lev.E),

      discountAmount: 0.0,
      batchCode: safeStr(ln.BatchCode || ln.U_BatchCode || ""),
      unitPrice,
    };
  });

  const totalLevy = money(
    items.reduce(
      (sum, it) =>
        sum +
        Number(it.levyAmountA || 0) +
        Number(it.levyAmountB || 0) +
        Number(it.levyAmountC || 0) +
        Number(it.levyAmountD || 0) +
        Number(it.levyAmountE || 0),
      0
    )
  );

  // baseTotal derived using same mode logic (more consistent)
  const factor = inclusiveFactor(levySumRate, GRA_VAT_RATE);
  let baseTotal = 0;
  if (ct === "INCLUSIVE") {
    for (const ln of lines) {
      const amt = getLineAmountForMode(ln, ct);
      baseTotal += factor > 0 ? amt / factor : 0;
    }
  } else {
    for (const ln of lines) baseTotal += getLineAmountForMode(ln, ct);
  }
  baseTotal = money(baseTotal);

  // VAT = VAT_RATE * (BASE + totalLevy)
  const totalVat = money(GRA_VAT_RATE * (baseTotal + totalLevy));

  // totalAmount meaning:
  //  - EXCLUSIVE: baseTotal
  //  - INCLUSIVE: prefer SAP DocTotal if close, else computed inclusive
  let totalAmount = 0;
  if (ct === "INCLUSIVE") {
    const computedInclusive = money(baseTotal + totalLevy + totalVat);
    const docTotal = Number(inv.DocTotal);
    if (Number.isFinite(docTotal) && Math.abs(money(docTotal) - computedInclusive) <= 0.05) totalAmount = money(docTotal);
    else totalAmount = computedInclusive;
  } else {
    totalAmount = baseTotal;
  }

  return {
    currency,
    exchangeRate,
    invoiceNumber,
    totalLevy,
    userName: GRA_USER_NAME,
    flag: GRA_FLAG,
    calculationType: ct,
    totalVat,
    transactionDate,
    totalAmount,
    voucherAmount: 0.0,
    businessPartnerName,
    businessPartnerTin,
    saleType: GRA_SALE_TYPE,
    discountType: GRA_DISCOUNT_TYPE,
    discountAmount: 0.0,
    reference: "",
    groupReferenceId: "",
    purchaseOrderReference,
    items,
  };
}

function validateGraPayload(payload) {
  const issues = [];
  const add = (level, title, detail, hint) => issues.push({ level, title, detail, hint });

  const ct = String(payload.calculationType || "").toUpperCase();
  if (!["INCLUSIVE", "EXCLUSIVE"].includes(ct)) {
    add("error", "Invalid calculationType", `calculationType="${payload.calculationType}"`, `Use "INCLUSIVE" or "EXCLUSIVE".`);
  }

  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!items.length) add("error", "No items", "Invoice has no items.", "Ensure invoice has at least one line item.");

  for (const it of items) {
    const code = String(it.itemCode || "");
    if (!code) add("error", "Missing itemCode", "An item has empty itemCode.", "Map SAP ItemCode -> GRA TXC item code.");
    else if (!code.startsWith("TXC")) {
      add("warn", "Item code not TXC", `itemCode="${code}"`, "GRA typically expects TXC... mapping.");
    }
  }

  const sumLevy = money(
    items.reduce(
      (s, it) =>
        s +
        Number(it.levyAmountA || 0) +
        Number(it.levyAmountB || 0) +
        Number(it.levyAmountC || 0) +
        Number(it.levyAmountD || 0) +
        Number(it.levyAmountE || 0),
      0
    )
  );
  if (money(payload.totalLevy) !== sumLevy) {
    add("error", "totalLevy mismatch", `totalLevy=${money(payload.totalLevy)} but sum(items levies)=${sumLevy}`, "Set totalLevy to the items levy sum.");
  }

  // Date-aware levy C expectation based on our default rule
  const rates = getLevyRatesForDate(payload.transactionDate);
  const expectC = Number(rates.C || 0) > 0;
  if (expectC) {
    const sumC = money(items.reduce((s, it) => s + Number(it.levyAmountC || 0), 0));
    if (sumC <= 0) {
      add(
        "error",
        "Levy C expected but zero",
        `transactionDate=${payload.transactionDate} and levyRateC=${rates.C}, but total levyAmountC=${sumC}`,
        `For dates before ${GRA_LEVY_C_END_DATE}, Levy C is expected by default.`
      );
    }
  }

  // VAT sanity check (approx)
  const levySumRate =
    Number(rates.A || 0) + Number(rates.B || 0) + Number(rates.C || 0) + Number(rates.D || 0) + Number(rates.E || 0);

  let baseApprox = 0;
  if (ct === "EXCLUSIVE") baseApprox = money(payload.totalAmount);
  else {
    const f = inclusiveFactor(levySumRate, GRA_VAT_RATE);
    baseApprox = f > 0 ? money(payload.totalAmount / f) : 0;
  }

  const vatExpected = money(GRA_VAT_RATE * (baseApprox + money(payload.totalLevy)));
  const vatSent = money(payload.totalVat);
  if (Math.abs(vatSent - vatExpected) > 0.02) {
    add(
      "warn",
      "VAT may not match expected",
      `totalVat=${vatSent} but expected≈${vatExpected} (VAT ${(GRA_VAT_RATE * 100).toFixed(0)}% of (base+levy))`,
      "Common cause of E818/E820: VAT must be computed on (value + levies) and rounded consistently."
    );
  }

  return issues;
}

// -------------------- GRA POST (structured result) --------------------
async function graPostInvoice(payload) {
  if (!GRA_POST_ENABLED) return { ok: false, kind: "CONFIG", message: "GRA posting is disabled (set GRA_POST_ENABLED=true)." };
  if (!GRA_POST_URL) return { ok: false, kind: "CONFIG", message: "Missing GRA_POST_URL in .env" };
  if (!GRA_SECURITY_KEY) return { ok: false, kind: "CONFIG", message: "Missing GRA_SECURITY_KEY in .env" };

  const resp = await httpGRA.post(GRA_POST_URL, payload, {
    headers: {
      "Content-Type": "application/json",
      // IMPORTANT: match docs exactly. If your docs say SECURITY_KEY, keep SECURITY_KEY.
      SECURITY_KEY: GRA_SECURITY_KEY,
    },
  });

  const body = resp.data;

  if (resp.status < 200 || resp.status >= 300) {
    return { ok: false, kind: "HTTP_ERROR", httpStatus: resp.status, responseBody: body };
  }

  const status = body?.response?.status;
  if (status && status !== "SUCCESS") {
    return { ok: false, kind: "GRA_REJECTED", httpStatus: resp.status, responseBody: body };
  }

  return { ok: true, httpStatus: resp.status, responseBody: body };
}

// -------------------- PDF EXPORT --------------------
async function buildGraResponsePdf({ docEntry, savedAt, requestPayload, responsePayload }) {
  return new Promise((resolve, reject) => {
    const outPath = path.join(os.tmpdir(), `gra_post_${docEntry}_${Date.now()}.pdf`);
    const payloadObj = { docEntry, savedAt, requestPayload, responsePayload };
    const b64 = Buffer.from(JSON.stringify(payloadObj), "utf-8").toString("base64");

    const py = `
import base64, json, sys
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors

out_path = sys.argv[1]
b64 = sys.argv[2]
data = json.loads(base64.b64decode(b64).decode("utf-8"))

docEntry = data.get("docEntry","")
savedAt = data.get("savedAt","")
reqp = data.get("requestPayload",{}) or {}
respwrap = data.get("responsePayload",{}) or {}
resp = (respwrap.get("response",{}) or {})

status = resp.get("status","")
dist_tin = resp.get("distributor_tin","")
msg = resp.get("message",{}) or {}
qr = resp.get("qr_code","")

c = canvas.Canvas(out_path, pagesize=A4)
w, h = A4

def line(y, label, value):
    c.setFont("Helvetica-Bold", 10)
    c.drawString(20*mm, y, f"{label}:")
    c.setFont("Helvetica", 10)
    c.drawString(60*mm, y, str(value) if value is not None else "")

c.setFont("Helvetica-Bold", 16)
c.drawString(20*mm, h-25*mm, "GRA E-VAT Posting Response")

c.setFont("Helvetica", 10)
c.drawString(20*mm, h-32*mm, "Generated from SAP → GRA integration tool")

y = h-45*mm
line(y, "DocEntry", docEntry); y -= 7*mm
line(y, "Saved At", savedAt); y -= 7*mm

c.setFillColor(colors.whitesmoke)
c.setStrokeColor(colors.lightgrey)
c.roundRect(20*mm, y-18*mm, w-40*mm, 20*mm, 4*mm, stroke=1, fill=1)
c.setFillColor(colors.black)
c.setFont("Helvetica-Bold", 12)
c.drawString(24*mm, y-6*mm, f"Status: {status}")
c.setFont("Helvetica", 10)
c.drawString(24*mm, y-13*mm, f"Distributor TIN: {dist_tin}")
y -= 28*mm

c.setFont("Helvetica-Bold", 12)
c.drawString(20*mm, y, "Response Details"); y -= 8*mm

fields = [
    ("Invoice No", msg.get("num","")),
    ("YSDC ID", msg.get("ysdcid","")),
    ("Record No", msg.get("ysdcrecnum","")),
    ("Internal Data", msg.get("ysdcintdata","")),
    ("Signature", msg.get("ysdcregsig","")),
    ("MRC", msg.get("ysdcmrc","")),
    ("MRC Time", msg.get("ysdcmrctim","")),
    ("YSDC Time", msg.get("ysdctime","")),
    ("Flag", msg.get("flag","")),
    ("Items", msg.get("ysdcitems","")),
]

for k,v in fields:
    if y < 30*mm:
        c.showPage()
        y = h-25*mm
    line(y, k, v); y -= 7*mm

y -= 4*mm
c.setFont("Helvetica-Bold", 12)
c.drawString(20*mm, y, "QR Verification Link"); y -= 8*mm

c.setFont("Helvetica", 9)
qr_str = str(qr) if qr else ""
max_chars = 95
for i in range(0, len(qr_str), max_chars):
    if y < 20*mm:
        c.showPage()
        y = h-25*mm
    c.drawString(20*mm, y, qr_str[i:i+max_chars]); y -= 5*mm

y -= 6*mm
if y < 45*mm:
    c.showPage()
    y = h-25*mm

c.setFont("Helvetica-Bold", 12)
c.drawString(20*mm, y, "Request Totals (Posted Payload)"); y -= 8*mm
line(y, "Invoice Number", reqp.get("invoiceNumber","")); y -= 7*mm
line(y, "Calculation Type", reqp.get("calculationType","")); y -= 7*mm
line(y, "Total Amount", reqp.get("totalAmount","")); y -= 7*mm
line(y, "Total Levy", reqp.get("totalLevy","")); y -= 7*mm
line(y, "Total VAT", reqp.get("totalVat","")); y -= 7*mm
line(y, "Transaction Date", reqp.get("transactionDate","")); y -= 7*mm

c.save()
print(out_path)
`;

    const child = spawn("python3", ["-c", py, outPath, b64], { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    child.on("close", (code) => {
      if (code !== 0) return reject(new Error(`PDF generation failed (code ${code}): ${stderr || stdout}`));
      resolve(outPath);
    });
  });
}

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

// -------------------- "GRA" SESSION (selection + settings) --------------------
function ensureGraSession(req) {
  if (!req.session.gra) req.session.gra = { lastSelection: [], calculationType: "EXCLUSIVE" };
  if (!req.session.gra.calculationType) req.session.gra.calculationType = "EXCLUSIVE";
}

app.get("/api/gra/selection", (req, res) => {
  ensureGraSession(req);
  res.json({ lastSelection: req.session.gra.lastSelection || [] });
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

app.get("/api/gra/config", (req, res) => {
  ensureGraSession(req);
  res.json({ calculationType: req.session.gra.calculationType });
});

app.post("/api/gra/config", (req, res) => {
  ensureGraSession(req);
  const ct = String(req.body.calculationType || "").toUpperCase();
  if (!["INCLUSIVE", "EXCLUSIVE"].includes(ct)) return sendApiError(res, new Error("Invalid calculationType"), 400);
  req.session.gra.calculationType = ct;
  res.json({ ok: true, calculationType: ct });
});

// Preview payload
app.get("/api/gra/payload/:docEntry", async (req, res) => {
  try {
    ensureGraSession(req);
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);

    let inv = cacheGet(docEntry);
    if (!inv) {
      inv = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, inv);
    }

    const calculationType = req.session.gra.calculationType || "EXCLUSIVE";
    const payload = mapSapInvoiceToGra(inv, calculationType);
    const issues = validateGraPayload(payload);

    res.json({
      ok: true,
      docEntry,
      calculationType,
      payload,
      issues,
      debug: {
        postEnabled: GRA_POST_ENABLED,
        vatRate: GRA_VAT_RATE,
        levyRatesForDate: getLevyRatesForDate(payload.transactionDate),
        levyCEndDate: GRA_LEVY_C_END_DATE,
        levyMode: GRA_LEVY_MODE,
        itemMapKeys: Object.keys(GRA_ITEM_CODE_MAP).length,
      },
    });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

// Post to GRA (with blocking checks + real error body)
app.post("/api/gra/post/:docEntry", async (req, res) => {
  try {
    ensureGraSession(req);
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);

    let inv = cacheGet(docEntry);
    if (!inv) {
      inv = await sapGet(`/Invoices(${docEntry})`);
      cacheSet(docEntry, inv);
    }

    const calculationType = req.session.gra.calculationType || "EXCLUSIVE";
    const payload = mapSapInvoiceToGra(inv, calculationType);
    const issues = validateGraPayload(payload);

    const blocking = (issues || []).filter((i) => i.level === "error");
    if (blocking.length) {
      return res.status(400).json({
        ok: false,
        blocked: true,
        reason: "Tax checks failed. Fix errors before posting.",
        issues,
        payload,
      });
    }

    const post = await graPostInvoice(payload);

    if (!post.ok) {
      return res.status(400).json({
        ok: false,
        blocked: false,
        postingFailed: true,
        kind: post.kind,
        httpStatus: post.httpStatus || null,
        responseBody: post.responseBody || null,
        message: post.message || "GRA rejected the request.",
        payload,
      });
    }

    const responsePayload = post.responseBody;

    graResults.set(docEntry, {
      savedAt: new Date().toISOString(),
      requestPayload: payload,
      responsePayload,
    });
    saveGraResults();

    res.json({ ok: true, docEntry, response: responsePayload });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

app.get("/api/gra/post-result/:docEntry", (req, res) => {
  try {
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);
    const rec = graResults.get(docEntry) || null;
    res.json({ ok: true, docEntry, result: rec });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

// PDF export
app.get("/api/gra/post-result/:docEntry/pdf", async (req, res) => {
  try {
    const docEntry = Number(req.params.docEntry);
    if (!Number.isFinite(docEntry) || docEntry <= 0) return sendApiError(res, new Error("Invalid DocEntry"), 400);

    const rec = graResults.get(docEntry);
    if (!rec) return sendApiError(res, new Error("No saved post result for this DocEntry yet."), 404);

    const pdfPath = await buildGraResponsePdf({
      docEntry,
      savedAt: rec.savedAt,
      requestPayload: rec.requestPayload,
      responsePayload: rec.responsePayload,
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="gra_post_${docEntry}.pdf"`);

    fs.createReadStream(pdfPath).pipe(res).on("close", () => {
      try {
        fs.unlinkSync(pdfPath);
      } catch {}
    });
  } catch (err) {
    sendApiError(res, err, 500);
  }
});

// -------------------- UI --------------------
function uiCss() {
  return `
  :root{
    --bg:#f5f7fb; --card:#fff; --text:#0f172a; --muted:#64748b; --border:#e2e8f0;
    --shadow: 0 10px 25px rgba(15, 23, 42, 0.06); --radius:14px;
    --primary:#1d4ed8; --primaryHover:#1e40af; --warning:#d97706; --danger:#dc2626; --ok:#16a34a;
    --chipBg:#f1f5f9; --chipText:#334155;
    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:var(--sans);background:var(--bg);color:var(--text)}
  a{color:var(--primary);text-decoration:none} a:hover{text-decoration:underline}
  .layout{min-height:100vh;display:grid;grid-template-columns:260px 1fr}
  .sidebar{background:#0b1220;color:#e5e7eb;padding:18px 16px}
  .brand{display:flex;gap:10px;align-items:center;padding:10px 10px 14px;border-bottom:1px solid rgba(226,232,240,.10);margin-bottom:14px}
  .mark{width:34px;height:34px;border-radius:10px;background:rgba(255,255,255,.10);display:flex;align-items:center;justify-content:center;font-weight:900}
  .navList{display:flex;flex-direction:column;gap:8px}
  .navItem{display:flex;justify-content:space-between;align-items:center;padding:10px 12px;border-radius:12px;color:#e5e7eb;border:1px solid rgba(226,232,240,.10)}
  .navItem.active{background:rgba(29,78,216,.25);border-color:rgba(59,130,246,.35)}
  .navHint{font-size:12px;color:rgba(229,231,235,.65)}
  .content{padding:22px 22px 50px}
  .topbar{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);
    padding:14px 16px;display:flex;justify-content:space-between;gap:12px;position:sticky;top:12px;z-index:10}
  .topTitle{font-weight:900}
  .topMeta{color:var(--muted);font-size:12px;line-height:1.35}
  .page{margin-top:14px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:16px}
  .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
  .grid{display:grid;gap:14px}
  .grid2{grid-template-columns:1fr 1fr}
  .chip{display:inline-flex;gap:8px;align-items:center;padding:7px 10px;border-radius:999px;background:var(--chipBg);color:var(--chipText);font-size:12px;border:1px solid var(--border)}
  button,select,input{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:#fff}
  button{cursor:pointer}
  .btnPrimary{background:var(--primary);color:#fff;border-color:var(--primary)} .btnPrimary:hover{background:var(--primaryHover)}
  .btnWarn{background:#fffbeb;color:#92400e;border-color:#fde68a}
  .btnDanger{background:var(--danger);color:#fff;border-color:var(--danger)}
  .btnOk{background:var(--ok);color:#fff;border-color:var(--ok)}
  .alert{display:flex;gap:12px;align-items:flex-start;padding:12px;border-radius:12px;border:1px solid var(--border);background:#fff}
  .alertIcon{width:34px;height:34px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-weight:900}
  .alertError{border-color:#fecaca} .alertError .alertIcon{background:#fee2e2;color:#7f1d1d}
  .alertWarn{border-color:#fde68a} .alertWarn .alertIcon{background:#fffbeb;color:#92400e}
  .alertOk{border-color:#bbf7d0} .alertOk .alertIcon{background:#dcfce7;color:#14532d}
  .tableWrap{margin-top:12px;max-height:70vh;overflow:auto;border-radius:12px}
  table{width:100%;border-collapse:separate;border-spacing:0;border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#fff;min-width:760px}
  thead th{font-size:12px;color:#64748b;padding:12px;background:#f8fafc;border-bottom:1px solid var(--border);text-align:left;position:sticky;top:0}
  tbody td{padding:12px;border-bottom:1px solid var(--border)}
  pre{background:#0b1020;color:#e6edf3;padding:12px;border-radius:12px;overflow:auto;max-height:70vh;font-family:var(--mono);font-size:12px}
  img.qr{max-width:240px;width:100%;height:auto;border-radius:12px;border:1px solid var(--border);background:#fff;padding:8px}

  /* Responsive */
  @media (max-width:980px){
    .layout{grid-template-columns:1fr}
    .sidebar{position:sticky;top:0;z-index:30;padding:14px 12px}
    .brand{margin-bottom:10px}
    .navList{flex-direction:row;flex-wrap:wrap}
    .navItem{flex:1 1 160px}
    .content{padding:14px 12px 30px}
    .topbar{position:relative;top:0}
    .grid2{grid-template-columns:1fr}
    table{min-width:720px}
  }
  @media (max-width:520px){
    .topbar{flex-direction:column;align-items:flex-start}
    .navItem{flex:1 1 100%}
    table{min-width:680px}
  }
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

function renderShell({ title, active, topTitle, topMeta, bodyHtml }) {
  const isActive = (k) => (k === active ? "navItem active" : "navItem");
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>${escapeHtml(title)}</title>
  <style>${uiCss()}</style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">
        <div class="mark">SL</div>
        <div>
          <div style="font-weight:900;">SAP → GRA E-VAT</div>
          <div class="navHint">Invoice Monitor</div>
        </div>
      </div>
      <nav class="navList">
        <a class="${isActive("live")}" href="/"><span>Live</span><span class="navHint">SSE</span></a>
        <a class="${isActive("invoices")}" href="/invoices"><span>Invoices</span><span class="navHint">List</span></a>
        <a class="${isActive("gra")}" href="/gra"><span>Payload</span><span class="navHint">Checks</span></a>
        <a class="${isActive("post")}" href="/post-gra"><span>Post</span><span class="navHint">GRA</span></a>
      </nav>
      <div style="margin-top:14px;padding:12px;border:1px solid rgba(226,232,240,.10);border-radius:12px">
        <div class="navHint">Posting</div>
        <div style="font-weight:900;margin-top:4px">${GRA_POST_ENABLED ? "ENABLED" : "DISABLED"}</div>
        <div class="navHint" style="margin-top:10px">Levy C cutoff</div>
        <div style="font-weight:900;margin-top:4px">${escapeHtml(GRA_LEVY_C_END_DATE)}</div>
      </div>
    </aside>

    <main class="content">
      <div class="topbar">
        <div>
          <div class="topTitle">${escapeHtml(topTitle)}</div>
          <div class="topMeta">${escapeHtml(topMeta)}</div>
        </div>
        <div class="row">
          <span class="chip">TLS Verify: <b>${VERIFY_TLS ? "ON" : "OFF"}</b></span>
          <span class="chip">VAT: <b>${Math.round(GRA_VAT_RATE * 100)}%</b></span>
          <span class="chip">Levy mode: <b>${escapeHtml(GRA_LEVY_MODE)}</b></span>
        </div>
      </div>
      <div class="page">${bodyHtml}</div>
      <div class="topMeta" style="margin-top:12px">
        Tip: If SAP has a self-signed certificate in dev, set VERIFY_TLS=false (testing only).
      </div>
    </main>
  </div>
</body>
</html>`;
}

// -------------------- UI ROUTES --------------------
app.get("/", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between">
        <div>
          <div style="font-weight:900;font-size:16px">Live invoice feed</div>
          <div class="topMeta" style="margin-top:6px">New invoices detected by polling and pushed here.</div>
        </div>
        <div class="row">
          <span class="chip">Polling: <b>${POLL_MS}ms</b></span>
          <span class="chip">Cache: <b>30 mins</b></span>
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
              <div style="font-weight:900">\${title}</div>
              <div style="margin-top:3px">\${message}</div>
              \${hint ? '<div class="topMeta" style="margin-top:6px">Tip: ' + hint + '</div>' : ''}
            </div>
          </div>\`;
      }
      function clearNotice(){ notice.innerHTML = ""; }
      function fmt(v){ return (v === null || v === undefined) ? "" : String(v); }

      es.onmessage = (evt) => {
        const msg = JSON.parse(evt.data);
        if (msg.type === "INFO") clearNotice();

        if (msg.type === "NEW_INVOICE") {
          clearNotice();
          const el = document.createElement("div");
          el.className = "card";
          el.innerHTML = \`
            <div class="row" style="justify-content:space-between">
              <div class="chip">New invoice</div>
              <div class="topMeta">DocEntry: <b>\${msg.docEntry}</b> • DocNum: <b>\${fmt(msg.docNum)}</b></div>
            </div>
            <div style="margin-top:10px">
              <div style="font-size:16px;font-weight:900">\${fmt(msg.cardName) || "—"}</div>
              <div class="topMeta">Total: <b>\${fmt(msg.docTotal)}</b> \${fmt(msg.docCurrency)}</div>
            </div>
            <div class="row" style="margin-top:12px">
              <a class="chip" href="/invoice/\${msg.docEntry}">Open invoice</a>
              <a class="chip" href="/gra#docEntry=\${msg.docEntry}">Payload</a>
              <a class="chip" href="/post-gra#docEntry=\${msg.docEntry}">Post</a>
            </div>\`;
          log.prepend(el);
        }

        if (msg.type === "ERROR") {
          showErrorBox("Live feed error", msg.message || "Something went wrong.", "Check SAP connection and try again.");
        }
      };
    </script>
  `;
  res.send(renderShell({ title: "Live Monitor", active: "live", topTitle: "Live Monitor", topMeta: "Real-time feed of newly detected SAP invoices.", bodyHtml }));
});

app.get("/invoices", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between">
        <div>
          <div style="font-weight:900;font-size:16px">Invoices</div>
          <div class="topMeta" style="margin-top:6px">Select invoices to preview & post.</div>
        </div>
        <div class="row">
          <span class="chip">Posting: <b>${GRA_POST_ENABLED ? "ON" : "OFF"}</b></span>
        </div>
      </div>

      <div class="row" style="margin-top:14px">
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

        <button class="btnWarn" id="goPreview">Go to Payload</button>
        <button class="btnOk" id="goPost">Go to Post</button>
      </div>

      <div id="meta" class="topMeta" style="margin-top:12px"></div>
      <div class="tableWrap" id="tableWrap"></div>
      <div class="topMeta" style="margin-top:10px">Tip: table scrolls horizontally on mobile.</div>
    </div>

    <script>
      let page = 1;
      let pageSize = 10;
      const selected = new Set();
      const metaEl = document.getElementById("meta");
      const tableWrap = document.getElementById("tableWrap");
      const pageNumEl = document.getElementById("pageNum");
      const pageSizeEl = document.getElementById("pageSize");

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      async function loadServerSelection(){
        try{
          const s = await fetchJson("/api/gra/selection");
          (s.lastSelection || []).forEach(d => selected.add(Number(d)));
        }catch(e){}
      }

      async function syncSelectionToServer(){
        await fetchJson("/api/gra/select", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ docEntries: Array.from(selected) })
        });
      }

      function safe(v){ return (v === null || v === undefined) ? "" : String(v); }
      function d10(x){ return x ? String(x).slice(0,10) : ""; }

      async function loadSummary(){
        pageNumEl.textContent = String(page);
        const data = await fetchJson(\`/api/invoices/summary?page=\${page}&pageSize=\${pageSize}\`);
        const rows = data.value || [];

        metaEl.textContent = rows.length ? \`Loaded \${rows.length} invoices • Selected: \${selected.size}\` : "No invoices found";

        tableWrap.innerHTML = \`
          <table>
            <thead>
              <tr>
                <th style="width:70px;">Select</th>
                <th>DocEntry</th>
                <th>DocNum</th>
                <th>Customer</th>
                <th>Total</th>
                <th>Date</th>
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
                    <td>\${safe(r.CardName)}</td>
                    <td><b>\${safe(r.DocTotal)}</b> \${safe(r.DocCurrency)}</td>
                    <td>\${d10(r.DocDate)}</td>
                  </tr>\`;
              }).join("")}
            </tbody>
          </table>\`;
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

      document.getElementById("reload").addEventListener("click", loadSummary);
      document.getElementById("prev").addEventListener("click", () => { if (page > 1){ page--; loadSummary(); } });
      document.getElementById("next").addEventListener("click", () => { page++; loadSummary(); });

      pageSizeEl.addEventListener("change", () => {
        pageSize = parseInt(pageSizeEl.value, 10);
        page = 1;
        loadSummary();
      });

      document.getElementById("goPreview").addEventListener("click", async () => {
        if (!selected.size){ alert("Select at least one invoice first."); return; }
        await syncSelectionToServer();
        location.href = "/gra";
      });

      document.getElementById("goPost").addEventListener("click", async () => {
        if (!selected.size){ alert("Select at least one invoice first."); return; }
        await syncSelectionToServer();
        location.href = "/post-gra";
      });

      (async function init(){
        await loadServerSelection();
        await loadSummary();
      })();
    </script>
  `;
  res.send(renderShell({ title: "Invoices", active: "invoices", topTitle: "Invoices", topMeta: "Browse, select, and open full invoice details.", bodyHtml }));
});

app.get("/invoice/:docEntry", (req, res) => {
  const docEntry = Number(req.params.docEntry || 0);

  const bodyHtml = `
    <div class="row" style="margin-bottom:12px">
      <span class="chip">DocEntry: <b id="docEntry">${docEntry}</b></span>
      <a class="chip" href="/gra#docEntry=${docEntry}">Payload + Checks</a>
      <a class="chip" href="/post-gra#docEntry=${docEntry}">Post to GRA</a>
    </div>

    <div class="grid grid2">
      <div class="card">
        <div style="font-weight:900;font-size:16px">GRA payload (STRICT)</div>
        <div class="topMeta" style="margin-top:6px">Preview + computed totals.</div>
        <pre id="payloadOut" style="margin-top:10px">Loading...</pre>
      </div>

      <div class="card">
        <div style="font-weight:900;font-size:16px">Raw SAP Invoice JSON</div>
        <div class="topMeta" style="margin-top:6px">Exact SAP response.</div>
        <pre id="rawOut" style="margin-top:10px">Loading...</pre>
      </div>
    </div>

    <script>
      const docEntry = Number(document.getElementById("docEntry").textContent || "0");
      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }
      (async function(){
        try{
          const raw = await fetchJson("/api/invoice/" + docEntry + "/raw");
          document.getElementById("rawOut").textContent = JSON.stringify(raw.data, null, 2);

          const gra = await fetchJson("/api/gra/payload/" + docEntry);
          document.getElementById("payloadOut").textContent = JSON.stringify(gra.payload, null, 2);
        }catch(e){
          document.getElementById("payloadOut").textContent = "Failed to load.";
        }
      })();
    </script>
  `;
  res.send(renderShell({ title: `Invoice ${docEntry}`, active: "invoices", topTitle: "Invoice details", topMeta: "Strict GRA payload preview + raw SAP JSON.", bodyHtml }));
});

app.get("/gra", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between">
        <div>
          <div style="font-weight:900;font-size:16px">GRA payload preview + tax checks</div>
          <div class="topMeta" style="margin-top:6px">Preview the mapped payload for selected invoices.</div>
        </div>
        <div class="row">
          <a class="chip" href="/invoices">Back to invoices</a>
          <a class="chip" href="/post-gra">Go to posting</a>
        </div>
      </div>

      <div class="row" style="margin-top:14px">
        <span class="chip">Tax mode</span>
        <select id="calcType">
          <option value="EXCLUSIVE">EXCLUSIVE (tax added)</option>
          <option value="INCLUSIVE">INCLUSIVE (tax inside total)</option>
        </select>
        <button class="btnPrimary" id="apply">Apply + Recompute</button>
      </div>

      <div id="selList" style="margin-top:14px;"></div>

      <div style="margin-top:14px;font-weight:900;">Pre-post checks</div>
      <div id="checks" style="margin-top:10px;"></div>

      <div style="margin-top:14px;font-weight:900;">Selected invoice payload</div>
      <pre id="payloadOut" style="margin-top:10px;">Loading...</pre>
    </div>

    <script>
      const selList = document.getElementById("selList");
      const payloadOut = document.getElementById("payloadOut");
      const checks = document.getElementById("checks");
      const calcType = document.getElementById("calcType");

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      function renderSelection(sel){
        if (!sel.length){
          selList.innerHTML = '<div class="topMeta">(none — go to Invoices and select some)</div>';
          payloadOut.textContent = "No selection.";
          checks.innerHTML = "";
          return;
        }

        selList.innerHTML = sel.map(d => \`
          <div class="row" style="justify-content:space-between;padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:#fff;margin-bottom:8px">
            <div>
              <div style="font-weight:900">DocEntry: \${d}</div>
              <div class="topMeta"><a href="/invoice/\${d}">Open invoice</a> • <a href="/post-gra#docEntry=\${d}">Post</a></div>
            </div>
            <button class="btnDanger" data-remove="\${d}">Remove</button>
          </div>\`).join("");

        selList.querySelectorAll("button[data-remove]").forEach(btn => {
          btn.addEventListener("click", async () => {
            const docEntry = Number(btn.getAttribute("data-remove"));
            await fetchJson("/api/gra/remove", {
              method:"POST",
              headers:{ "Content-Type":"application/json" },
              body: JSON.stringify({ docEntry })
            });
            await load();
          });
        });
      }

      function issueBox(i){
        const cls = i.level === "error" ? "alert alertError" : (i.level === "warn" ? "alert alertWarn" : "alert alertOk");
        const icon = i.level === "error" ? "!" : (i.level === "warn" ? "!" : "✓");
        return \`
          <div class="\${cls}" style="margin-bottom:10px">
            <div class="alertIcon">\${icon}</div>
            <div>
              <div style="font-weight:900">\${i.title}</div>
              <div style="margin-top:3px">\${i.detail || ""}</div>
              \${i.hint ? '<div class="topMeta" style="margin-top:6px">Tip: ' + i.hint + '</div>' : ''}
            </div>
          </div>\`;
      }

      function renderChecks(issues){
        if (!issues || !issues.length){
          checks.innerHTML = \`
            <div class="alert alertOk">
              <div class="alertIcon">✓</div>
              <div>
                <div style="font-weight:900">No issues detected</div>
                <div style="margin-top:3px">Payload looks consistent for the selected tax mode.</div>
              </div>
            </div>\`;
          return;
        }
        checks.innerHTML = issues.map(issueBox).join("");
      }

      async function loadConfig(){
        const cfg = await fetchJson("/api/gra/config");
        calcType.value = cfg.calculationType || "EXCLUSIVE";
      }

      async function applyConfig(){
        await fetchJson("/api/gra/config", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ calculationType: calcType.value })
        });
      }

      async function load(){
        await loadConfig();

        const selResp = await fetchJson("/api/gra/selection");
        const sel = selResp.lastSelection || [];
        renderSelection(sel);

        const hash = new URLSearchParams((location.hash || "").replace("#",""));
        const fromHash = hash.get("docEntry");
        const useDocEntry = fromHash ? Number(fromHash) : (sel[0] ? Number(sel[0]) : null);

        if (!useDocEntry){
          payloadOut.textContent = "No invoice selected.";
          checks.innerHTML = "";
          return;
        }

        const data = await fetchJson("/api/gra/payload/" + useDocEntry);
        renderChecks(data.issues || []);
        payloadOut.textContent = JSON.stringify(data.payload, null, 2);
      }

      document.getElementById("apply").addEventListener("click", async () => {
        await applyConfig();
        await load();
      });

      load();
    </script>
  `;
  res.send(renderShell({ title: "GRA Payload Preview", active: "gra", topTitle: "GRA Payload Preview", topMeta: "Preview mapping with tax checks.", bodyHtml }));
});

app.get("/post-gra", (req, res) => {
  const bodyHtml = `
    <div class="card">
      <div class="row" style="justify-content:space-between">
        <div>
          <div style="font-weight:900;font-size:16px">Post to GRA + show response</div>
          <div class="topMeta" style="margin-top:6px">
            Posting is: <b>${GRA_POST_ENABLED ? "ENABLED" : "DISABLED"}</b>. This page blocks posting when checks fail.
          </div>
        </div>
        <div class="row">
          <a class="chip" href="/invoices">Back to invoices</a>
          <a class="chip" href="/gra">Payload checks</a>
        </div>
      </div>

      <div class="row" style="margin-top:14px">
        <span class="chip">Tax mode</span>
        <select id="calcType">
          <option value="EXCLUSIVE">EXCLUSIVE (tax added)</option>
          <option value="INCLUSIVE">INCLUSIVE (tax inside total)</option>
        </select>
        <button class="btnPrimary" id="apply">Apply</button>

        <span class="chip">Selected DocEntry</span>
        <select id="docSelect"></select>

        <button class="btnOk" id="postBtn">Post to GRA</button>
        <span class="topMeta" id="postHint"></span>
      </div>

      <div style="margin-top:14px;font-weight:900;">Checks (must be clean to post)</div>
      <div id="checks" style="margin-top:10px;"></div>

      <div class="grid grid2" style="margin-top:14px">
        <div class="card">
          <div style="font-weight:900">Payload being posted</div>
          <div class="topMeta" style="margin-top:6px">Exactly what will be sent to GRA.</div>
          <pre id="payloadOut" style="margin-top:10px;">Loading...</pre>
        </div>
        <div class="card">
          <div style="font-weight:900">GRA response</div>
          <div class="topMeta" style="margin-top:6px">Shows SUCCESS response and PDF export when available.</div>
          <div id="postResult" style="margin-top:10px;"></div>
          <pre id="respRaw" style="margin-top:10px;">(no response yet)</pre>
        </div>
      </div>
    </div>

    <script>
      const calcType = document.getElementById("calcType");
      const docSelect = document.getElementById("docSelect");
      const postBtn = document.getElementById("postBtn");
      const postHint = document.getElementById("postHint");
      const payloadOut = document.getElementById("payloadOut");
      const checks = document.getElementById("checks");
      const postResult = document.getElementById("postResult");
      const respRaw = document.getElementById("respRaw");

      async function fetchJson(url, opts){
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) throw data;
        return data;
      }

      function issueBox(i){
        const cls = i.level === "error" ? "alert alertError" : (i.level === "warn" ? "alert alertWarn" : "alert alertOk");
        const icon = i.level === "error" ? "!" : (i.level === "warn" ? "!" : "✓");
        return \`
          <div class="\${cls}" style="margin-bottom:10px">
            <div class="alertIcon">\${icon}</div>
            <div>
              <div style="font-weight:900">\${i.title}</div>
              <div style="margin-top:3px">\${i.detail || ""}</div>
              \${i.hint ? '<div class="topMeta" style="margin-top:6px">Tip: ' + i.hint + '</div>' : ''}
            </div>
          </div>\`;
      }

      function renderChecks(issues){
        if (!issues || !issues.length){
          checks.innerHTML = \`
            <div class="alert alertOk">
              <div class="alertIcon">✓</div>
              <div>
                <div style="font-weight:900">No issues detected</div>
                <div style="margin-top:3px">Posting is allowed.</div>
              </div>
            </div>\`;
          return;
        }
        checks.innerHTML = issues.map(issueBox).join("");
      }

      function renderPostResult(rec){
        if(!rec){
          postResult.innerHTML = "";
          respRaw.textContent = "(no response yet)";
          return;
        }

        respRaw.textContent = JSON.stringify(rec.responsePayload, null, 2);

        const status = rec?.responsePayload?.response?.status || "";
        const msg = rec?.responsePayload?.response?.message || {};
        const qr = rec?.responsePayload?.response?.qr_code || "";

        const ok = status === "SUCCESS";
        const cls = ok ? "alert alertOk" : "alert alertWarn";
        const icon = ok ? "✓" : "!";

        const qrImg = qr ? ("https://api.qrserver.com/v1/create-qr-code/?size=240x240&data=" + encodeURIComponent(qr)) : "";
        const de = Number(docSelect.value || "0");

        postResult.innerHTML = \`
          <div class="\${cls}">
            <div class="alertIcon">\${icon}</div>
            <div style="width:100%">
              <div style="font-weight:900">Status: \${status || "—"}</div>
              <div class="topMeta" style="margin-top:6px">
                Receipt: <b>\${msg.ysdcrecnum || "—"}</b> • Signature: <b>\${msg.ysdcregsig || "—"}</b>
              </div>
              <div class="topMeta" style="margin-top:6px">
                Time: <b>\${msg.ysdctime || msg.ysdcmrctim || "—"}</b> • Items: <b>\${msg.ysdcitems || "—"}</b>
              </div>

              \${qr ? \`
                <div class="row" style="margin-top:10px">
                  <a class="chip" href="\${qr}" target="_blank" rel="noreferrer">Open verification link</a>
                </div>
              \` : ""}

              \${qrImg ? \`
                <div style="margin-top:10px">
                  <img class="qr" src="\${qrImg}" alt="QR Code"/>
                </div>
              \` : ""}

              \${ok ? \`
                <div class="row" style="margin-top:10px">
                  <a class="chip" href="/api/gra/post-result/\${de}/pdf" target="_blank" rel="noreferrer">Export response as PDF</a>
                </div>
              \` : ""}

              <div class="topMeta" style="margin-top:10px">Saved at: \${rec.savedAt || "—"}</div>
            </div>
          </div>\`;
      }

      function renderPostError(e){
        const kind = e?.kind || "UNKNOWN";
        const httpStatus = e?.httpStatus || "";
        const body = e?.responseBody;

        const code = body?.code || body?.response?.code || "";
        const msg = body?.message || body?.response?.message || e?.message || "Posting failed.";

        const pretty = body ? JSON.stringify(body, null, 2) : "";

        postResult.innerHTML = \`
          <div class="alert alertError">
            <div class="alertIcon">!</div>
            <div style="width:100%">
              <div style="font-weight:900">GRA Posting Failed</div>
              <div class="topMeta" style="margin-top:6px">
                Type: <b>\${kind}</b> \${httpStatus ? '• HTTP: <b>' + httpStatus + '</b>' : ''}
              </div>
              \${code || msg ? '<div style="margin-top:8px"><b>' + (code||'') + '</b> ' + (msg ? '— ' + msg : '') + '</div>' : ''}
              \${pretty ? '<pre style="margin-top:10px">' + pretty + '</pre>' : ''}
            </div>
          </div>\`;

        respRaw.textContent = "(posting failed)";
      }

      async function loadConfig(){
        const cfg = await fetchJson("/api/gra/config");
        calcType.value = cfg.calculationType || "EXCLUSIVE";
      }

      async function applyConfig(){
        await fetchJson("/api/gra/config", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ calculationType: calcType.value })
        });
      }

      async function loadSelection(){
        const selResp = await fetchJson("/api/gra/selection");
        const sel = selResp.lastSelection || [];

        const hash = new URLSearchParams((location.hash || "").replace("#",""));
        const fromHash = hash.get("docEntry");
        const preferred = fromHash ? Number(fromHash) : (sel[0] ? Number(sel[0]) : null);

        docSelect.innerHTML = sel.length
          ? sel.map(d => \`<option value="\${d}">\${d}</option>\`).join("")
          : \`<option value="">(no selection)</option>\`;

        if (preferred && sel.includes(preferred)) docSelect.value = String(preferred);
      }

      async function refreshPreview(){
        const docEntry = Number(docSelect.value || "0");
        if (!docEntry){
          payloadOut.textContent = "No invoice selected.";
          checks.innerHTML = "";
          renderPostResult(null);
          return;
        }

        const data = await fetchJson("/api/gra/payload/" + docEntry);
        renderChecks(data.issues || []);
        payloadOut.textContent = JSON.stringify(data.payload, null, 2);

        const last = await fetchJson("/api/gra/post-result/" + docEntry);
        renderPostResult(last.result);
      }

      document.getElementById("apply").addEventListener("click", async () => {
        await applyConfig();
        await refreshPreview();
      });

      docSelect.addEventListener("change", refreshPreview);

      postBtn.addEventListener("click", async () => {
        const docEntry = Number(docSelect.value || "0");
        if (!docEntry){ alert("No invoice selected."); return; }

        postHint.textContent = "Posting...";
        postBtn.disabled = true;

        try{
          await fetchJson("/api/gra/post/" + docEntry, { method: "POST" });
          postHint.textContent = "Posted successfully.";
          await refreshPreview();
        }catch(e){
          postHint.textContent = "";
          if (e?.blocked && e?.issues){
            renderChecks(e.issues || []);
            if (e.payload) payloadOut.textContent = JSON.stringify(e.payload, null, 2);
            postResult.innerHTML = "";
            respRaw.textContent = "(blocked by checks)";
            alert("Posting blocked: fix the tax errors shown in the checks.");
          } else {
            renderPostError(e);
          }
        }finally{
          postBtn.disabled = false;
        }
      });

      (async function init(){
        await loadConfig();
        await loadSelection();
        await refreshPreview();
      })();
    </script>
  `;
  res.send(renderShell({ title: "Post to GRA", active: "post", topTitle: "Post to GRA", topMeta: "Select tax mode, post invoices, view SUCCESS response, export PDF.", bodyHtml }));
});

// -------------------- START --------------------
app.listen(process.env.PORT || PORT, () => {
  console.log(`Running on http://localhost:${process.env.PORT || PORT}`);
  console.log(`Polling every ${POLL_MS}ms`);
  console.log(`Posting enabled: ${GRA_POST_ENABLED}`);
  console.log(`VAT rate: ${GRA_VAT_RATE}`);
  console.log(`Levy C cutoff (exclusive): ${GRA_LEVY_C_END_DATE}`);
  console.log(`Item map keys: ${Object.keys(GRA_ITEM_CODE_MAP).length}`);
});