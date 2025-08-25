// index.js — Bright Data → append to Google Sheet (Sheet1) with dynamic headers from response
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 8086;

app.use(cors());
app.use(express.json());

// ===== Bright Data config =====
const BDATA_API = 'https://api.brightdata.com/datasets/v3';
const BDATA_KEY = process.env.BRIGHT_DATA_API_KEY;
const DS_LINKEDIN = normalizeDatasetId(process.env.BRIGHTDATA_LINKEDIN_DATASET_ID); // e.g. gd_lpfll7v5hcqtkxl6l

if (!BDATA_KEY) console.warn('⚠️ BRIGHT_DATA_API_KEY not set');
if (!DS_LINKEDIN) console.warn('⚠️ BRIGHTDATA_LINKEDIN_DATASET_ID not set or invalid (expect "gd_...")');

// ===== Small helpers =====
function normalizeDatasetId(v) {
  const m = String(v || '').match(/\bgd_[a-z0-9]+/i);
  return m ? m[0] : '';
}
function locationFromCountry(country) {
  const c = String(country || '').trim().toUpperCase();
  const map = {
    US: 'United States',
    IN: 'India',
    GB: 'United Kingdom',
    CA: 'Canada',
    AU: 'Australia',
    FR: 'France',
    DE: 'Germany',
  };
  return map[c] || 'United States';
}
function isValidationErr(text) {
  return /validation_error|This input should not contain|Required field/i.test(String(text || ''));
}

// ===== Polling helpers =====
// Force JSON array by default; allow tuning batch/part if ever needed.
const SNAPSHOT_URL = (id, { format = 'json', batchSize, part } = {}) => {
  const qs = new URLSearchParams({ format });
  if (batchSize) qs.set('batch_size', String(batchSize));
  if (part) qs.set('part', String(part));
  return `${BDATA_API}/snapshot/${id}?${qs.toString()}`;
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// NDJSON/JSONL parser (line-delimited JSON objects)
function parseNdjson(text) {
  return text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => JSON.parse(l));
}

async function fetchSnapshotOnce(snapshotId) {
  // Request array JSON. If BD still gives NDJSON or file_url, we handle below.
  const r = await fetch(SNAPSHOT_URL(snapshotId, { format: 'json', batchSize: 10000 }), {
    headers: { Authorization: `Bearer ${BDATA_KEY}` },
  });

  const text = await r.text();

  // 202 = “Snapshot is not ready yet, try again in Xs”
  if (r.status === 202) {
    const waitSec = parseInt((text.match(/\d+/) || [10])[0], 10);
    return { state: 'waiting', waitMs: waitSec * 1000, raw: text };
  }

  if (!r.ok) {
    // BD sometimes returns plain text (“Snapshot is empty”) with 4xx
    if (/snapshot is empty/i.test(text)) return { state: 'ready', items: [] };
    throw new Error(`BrightData snapshot ${r.status}: ${text}`);
  }

  // 200 OK — can be an array, or an object with items/file_url, or NDJSON
  try {
    const json = JSON.parse(text);

    // Case A: Direct array of items
    if (Array.isArray(json)) return { state: 'ready', items: json };

    // Case B: Object with items
    if (json?.items && Array.isArray(json.items)) {
      return { state: 'ready', items: json.items };
    }

    // Case C: Object with file_url → fetch it (JSON or NDJSON)
    if (json?.file_url) {
      const fr = await fetch(json.file_url);
      const ftext = await fr.text();
      try {
        const fjson = JSON.parse(ftext);
        if (Array.isArray(fjson)) return { state: 'ready', items: fjson };
        if (fjson?.items && Array.isArray(fjson.items)) {
          return { state: 'ready', items: fjson.items };
        }
      } catch {
        // NDJSON fallback from file_url
        return { state: 'ready', items: parseNdjson(ftext) };
      }
      // Unexpected but handled above; fallback to empty
      return { state: 'ready', items: [] };
    }

    // Fallback: if structure is unexpected, treat as none
    return { state: 'ready', items: [] };
  } catch {
    // Non-JSON 200 → often NDJSON: handle it
    if (/snapshot is empty/i.test(text)) return { state: 'ready', items: [] };
    return { state: 'ready', items: parseNdjson(text) };
  }
}

/**
 * Polls the snapshot until it’s ready (or until max wait reached).
 * @param {string} snapshotId
 * @param {{ maxWaitMs?: number, baseDelayMs?: number, backoffCapMs?: number }} opts
 * @returns {Promise<Array>} items
 */
async function pollSnapshotUntilReady(
  snapshotId,
  {
    maxWaitMs = 15 * 60 * 1000, // 15 minutes default
    baseDelayMs = 5000,         // min backoff
    backoffCapMs = 60000,       // cap backoff at 60s
  } = {}
) {
  const start = Date.now();
  let attempt = 0;

  while (true) {
    const { state, waitMs, items, raw } = await fetchSnapshotOnce(snapshotId);
    if (state === 'ready') return items || [];

    attempt += 1;
    // Use BD’s suggested wait if present, otherwise exponential backoff
    const backoff = Math.min(baseDelayMs * attempt, backoffCapMs);
    const delay = Math.max(waitMs || 0, backoff);

    if (Date.now() - start + delay > maxWaitMs) {
      throw new Error(`Timed out waiting for snapshot. Last msg: ${raw || 'no message'}`);
    }
    await sleep(delay);
  }
}

// ===== ONE trigger attempt (schema = 'classic' | 'generic') =====
async function triggerOnce(datasetId, { schema, keyword, country, limit }) {
  const limitNum = Math.max(1, Math.min(Number(limit) || 25, 100));

  const qs = new URLSearchParams({
    dataset_id: datasetId,
    include_errors: 'true',
    type: 'discover_new',
    discover_by: 'keyword',
    limit_per_input: String(limitNum),
  });
  const url = `${BDATA_API}/trigger?${qs.toString()}`;

  let inputs;
  if (schema === 'classic') {
    // Many LinkedIn jobs datasets accept this minimal shape
    inputs = [
      {
        keyword: String(keyword || ''),
        location: locationFromCountry(country),
      },
    ];
  } else if (schema === 'generic') {
    // Some datasets require generic names
    inputs = [
      {
        keyword_search: String(keyword || ''),
        domain: 'linkedin.com',
        country: String(country || 'US').toUpperCase(),
        location: locationFromCountry(country),
      },
    ];
  } else {
    throw new Error(`Unknown schema: ${schema}`);
  }

  const r = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${BDATA_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(inputs),
  });

  const text = await r.text();
  if (!r.ok) throw new Error(`BrightData trigger ${r.status}: ${text}`);

  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(text || 'Trigger returned non-JSON');
  }
  if (!json?.snapshot_id) throw new Error(`No snapshot_id in response: ${text}`);

  return json.snapshot_id;
}

// ===== Auto schema (classic → generic fallback) =====
async function triggerAuto(datasetId, { keyword, country, limit }) {
  try {
    return await triggerOnce(datasetId, { schema: 'classic', keyword, country, limit });
  } catch (e) {
    const msg = String(e?.message || '');
    if (isValidationErr(msg)) {
      return await triggerOnce(datasetId, { schema: 'generic', keyword, country, limit });
    }
    throw e;
  }
}

// ===== Dynamic headers & rows (use response keys exactly) =====
function collectHeaders(items, { maxSample = 1000, excludeKeys = [] } = {}) {
  const seen = new Set();
  const headers = [];
  const exclude = new Set(excludeKeys);
  for (let i = 0; i < items.length && i < maxSample; i++) {
    const it = items[i];
    if (it && typeof it === 'object' && !Array.isArray(it)) {
      for (const k of Object.keys(it)) {
        if (!exclude.has(k) && !seen.has(k)) {
          seen.add(k);
          headers.push(k); // preserve first-seen order
        }
      }
    }
  }
  return headers;
}

function normalizeRowToHeaders(item, headers) {
  const row = {};
  for (const h of headers) {
    let v = item?.[h];
    if (v === undefined || v === null) v = '';
    if (typeof v === 'object') {
      try { v = JSON.stringify(v); } catch { v = String(v); }
    }
    row[h] = String(v);
  }
  return row;
}

function buildRows(items, headers, limit) {
  return items.slice(0, limit).map((it) => normalizeRowToHeaders(it, headers));
}

// ===== Route: trigger → wait until ready → append =====
app.post('/api/fetch', async (req, res) => {
  // Disable per-response timeout so we can wait
  res.setTimeout(0);

  try {
    const { sheet_id, keyword, country = 'US', count = 25 } = req.body || {};
    if (!sheet_id) {
      return res.status(400).json({ success: false, message: 'sheet_id is required' });
    }
    if (!DS_LINKEDIN) {
      return res
        .status(400)
        .json({ success: false, message: 'BRIGHTDATA_LINKEDIN_DATASET_ID not configured' });
    }

    const limit = Math.max(1, Math.min(Number(count) || 25, 100));

    // 1) Trigger dataset snapshot
    const snapshotId = await triggerAuto(DS_LINKEDIN, { keyword, country, limit });
    console.log('📌 LinkedIn snapshot_id:', snapshotId);

    // 2) Poll until snapshot is ready
    console.log('⏳ Waiting for snapshot to be ready…');
    const allItems = await pollSnapshotUntilReady(snapshotId, {
      maxWaitMs: Infinity, // ⚠️ your host/proxy may still have hard caps
      baseDelayMs: 5000,
      backoffCapMs: 60000,
    });

    console.log('✅ Snapshot ready. Items:', allItems.length);

    // 3) Build headers EXACTLY from response keys, then rows, then append
    const headers = collectHeaders(allItems);
    console.log('🧾 Derived headers:', headers);

    if (!headers.length) {
      return res.json({
        success: true,
        snapshot_id: snapshotId,
        total_found: 0,
        appended: 0,
        preview: [],
      });
    }

    const rows = buildRows(allItems, headers, limit);

    await appendToGoogleSheet(rows, {
      sheetId: sheet_id,
  sheetName: 'Sheet1',
  headers,                    // from collectHeaders(...)
  insertHeadersIfMissing: true,
  valueInputOption: 'RAW',
  firstDataColIndex: 2,  
    });

    // 4) Respond after appending
    return res.json({
      success: true,
      snapshot_id: snapshotId,
      total_found: allItems.length,
      appended: rows.length,
      preview: rows.slice(0, 5),
    });
  } catch (err) {
    console.error('❌ Error in /api/fetch:', err);
    return res.status(500).json({ success: false, message: err?.message || String(err) });
  }
});

// ===== Start HTTP server with no request timeout (so long waits won't be killed by Node) =====
const server = createServer(app);
server.requestTimeout = 0;  // disable overall request timeout
server.headersTimeout = 0;  // optional: disable header timeout for very long waits
server.keepAliveTimeout = 75_000;

server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
