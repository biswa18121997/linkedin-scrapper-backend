// index.js — Apify actors → Google Sheets with dynamic columns + checkbox + userID (fault-tolerant)
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { ApifyClient } from 'apify-client';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 8086;

app.use(cors());
app.use(express.json());

// ===============================
// Input normalization & mapping
// ===============================
function ensureUSDefaults(input) {
  const out = { ...input };
  if (!out.location && !out.country) out.location = 'United States';
  if (!out.country) out.country = 'US';
  return out;
}

function ensureQueryAndKeyword(input, fallback = 'Software Engineer') {
  const out = { ...input };
  const q = String(out.query || out.keyword || '').trim();
  out.query = q || fallback;
  out.keyword = out.query;
  return out;
}

// Build a US Indeed search URL from query + location
function buildIndeedSearchUrl({ query, location }) {
  const base = 'https://www.indeed.com/jobs';
  const params = new URLSearchParams();
  params.set('q', query || 'Software Engineer');
  params.set('l', location || 'United States');
  return `${base}?${params.toString()}`;
}

/**
 * Map inputs per actor quirks.
 * For curious_coder~indeed-scraper, return the *minimal* payload ONLY:
 *   { scrapeJobs: { searchUrl }, count }
 * For others, keep normalized generic inputs.
 */
function mapInputForActor(actorId, rawInput, { fallbackKeyword = 'Software Engineer', limit } = {}) {
  const idRaw = String(actorId || '');
  const id = idRaw.toLowerCase().replace(/\s+/g, '');

  // generic normalization first
  let input = ensureUSDefaults(rawInput || {});
  input = ensureQueryAndKeyword(input, fallbackKeyword);

  if (id.includes('curious_coder~indeed-scraper')) {
    const searchUrl =
      (input && input.scrapeJobs && input.scrapeJobs.searchUrl) ||
      buildIndeedSearchUrl({ query: input.query, location: input.location });

    const minimal = { scrapeJobs: { searchUrl } };
    if (Number.isFinite(limit)) minimal.count = Number(limit);
    console.log('🟦 Indeed minimal payload:', JSON.stringify(minimal, null, 2));
    return minimal;
  }

  console.log(`🟩 Actor ${actorId} normalized payload:`, JSON.stringify(input, null, 2));
  return input;
}

// ===============================
// Apify Client
// ===============================
const client = new ApifyClient({
  token: process.env.APIFY_API_KEY,
});

// ===============================
// Utilities for Sheet rows
// ===============================
function collectHeaders(items, { maxSample = 1000 } = {}) {
  const seen = new Set();
  const headers = [];
  for (let i = 0; i < items.length && i < maxSample; i++) {
    const it = items[i];
    if (it && typeof it === 'object' && !Array.isArray(it)) {
      for (const k of Object.keys(it)) {
        if (!seen.has(k)) {
          seen.add(k);
          headers.push(k);
        }
      }
    }
  }
  return headers;
}

function buildFinalHeaders(apiHeaders, opts = {}) {
  const tickCol = opts.tickColName ?? 'Done';
  const userCol = opts.userColName ?? 'userID';
  return [tickCol, ...apiHeaders, userCol];
}

function buildRowsWithTickAndUser(items, apiHeaders, { userID, tickColName = 'Done', userColName = 'userID', limit } = {}) {
  const rows = [];
  const capped = limit ? items.slice(0, limit) : items;
  for (const it of capped) {
    const row = {};
    row[tickColName] = 'FALSE'; // default unchecked
    for (const h of apiHeaders) {
      let v = it?.[h];
      if (v === undefined || v === null) v = '';
      if (typeof v === 'object') {
        try { v = JSON.stringify(v); } catch { v = String(v); }
      }
      row[h] = String(v);
    }
    row[userColName] = userID || '';
    rows.push(row);
  }
  return rows;
}

// ===============================
// Safe actor runner (no throw)
// ===============================
async function runActorAndGetItems(actorId, input, { limit, clean = true } = {}) {
  const run = await client.actor(actorId).call(input || {});
  const dsId = run?.defaultDatasetId;
  if (!dsId) return [];
  const list = await client.dataset(dsId).listItems({ clean, limit });
  return Array.isArray(list?.items) ? list.items : [];
}

async function safeRun(actorKey, actorId, input, opts) {
  try {
    if (!actorId) throw new Error(`${actorKey} actor ID not configured`);
    const items = await runActorAndGetItems(actorId, input, opts);
    return { actor: actorKey, items, error: null };
  } catch (err) {
    console.error(`⚠️ ${actorKey} run failed:`, err);
    return { actor: actorKey, items: [], error: err?.message || String(err) };
  }
}

// ===============================
// Route
// ===============================
app.post('/api/fetch', async (req, res) => {
  res.setTimeout(0);
  const startedAt = Date.now();

  try {
    const {
      sheet_id,
      userID,
      limit = 100,
      linkedinInput = {},
      indeedInput = {},
      glassdoorInput = {},
    } = req.body || {};

    if (!sheet_id) {
      return res.status(400).json({ success: false, message: 'sheet_id is required' });
    }
    if (!process.env.APIFY_API_KEY) {
      return res.status(400).json({ success: false, message: 'APIFY_API_KEY not set' });
    }

    const ACTORS = {
      linkedin: process.env.APIFY_LINKEDIN_ACTOR,
      indeed: process.env.APIFY_INDEED_ACTOR,
      glassdoor: process.env.APIFY_GLASSDOOR_ACTOR,
    };

    // Map inputs per actor (adds US defaults, keyword; special-case for Indeed)
    const normLinkedIn  = mapInputForActor(ACTORS.linkedin,  linkedinInput,  { limit });
    const normIndeed    = mapInputForActor(ACTORS.indeed,    indeedInput,    { limit });
    const normGlassdoor = mapInputForActor(ACTORS.glassdoor, glassdoorInput, { limit });

    console.log('🔎 Final inputs:', {
      linkedin: normLinkedIn,
      indeed: normIndeed,
      glassdoor: normGlassdoor,
    });

    // SAFE parallel runs: none of these will throw thanks to safeRun
    const [rLi, rIn, rGd] = await Promise.all([
      safeRun('linkedin',  ACTORS.linkedin,  normLinkedIn,  { limit }),
      safeRun('indeed',    ACTORS.indeed,    normIndeed,    { limit }),
      safeRun('glassdoor', ACTORS.glassdoor, normGlassdoor, { limit }),
    ]);

    // Per-source append (each in its own try/catch)
    const plan = [
      { key: 'linkedin',  items: rLi.items, error: rLi.error, sheetName: 'Sheet1' },
      { key: 'indeed',    items: rIn.items, error: rIn.error, sheetName: 'Sheet2' },
      { key: 'glassdoor', items: rGd.items, error: rGd.error, sheetName: 'Sheet3' },
    ];

    const per_source = {};
    for (const p of plan) {
      let appendErr = null;
      let headers = [];
      let rows = [];

      try {
        const apiHeaders = collectHeaders(p.items);
        headers = buildFinalHeaders(apiHeaders, { tickColName: 'Done', userColName: 'userID' });
        rows = p.items.length
          ? buildRowsWithTickAndUser(p.items, apiHeaders, {
              userID,
              tickColName: 'Done',
              userColName: 'userID',
              limit,
            })
          : [];

        if (rows.length) {
          await appendToGoogleSheet(rows, {
            sheetId: sheet_id,
            sheetName: p.sheetName,
            headers,
            insertHeadersIfMissing: true,
            valueInputOption: 'RAW',
            firstDataColIndex: 0,
          });
        }
      } catch (e) {
        console.error(`⚠️ Append failed for ${p.key}:`, e);
        appendErr = e?.message || String(e);
      }

      per_source[p.key] = {
        run_error: p.error || null,       // actor run error (if any)
        append_error: appendErr,          // append error (if any)
        total_found: p.items.length,
        appended: rows.length || 0,
        headers,
        preview: (rows || []).slice(0, 5),
      };
    }

    const tookMs = Date.now() - startedAt;

    // success if at least one source appended or had items
    const anySuccess = Object.values(per_source).some(s => (s.appended || 0) > 0 || s.total_found > 0);
    return res.json({
      success: anySuccess,
      took_ms: tookMs,
      per_source,
    });
  } catch (err) {
    console.error('❌ /api/fetch fatal error:', err);
    // Even on unexpected fatal errors, respond with 200 + partial info style if possible
    return res.status(200).json({
      success: false,
      message: err?.message || String(err),
      per_source: {},
    });
  }
});

// ===============================
// Server
// ===============================
const server = createServer(app);
server.requestTimeout = 0;
server.headersTimeout = 0;
server.keepAliveTimeout = 75_000;

server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
