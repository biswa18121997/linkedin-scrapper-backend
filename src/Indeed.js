// indeedBrightData.js
// Node 18+ (global fetch). For Node 16, `npm i node-fetch` and `import fetch from 'node-fetch'`.

const API = "https://api.brightdata.com/datasets/v3";
const API_KEY = process.env.BRIGHT_DATA_API_KEY;

// Set your dataset id (or via env)
const INDEED_DATASET_ID =
  process.env.BRIGHTDATA_INDEED_DATASET_ID || "gd_xxxxxxxxxxxxx";

/* -------------------- shared helpers -------------------- */
function authHeaders() {
  if (!API_KEY) throw new Error("Missing BRIGHT_DATA_API_KEY env var.");
  return { Authorization: `Bearer ${API_KEY}`, "Content-Type": "application/json" };
}

async function triggerSnapshot(datasetId, inputs, {
  type = "search",            // "search" is faster than "discover_new"
  discover_by = "keyword",
  include_errors = true
} = {}) {
  const url = `${API}/trigger?dataset_id=${encodeURIComponent(datasetId)}`
    + `&type=${type}&discover_by=${discover_by}&include_errors=${include_errors}`;

  const r = await fetch(url, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(inputs),
  });
  if (!r.ok) throw new Error(`Trigger failed: ${r.status} ${await r.text()}`);
  return r.json(); // { snapshot_id }
}

async function getPartsInfo(snapshotId) {
  const r = await fetch(`${API}/snapshot/${snapshotId}/parts`, { headers: authHeaders() });
  if (!r.ok) throw new Error(`Parts query failed: ${r.status} ${await r.text()}`);
  return r.json(); // { total_parts: number }
}

async function downloadPart(snapshotId, partIndex) {
  const r = await fetch(`${API}/snapshot/${snapshotId}/download?format=json&part=${partIndex}`, {
    headers: authHeaders()
  });
  if (!r.ok) return []; // part may not be ready yet
  return r.json();
}

function normalizeJob(j) {
  return {
    source: j.source || "indeed",
    title: j.title || j.position || null,
    company: j.company || j.companyName || null,
    location: j.location || j.jobLocation || null,
    url: j.url || j.jobUrl || j.jobLink || null,
    posted_at: j.postedAt || j.publishedAt || j.datePosted || null,
    description: j.description || j.snippet || j.plainDescription || null,
    _raw: j,
  };
}

/** Collect parts until `limit` or `timeoutMs`. */
async function collectSnapshot(snapshotId, {
  limit = 20,
  timeoutMs = 60000,
  pollMs = 2000
} = {}) {
  const start = Date.now();
  const seen = new Set();
  const out = [];

  while (Date.now() - start < timeoutMs && out.length < limit) {
    const { total_parts = 0 } = await getPartsInfo(snapshotId);
    for (let i = 0; i < total_parts; i++) {
      if (seen.has(i)) continue;
      const records = await downloadPart(snapshotId, i);
      if (records.length) {
        seen.add(i);
        for (const rec of records) {
          out.push(normalizeJob(rec));
          if (out.length >= limit) break;
        }
      }
      if (out.length >= limit) break;
    }
    if (out.length >= limit) break;
    await new Promise(r => setTimeout(r, pollMs));
  }
  return out.slice(0, limit);
}

/* -------------------- 1) QUICK / “INSTANT” -------------------- */
/**
 * Single small query with short timeout. Good for ~10–30 results.
 * Params:
 *  - keyword, location, job_type, experience_level, remote, company
 *  - limit (default 20), timeoutMs (default 45s)
 *  - datasetId (optional), mode: "search"|"discover_new" (default "search")
 */
export async function fetchIndeedJobsQuick({
  keyword,
  location,
  job_type = "",
  experience_level = "",
  remote = "",
  company = "",
  limit = 20,
  timeoutMs = 45000,
  datasetId = INDEED_DATASET_ID,
  mode = "search",
} = {}) {
  const input = [{
    keyword,
    location,
    job_type,
    experience_level,
    remote,
    company,
  }];

  const { snapshot_id } = await triggerSnapshot(datasetId, input, { type: mode, discover_by: "keyword" });
  return collectSnapshot(snapshot_id, { limit, timeoutMs, pollMs: 1500 });
}

/* -------------------- 2) BULK / PARALLEL -------------------- */
/**
 * Many queries (array of inputs), higher limits, parallelized.
 * Params:
 *  - inputs: [{ keyword, location, job_type, experience_level, remote, company, limit? }, ...]
 *  - perInputLimit (default 40), timeoutMs (default 3 min), concurrency (default 3)
 *  - datasetId (optional), mode (default "search")
 */
export async function fetchIndeedJobsBulk({
  inputs,
  perInputLimit = 40,
  timeoutMs = 180000,
  concurrency = 3,
  datasetId = INDEED_DATASET_ID,
  mode = "search",
} = {}) {
  if (!Array.isArray(inputs) || inputs.length === 0) return [];

  const results = [];
  const queue = inputs.map((p, idx) => ({ idx, p }));

  async function worker() {
    while (queue.length) {
      const { p } = queue.shift();
      try {
        const { snapshot_id } = await triggerSnapshot(datasetId, [p], { type: mode, discover_by: "keyword" });
        const items = await collectSnapshot(snapshot_id, {
          limit: p.limit || perInputLimit,
          timeoutMs,
          pollMs: 2000,
        });
        results.push(...items);
      } catch (e) {
        console.error("Indeed bulk worker error:", e?.message || e);
      }
    }
  }

  const workers = Array.from(
    { length: Math.max(1, Math.min(concurrency, inputs.length)) },
    () => worker()
  );
  await Promise.all(workers);

  return results;
}
