// index.js
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

/**
 * ===== EXCLUDE LIST (edit this) =====
 * Case-insensitive substring match against company name (and company_profile host).
 * Example: ['Meta', 'Google']  -> will skip "Meta Platforms" and "google-cloud".
 */
const EXCLUDE_COMPANIES = [
  // 'Meta',
  // 'Google',
  'Lensa',
  'TieTalent'
];

/**
 * Tunables (edit if needed)
 */
const MAX_PAGES_PER_COMBO = 5; // how many pages to try per (job_type, exp_level)
const MAX_FALLBACK_PAGES  = 8; // how many pages to try in broad (no filters) fallback

/** Build ScrapingDog LinkedIn Jobs URL */
function buildScrapingDogUrl(params) {
  const search = new URLSearchParams();
  search.set('api_key', process.env.SCRAPPING_DOG_API_KEY || '');
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') {
      search.set(k, String(v));
    }
  }
  return `https://api.scrapingdog.com/linkedinjobs/?${search.toString()}`;
}

/** Normalize to array (accepts comma-separated string or array) */
function toArray(val) {
  if (!val && val !== 0) return [];
  if (Array.isArray(val)) return val.map(String).map(s => s.trim()).filter(Boolean);
  return String(val).split(',').map(s => s.trim()).filter(Boolean);
}

/** Unique key for dedupe */
function jobKey(j) {
  return (
    j.job_link ||
    j.job_id ||
    `${j.company_name || ''}|${j.job_position || ''}|${j.job_location || ''}`
  );
}

/** One ScrapingDog call */
async function fetchJobsOnce(params) {
  const url = buildScrapingDogUrl(params);
  const apiRes = await fetch(url);
  if (!apiRes.ok) {
    const txt = await apiRes.text().catch(() => '');
    throw new Error(`ScrapingDog API ${apiRes.status}: ${txt || apiRes.statusText}`);
  }
  const data = await apiRes.json();
  return Array.isArray(data) ? data : (data.jobs || data.results || []);
}

app.post('/api/fetch-jobs', async (req, res) => {
  try {
    const {
      // search params
      field,
      location,
      geoid,
      sort_by,              // "", "day", "week", "month"
      work_type,
      filter_by_company,

      // multi-select
      job_types,
      exp_levels,

      // sheet params from frontend
      sheet_id,
      sheet_name = 'Sheet1',

      // totals/pagination
      total_records = 50,
      per_request_count = 10,   // 10 per page

      // passthrough
      ...rest
    } = req.body || {};

    if (!process.env.SCRAPPING_DOG_API_KEY) {
      return res.status(400).json({ success: false, message: 'API key missing' });
    }
    if (!sheet_id) {
      return res.status(400).json({ success: false, message: 'sheet_id is required' });
    }

    // Build filter combos
    const jt = toArray(job_types).length ? toArray(job_types) : [null];
    const xl = toArray(exp_levels).length ? toArray(exp_levels) : [null];
    const combos = [];
    for (const j of jt) for (const x of xl) combos.push({ job_type: j, exp_level: x });

    // Targets
    const TOTAL = Math.max(1, Number(total_records));
    const CHUNK = Math.min(49, Math.max(1, Number(per_request_count)));

    // Exclusion list (normalized once)
    const excludeList = EXCLUDE_COMPANIES.map(s => String(s || '').toLowerCase()).filter(Boolean);

    // Params common to every request
    const baseParams = {
      field,
      location,
      geoid,
      sort_by,             // values day/week/month/"" from frontend
      work_type,
      filter_by_company,
      count: CHUNK,
      ...rest
    };

    const allJobs = [];
    const seen = new Set();
    let totalRequests = 0;

    console.log(`🎯 total=${TOTAL} | chunk=${CHUNK} | combos=${combos.length}`);
    console.log(`↗️ Target sheet: ${sheet_id} (tab: ${sheet_name})`);
    if (excludeList.length) {
      console.log(`🚫 Excluding companies: ${excludeList.join(', ')}`);
    }

    let remaining = TOTAL;

    // Helpers
    function companyIsExcluded(job) {
      if (!excludeList.length) return false;
      const nameLc = (job.company_name || '').toLowerCase();
      let hostLc = '';
      try { hostLc = new URL(job.company_profile || '').hostname.toLowerCase(); } catch {}
      return excludeList.some(x => nameLc.includes(x) || (hostLc && hostLc.includes(x)));
    }

    function addUnique(items, combo, page) {
      let added = 0;
      for (const j of items) {
        if (companyIsExcluded(j)) {
          // console.log(`    ↷ Skipped excluded: ${j.company_name}`);
          continue;
        }
        const key = jobKey(j);
        if (seen.has(key)) continue;

        seen.add(key);
        allJobs.push({
          job_position: j.job_position || '',
          job_link: j.job_link || '',
          job_id: j.job_id || '',
          company_name: j.company_name || '',
          company_profile: j.company_profile || '',
          job_location: j.job_location || '',
          job_posting_date: j.job_posting_date || '',
          company_logo_url: j.company_logo_url || '',
          job_type: combo?.job_type || '',
          exp_level: combo?.exp_level || '',
          page
        });
        added++;
        remaining--;
        if (remaining <= 0) break;
      }
      return added;
    }

    // PHASE 1: iterate each combo, fetch multiple pages, fill globally until TOTAL
    for (const [idx, combo] of combos.entries()) {
      if (remaining <= 0) break;

      console.log(`▶️ Combo ${idx + 1}/${combos.length}: job_type=${combo.job_type || 'any'}, exp_level=${combo.exp_level || 'any'}`);

      for (let page = 1; page <= MAX_PAGES_PER_COMBO && remaining > 0; page++) {
        const params = {
          ...baseParams,
          page,
          job_type: combo.job_type || undefined,
          exp_level: combo.exp_level || undefined
        };

        console.log(`  • Fetch page ${page}/${MAX_PAGES_PER_COMBO} for combo ${idx + 1}`);
        const items = await fetchJobsOnce(params);
        totalRequests++;

        if (!items.length) {
          console.log(`  ⚠️ No items on page ${page}. Stopping this combo early.`);
          break;
        }

        const added = addUnique(items, combo, page);
        console.log(`    ✓ Added ${added} from page ${page}. Remaining=${remaining}`);
      }
    }

    // PHASE 2: Broad fallback (no job_type / exp_level) to top up
    if (remaining > 0) {
      console.log(`🔁 Fallback: need ${remaining} more — running broad search`);
      for (let page = 1; page <= MAX_FALLBACK_PAGES && remaining > 0; page++) {
        const params = {
          ...baseParams,
          page,
          job_type: undefined,
          exp_level: undefined
        };
        const items = await fetchJobsOnce(params);
        totalRequests++;

        if (!items.length) {
          console.log(`  ⚠️ Fallback page ${page} empty — stopping fallback.`);
          break;
        }

        const added = addUnique(items, null, page);
        console.log(`  ✓ Fallback added ${added} (page ${page}). Remaining=${remaining}`);
      }
    }

    const finalJobs = allJobs.slice(0, TOTAL);

    // Headers for the sheet
    const HEADERS = [
      'Position',
      'Link',
      'Job ID',
      'Company',
      'Company Profile',
      'Location',
      'Posted',
      'Logo URL',
      'Job Type',
      'Experience Level',
      'Page'
    ];

    const rows = finalJobs.map(j => [
      j.job_position,
      j.job_link,
      j.job_id,
      j.company_name,
      j.company_profile,
      j.job_location,
      j.job_posting_date,
      j.company_logo_url,
      j.job_type,
      j.exp_level,
      j.page || ''
    ]);

    if (rows.length) {
      await appendToGoogleSheet(rows, {
        sheetId: sheet_id,
        sheetName: sheet_name,
        headers: HEADERS,
        insertHeadersIfMissing: true
      });
      console.log(`✅ Appended ${rows.length} rows to sheet ${sheet_id} (${sheet_name})`);
    } else {
      console.log('ℹ️ No rows to append.');
    }

    res.json({
      success: true,
      rowCount: rows.length,
      requestsMade: totalRequests,
      combos: combos.map(c => ({ job_type: c.job_type || 'any', exp_level: c.exp_level || 'any' })),
      jobs: finalJobs
    });

  } catch (err) {
    console.error('❌ Error in /api/fetch-jobs:', err);
    res.status(500).json({ success: false, message: err?.message || String(err) });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
