import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

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

/** Normalize to array */
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

    const jt = toArray(job_types).length ? toArray(job_types) : [null];
    const xl = toArray(exp_levels).length ? toArray(exp_levels) : [null];

    // Build filter combos
    const combos = [];
    for (const j of jt) for (const x of xl) combos.push({ job_type: j, exp_level: x });

    const TOTAL = Math.max(1, Number(total_records));
    const CHUNK = Math.min(49, Math.max(1, Number(per_request_count)));
    const targetPerCombo = Math.ceil(TOTAL / combos.length);
    const pagesNeeded = Math.ceil(targetPerCombo / CHUNK);

    const baseParams = {
      field,
      location,
      geoid,
      sort_by,             // frontend shows friendly labels; values still day/week/month
      work_type,
      filter_by_company,
      count: CHUNK,
      ...rest
    };

    const allJobs = [];
    const seen = new Set();
    let totalRequests = 0;

    console.log(`🎯 total=${TOTAL} | chunk=${CHUNK} | combos=${combos.length} | perCombo=${targetPerCombo} | pages=${pagesNeeded}`);
    console.log(`↗️ Target sheet: ${sheet_id} (tab: ${sheet_name})`);

    for (const [idx, combo] of combos.entries()) {
      let collected = 0;
      console.log(`▶️ Combo ${idx + 1}/${combos.length}: job_type=${combo.job_type || 'any'}, exp_level=${combo.exp_level || 'any'}`);

      for (let currentPage = 1; currentPage <= pagesNeeded; currentPage++) {
        const params = {
          ...baseParams,
          page: currentPage,                           // true pagination 1..N
          job_type: combo.job_type || undefined,
          exp_level: combo.exp_level || undefined
        };

        console.log(`  • Fetch page ${currentPage}/${pagesNeeded} for combo ${idx + 1}`);
        const items = await fetchJobsOnce(params);
        totalRequests++;

        if (!items.length) {
          console.log(`  ⚠️ No items on page ${currentPage}. Stopping this combo early.`);
          break;
        }

        let addedThisPage = 0;
        for (const j of items) {
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
            job_type: combo.job_type || '',
            exp_level: combo.exp_level || '',
            page: currentPage
          });
          addedThisPage++;
          collected++;
          if (collected >= targetPerCombo) break;
        }

        console.log(`    ✓ Added ${addedThisPage} new jobs from page ${currentPage}. Collected=${collected}/${targetPerCombo}`);
        if (collected >= targetPerCombo) break;
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
    res.status(500).json({ success: false, message: err});
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
