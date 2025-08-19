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

/** ===== EXCLUDE LIST (case-insensitive contains) ===== */
const EXCLUDE_COMPANIES = ['Lensa', 'TieTalent'];

/** ===== Tunables ===== */
const DEFAULT_LOCATION = 'United States';

/** ===== EXACT Column Order you requested ===== */
const HEADERS = [
  'Title','title',
  'Location','location',
  'Posted time','postedTime',
  'Published at','publishedAt',
  'Job Url','jobUrl',
  'Company Name','companyName',
  'Campany Url','companyUrl',
  'Description','description',
  'Applications count','applicationsCount',
  'Employment type','contractType',
  'Seniority level','experienceLevel',
  'Job function','workType',
  'Industries','sector',
  'Salary','salary',
  'Posted by','posterFullName',
  'Poster profile url','posterProfileUrl',
  'Company ID','companyId',
  'Apply Url','applyUrl',
  'Apply Type','applyType',
  'Benefits','benefits'
];

/* ------------------------- helpers ------------------------- */

function toArray(val) {
  if (!val && val !== 0) return [];
  if (Array.isArray(val)) return val.map(String).map(s => s.trim()).filter(Boolean);
  return String(val).split(',').map(s => s.trim()).filter(Boolean);
}

function mapPublishedAt(sort_by) {
  // actor accepts: r86400 (24h), r604800 (7d), r2592000 (30d)
  const m = { day: 'r86400', week: 'r604800', month: 'r2592000' };
  const key = String(sort_by || 'day').toLowerCase(); // default latest (24h)
  return m[key] || 'r86400';
}

function mapExperienceLevel(s) {
  if (!s) return undefined;
  const k = String(s).toLowerCase();
  const map = {
    internship: '1',
    'entry_level': '2', 'entry level': '2', entry: '2', junior: '2',
    associate: '3',
    'mid_senior_level': '4', 'mid-senior': '4', 'mid senior': '4', senior: '4',
    director: '5', executive: '5',
  };
  return map[k] || undefined;
}

function mapWorkType(s) {
  if (!s) return undefined;
  const k = String(s).toLowerCase();
  const map = { onsite: '1', 'on-site': '1', remote: '2', hybrid: '3' };
  return map[k] || undefined;
}

function companyIsExcluded(name, profileUrl, excludes) {
  if (!excludes.length) return false;
  const nameLc = (name || '').toLowerCase();
  let hostLc = '';
  try { hostLc = new URL(profileUrl || '').hostname.toLowerCase(); } catch {}
  return excludes.some(x => nameLc.includes(x) || (hostLc && hostLc.includes(x)));
}

function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v !== undefined && v !== null && String(v).trim() !== '') return v;
  }
  return '';
}

function joinIfArray(x, sep = ', ') {
  return Array.isArray(x) ? x.filter(Boolean).join(sep) : (x ?? '');
}

function toRelative(raw) {
  return firstNonEmpty(raw.postedTime, raw.posted_time, raw.timeAgo, raw.time_ago, raw.listedAtRelative, raw.posted);
}
function toPublishedAtISO(raw) {
  return firstNonEmpty(raw.publishedAt, raw.postedAt, raw.datePosted, raw.listedAt, raw.createdAt);
}
function minutesFromRelative(rel) {
  if (!rel) return Infinity;
  const s = String(rel).toLowerCase();
  const num = (re) => { const m = s.match(re); return m ? parseInt(m[1], 10) : 0; };
  if (s.includes('just now')) return 0;
  if (/(\d+)\s*(minute|min|m)\b/.test(s)) return num(/(\d+)\s*(?:minute|min|m)\b/);
  if (/(\d+)\s*(hour|hr|h)\b/.test(s))   return num(/(\d+)\s*(?:hour|hr|h)\b/) * 60;
  if (/(\d+)\s*(day|d)\b/.test(s))       return num(/(\d+)\s*(?:day|d)\b/) * 1440;
  if (/(\d+)\s*(week|w)\b/.test(s))      return num(/(\d+)\s*(?:week|w)\b/) * 10080;
  return Infinity;
}
function newestTimestamp(raw) {
  // Prefer ISO timestamp, else derive from relative
  const iso = toPublishedAtISO(raw);
  if (iso && /^\d{4}-\d{2}-\d{2}T/.test(String(iso))) {
    const t = new Date(iso).getTime();
    return Number.isFinite(t) ? t : 0;
  }
  const rel = toRelative(raw);
  const mins = minutesFromRelative(rel);
  if (!Number.isFinite(mins)) return 0;
  return Date.now() - mins * 60000;
}

/* ------------------- Apify actor call (bebity) ------------------- */
async function runApifyLinkedInJobs({ title, location, rows, publishedAt, workType, experienceLevel, companyName }) {
  const ACTOR_ID = process.env.APIFY_ACTOR_ID || 'bebity~linkedin-jobs-scraper';
  const token = process.env.APIFY_TOKEN;
  if (!token) throw new Error('APIFY_TOKEN missing');

  const url = new URL(`https://api.apify.com/v2/acts/${ACTOR_ID}/run-sync-get-dataset-items`);
  url.searchParams.set('token', token);
  url.searchParams.set('format', 'json');

  // NOTE: we deliberately DO NOT pass contractType (job type) to keep its relevance low
  const input = {
    title: title || '',
    location: location || DEFAULT_LOCATION,
    rows: Math.max(1, Number(rows) || 50),
    publishedAt: publishedAt || 'r86400',         // default 24h (latest)
    workType: workType || undefined,              // 1 onsite, 2 remote, 3 hybrid
    experienceLevel: experienceLevel || undefined, // 1..5
    companyName: (Array.isArray(companyName) && companyName.length) ? companyName : undefined,
  };

  const apiRes = await fetch(url.toString(), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });

  if (!apiRes.ok) {
    const txt = await apiRes.text().catch(() => '');
    throw new Error(`Apify ${ACTOR_ID} ${apiRes.status}: ${txt || apiRes.statusText}`);
  }
  const data = await apiRes.json();
  return Array.isArray(data) ? data : [];
}

/* -------------------- Normalize to requested columns -------------------- */
function normalizeToRequestedColumns(raw) {
  const title = firstNonEmpty(raw.title, raw.jobTitle, raw.position);
  const location = firstNonEmpty(raw.location, raw.jobLocation);
  const postedTime = toRelative(raw);
  const publishedAt = toPublishedAtISO(raw);
  const jobUrl = firstNonEmpty(raw.jobUrl, raw.jobLink, raw.url, raw.link);
  const companyName = firstNonEmpty(raw.companyName, raw.company);
  const companyUrl = firstNonEmpty(raw.companyUrl, raw.companyProfile, raw.company_link);
  const description = firstNonEmpty(raw.description, raw.jobDescription, raw.plainDescription);
  const applicationsCount = firstNonEmpty(raw.applicationsCount, raw.numApplicants, raw.applicants, raw.applicantCount, raw.applications);
  const contractType = firstNonEmpty(raw.contractType, raw.employmentType, raw.jobType);
  const experienceLevel = firstNonEmpty(raw.experienceLevel, raw.seniority, raw.seniorityLevel);
  const workType = firstNonEmpty(raw.workType); // per your mapping: "Job function" -> workType
  const sector = firstNonEmpty(raw.sector, joinIfArray(raw.industries));
  const salary = firstNonEmpty(raw.salary, raw.pay, raw.compensation);
  const posterFullName = firstNonEmpty(raw.posterFullName, raw.posterName);
  const posterProfileUrl = firstNonEmpty(raw.posterProfileUrl, raw.posterUrl, raw.recruiterUrl);
  const companyId = firstNonEmpty(raw.companyId);
  const applyUrl = firstNonEmpty(raw.applyUrl, raw.applicationUrl);
  const applyType = firstNonEmpty(raw.applyType, raw.applicationType, raw.apply_type);
  const benefits = joinIfArray(raw.benefits);

  return {
    'Title': title,
    'Location': location,
    'Posted time': postedTime, 
    'Published at': publishedAt, 
    'Job Url': jobUrl, 
    'Company Name': companyName, 
    'Campany Url': companyUrl, 
    'Description': description, 
    'Applications count': String(applicationsCount ?? '').trim(),
    'Employment type': contractType, 
    'Seniority level': experienceLevel, 
    'Job function': workType,
    'Industries': sector, 
    'Salary': salary, 
    'Posted by': posterFullName, 
    'Poster profile url': posterProfileUrl, 
    'Company ID': companyId,
    'Apply Url': applyUrl, 
    'Apply Type': applyType, 
    'Benefits': benefits
  };
}

/* ----------------------------- Route ----------------------------- */

app.post('/api/fetch-jobs', async (req, res) => {
  try {
    const {
      field,                 // -> title
      location,
      sort_by,               // "day" (default), "week", "month"
      work_type,             // onsite/remote/hybrid (1/2/3)
      filter_by_company,     // comma list -> companyName[]

      // SINGLE experience level (optional)
      exp_levels,            // internship/entry_level/associate/mid_senior_level/director/executive

      // sheet params
      sheet_id,
      sheet_name = 'Sheet1',

      // total result target (exact rows for Apify)
      total_records = 50,

      // optional
      last_hour_only = false,
    } = req.body || {};

    if (!sheet_id) {
      return res.status(400).json({ success: false, message: 'sheet_id is required' });
    }
    if (!process.env.APIFY_TOKEN) {
      return res.status(400).json({ success: false, message: 'APIFY_TOKEN missing' });
    }

    const excludeList = EXCLUDE_COMPANIES.map(s => s.toLowerCase());
    const TOTAL = Math.max(1, Number(total_records));
    const publishedAt = mapPublishedAt(sort_by);     // default 24h (latest window)
    const workType = mapWorkType(work_type);
    const expCode = mapExperienceLevel(exp_levels);
    const companyNames = filter_by_company ? toArray(filter_by_company) : undefined;

    // Single, fast Apify call
    const items = await runApifyLinkedInJobs({
      title: field,
      location,
      rows: TOTAL,
      publishedAt,
      workType,
      experienceLevel: expCode,
      companyName: companyNames,
    });

    // Sort newest -> oldest (independent of actor order)
    items.sort((a, b) => newestTimestamp(b) - newestTimestamp(a));

    // Filter, dedupe, and cap to TOTAL
    const seen = new Set();
    const rows = [];
    for (const raw of items) {
      // Optional last-hour filter
      if (last_hour_only) {
        const rel = toRelative(raw);
        let mins = minutesFromRelative(rel);
        if (!Number.isFinite(mins)) {
          const iso = toPublishedAtISO(raw);
          if (iso) {
            const t = new Date(iso).getTime();
            if (Number.isFinite(t)) mins = (Date.now() - t) / 60000;
          }
        }
        if (!Number.isFinite(mins) || mins > 60) continue;
      }

      const companyName = firstNonEmpty(raw.companyName, raw.company);
      const companyUrl = firstNonEmpty(raw.companyUrl, raw.companyProfile, raw.company_link);
      if (companyIsExcluded(companyName, companyUrl, excludeList)) continue;

      const jobUrl = firstNonEmpty(raw.jobUrl, raw.jobLink, raw.url, raw.link);
      const title = firstNonEmpty(raw.title, raw.jobTitle, raw.position);
      const locationStr = firstNonEmpty(raw.location, raw.jobLocation);
      const key = `${jobUrl}|${companyName}|${title}|${locationStr}`;
      if (seen.has(key)) continue;

      seen.add(key);
      rows.push(normalizeToRequestedColumns(raw));
      if (rows.length >= TOTAL) break;
    }

    if (rows.length) {
      await appendToGoogleSheet(rows, {
        sheetId: sheet_id,
        sheetName: sheet_name,
        headers: HEADERS,
        insertHeadersIfMissing: true,
        valueInputOption: 'RAW'
      });
      console.log(`✅ Appended ${rows.length} rows to sheet ${sheet_id} (${sheet_name})`);
    } else {
      console.log('ℹ️ No rows to append.');
    }

    res.json({
      success: true,
      saved: rows.length,
      requested: TOTAL,
      jobs: rows   // so your UI preview works immediately
    });

  } catch (err) {
    console.error('❌ Error in /api/fetch-jobs:', err);
    res.status(500).json({ success: false, message: err?.message || String(err) });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
