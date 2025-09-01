import { ApifyClient } from 'apify-client';
import dotenv from 'dotenv/config';
import { appendToGoogleSheet } from '../utils/GoogleSheetsHelper.js';

function toPositiveInt(value, fallback = 25) {
  const n = Number.parseInt(String(value ?? '').trim(), 10);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function normalizePublishedAt(v) {
  // your special cases → map to valid window codes
  if (v === 'r259200') return 'r604800';   // 3d → 7d
  if (v === 'r1209600') return 'r604800';  // 14d → 7d
  return typeof v === 'string' && v.startsWith('r') ? v : undefined;
}
function unionKeys(rows) {
  const s = new Set();
  for (const r of rows || []) if (r && typeof r === 'object') {
    for (const k of Object.keys(r)) s.add(k);
  }
  return Array.from(s);
}

export default async function Linkedin(req, res, next) {
  try {
    if(req.body?.fetchfrom?.includes('linkedin')){

        const client = new ApifyClient({ token: process.env.APIFY_API_KEY});
        const limit = toPositiveInt(req.body.limit, 25);
        // Build input and omit undefined values so the schema stays clean
        const input = {
          title: (req.body.title ?? '').toString().trim(),
          location: 'United States',
          companyName: [],
          companyId: [],
          workType: req.body.workType ?? undefined,
          contractType: req.body.contractType ?? undefined,
          experienceLevel: req.body.experienceLevel ?? undefined,
          publishedAt: normalizePublishedAt(req.body.publishedAt),
          rows: limit,          // must be integer
          maxItems: limit,      // must be integer
          proxy: {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
          },
        };
        // Remove undefined keys (important for strict schemas)
        Object.keys(input).forEach((k) => input[k] === undefined && delete input[k]);
        console.log('process started..');
        // NOTE: ensure the actor ID is correct for LinkedIn scraper
        const run = await client.actor('9eTAaHrnHrljnL3Tg').call(input);
        const { items } = await client.dataset(run.defaultDatasetId).listItems();
        req.body.linkedInItems = items || [];
        const sheetId = req.body.sheet_id;            // <-- from frontend
        const sheetName = 'Sheet1';
        const userID = String(req.body.userID || '');
        if (sheetId) {
          // Build rows: include userID; your helper will put checkbox first col and userID last col.
          const liRows = (items || []).map(obj => ({ ...obj, userID }));

          // Headers = union of keys from the rows (your helper will enforce [Done, ...headers, userID])
          const headers = unionKeys(liRows);

          await appendToGoogleSheet(liRows, {
            sheetId,
            sheetName,
            headers,                 // headers as key names (requirement satisfied)
            valueInputOption: 'RAW', // keep as is
            tickColName: 'Done',     // your helper expects this name for the checkbox col
            userColName: 'userID',   // last column
          });
          req.body.linkedin = items;
          console.log('linkedin :- appended to google sheets');
          } else {
          console.warn('LinkedIn: sheetId missing in request; skipping sheet append.');
        }
    }   
  next();
  } catch (err) {
    console.error('LinkedIn actor error:', err?.message || err);
    // pass the error to your error middleware instead of crashing the process
  next();
  }
}
