import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

app.post('/api/fetch-jobs', async (req, res) => {
  try {
    // Build ScrapingDog API request params
    const params = {
      api_key: process.env.SCRAPPING_DOG_API_KEY,
      ...req.body, // expect { field, location, sort_by, ... }
    };

    const query = new URLSearchParams(params).toString();
    const apiUrl = `https://api.scrapingdog.com/linkedinjobs/?${query}`;
    console.log(`📡 Fetching jobs from: ${apiUrl}`);

    const apiRes = await fetch(apiUrl);
    if (!apiRes.ok) throw new Error(`ScrapingDog API returned ${apiRes.status}`);
    const jobs = await apiRes.json();

    const rows = jobs.map(j => [
      j.job_position || '',
      j.job_link || '',
      j.job_id || '',
      j.company_name || '',
      j.company_profile || '',
      j.job_location || '',
      j.job_posting_date || '',
      j.company_logo_url || '',
    ]);

    const hasGoogleCreds = true;

    if (hasGoogleCreds) {
      try {
        await appendToGoogleSheet(rows);
        console.log(`✅ Appended ${rows.length} rows to Google Sheets`);
      } catch (e) {
        console.error('⚠️ Failed to append to Google Sheets:', e.message);
      }
    } else {
      console.warn('⚠️ Google Sheets creds not set — skipping append.');
    }

    res.json({ success: true, jobs, rowCount: rows.length });
  } catch (err) {
    console.error('❌ Error in /api/fetch-jobs:', err.message);
    res.status(500).json({ success: false, message: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
