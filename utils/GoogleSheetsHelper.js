// utils/GoogleSheetsHelper.js
import { google } from 'googleapis';

/* -------------------------- AUTH UTILITIES -------------------------- */

function normalizePrivateKey(k = '') {
  let key = (k || '').trim();
  // strip accidental wrapping quotes
  if ((key.startsWith('"') && key.endsWith('"')) || (key.startsWith("'") && key.endsWith("'"))) {
    key = key.slice(1, -1);
  }
  // convert literal \n to real newlines
  return key.replace(/\\n/g, '\n');
}

function readServiceAccountFromEnv() {
  // If explicit vars are present, use them and skip JSON/B64 entirely
  const hasPair =
    (process.env.GOOGLE_CLIENT_EMAIL || '').trim() &&
    (process.env.GOOGLE_PRIVATE_KEY || '').trim();
  if (hasPair) return null;

  // Prefer base64
  let b64 = process.env.GOOGLE_SERVICE_KEY_B64?.trim();
  if (b64) {
    try {
      // If someone pasted JSON into _B64 by mistake, handle it directly
      if (b64.startsWith('{')) return JSON.parse(b64);
      const json = Buffer.from(b64, 'base64').toString('utf8').replace(/^\uFEFF/, '');
      return JSON.parse(json);
    } catch {
      throw new Error('GOOGLE_SERVICE_KEY_B64 is not valid base64/JSON');
    }
  }

  // Fallback: plain JSON string
  let raw = process.env.GOOGLE_SERVICE_KEY;
  if (raw && raw.trim()) {
    raw = raw.trim();
    if ((raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'"))) {
      raw = raw.slice(1, -1);
    }
    raw = raw.replace(/^\uFEFF/, '');
    try {
      return JSON.parse(raw);
    } catch {
      throw new Error('GOOGLE_SERVICE_KEY is not valid JSON. Use one line with \\n in private_key or prefer GOOGLE_SERVICE_KEY_B64.');
    }
  }

  return null;
}

export async function getSheetsClient() {
  let email = process.env.GOOGLE_CLIENT_EMAIL || '';
  let key = process.env.GOOGLE_PRIVATE_KEY || '';

  const svc = readServiceAccountFromEnv();
  if (svc) {
    email = svc.client_email || email;
    key = svc.private_key || key;
  }

  key = normalizePrivateKey(key);

  if (!email) throw new Error('Google SA client_email missing.');
  if (!key) throw new Error('Google SA private_key missing.');

  console.log('[Sheets Auth] Email present:', !!email, '| Key length:', key.length);

  const jwt = new google.auth.JWT({
    email,
    key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  await jwt.authorize(); // surfaces auth errors clearly
  return google.sheets({ version: 'v4', auth: jwt });
}

/* --------------------------- SHEETS HELPERS -------------------------- */

async function ensureSheetExists(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const found = meta.data.sheets?.find((s) => s.properties?.title === sheetName);
  if (found) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
  });
}

async function getExistingHeaders(sheets, spreadsheetId, sheetName) {
  const range = `${sheetName}!1:1`;
  const read = await sheets.spreadsheets.values.get({ spreadsheetId, range }).catch(() => null);
  return read?.data?.values?.[0] || [];
}

async function writeHeaders(sheets, spreadsheetId, sheetName, headers) {
  const range = `${sheetName}!1:1`;
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range,
    valueInputOption: 'RAW',
    requestBody: { values: [headers] },
  });
}

function isObjectRow(r) {
  return r && !Array.isArray(r) && typeof r === 'object';
}

function collectAllKeys(rows) {
  const set = new Set();
  for (const r of rows) Object.keys(r || {}).forEach((k) => set.add(k));
  return Array.from(set);
}

function makeRowsFromObjects(rows, headers) {
  return rows.map((obj) => headers.map((h) => (obj?.[h] ?? '')));
}

function validateArrayRows(rows, expectedCols) {
  rows.forEach((r, i) => {
    if (!Array.isArray(r) || r.length !== expectedCols) {
      throw new Error(`Row ${i} has ${r?.length || 0} cols; expected ${expectedCols}`);
    }
  });
}

/* --------------------------- PUBLIC APPEND --------------------------- */

/**
 * Append data to Google Sheets.
 *
 * rows:
 *  - Array-of-arrays (old behavior), where width must match headers length (if provided)
 *  - OR array-of-objects (new), where columns auto-expand to include all object keys
 *
 * options:
 *  - sheetId (required)
 *  - sheetName = 'Sheet1'
 *  - headers?: string[]             (optional if rows are objects)
 *  - insertHeadersIfMissing = true
 *  - valueInputOption = 'RAW'       ('RAW' | 'USER_ENTERED')
 *  - chunkSize = 5000               (rows per API append call)
 */
export async function appendToGoogleSheet(
  rows,
  {
    sheetId,
    sheetName = 'Sheet1',
    headers,
    insertHeadersIfMissing = true,
    valueInputOption = 'RAW',
    chunkSize = 5000,
  } = {}
) {
  if (!sheetId) throw new Error('sheetId is required');
  if (!Array.isArray(rows) || rows.length === 0) {
    return { success: true, saved: 0, message: 'No rows to append.' };
  }

  const sheets = await getSheetsClient();

  console.log(`↗️ Google Sheets target: ${sheetId} (tab: ${sheetName})`);
  await ensureSheetExists(sheets, sheetId, sheetName);

  let finalHeaders = headers && headers.length ? [...headers] : null;
  const rowsAreObjects = isObjectRow(rows[0]);

  if (rowsAreObjects) {
    // Build header set from existing + new keys (keeps order)
    const existing = await getExistingHeaders(sheets, sheetId, sheetName);
    const newKeys = collectAllKeys(rows);
    if (finalHeaders) {
      // If caller supplied headers, keep that order, then add any missing keys
      const merged = finalHeaders.slice();
      for (const k of newKeys) if (!merged.includes(k)) merged.push(k);
      finalHeaders = merged;
    } else if (existing.length) {
      // Use existing, then add any new keys
      const merged = existing.slice();
      for (const k of newKeys) if (!merged.includes(k)) merged.push(k);
      finalHeaders = merged;
    } else {
      // No headers in sheet and none supplied -> use discovered keys
      finalHeaders = newKeys;
    }

    // Ensure headers row is present/up-to-date
    if (insertHeadersIfMissing !== false) {
      await writeHeaders(sheets, sheetId, sheetName, finalHeaders);
    }

    // Convert object rows → arrays per finalHeaders order
    rows = makeRowsFromObjects(rows, finalHeaders);
  } else {
    // Array rows: keep strict behavior with optional headers
    if (insertHeadersIfMissing && finalHeaders?.length) {
      const existing = await getExistingHeaders(sheets, sheetId, sheetName);
      const same =
        existing.length === finalHeaders.length && existing.every((v, i) => v === finalHeaders[i]);
      if (!existing.length || !same) {
        await writeHeaders(sheets, sheetId, sheetName, finalHeaders);
      }
      validateArrayRows(rows, finalHeaders.length);
    } else if (!finalHeaders?.length) {
      // No headers: ensure consistent width
      const expected = rows[0]?.length ?? 0;
      validateArrayRows(rows, expected);
    }
  }

  // Append in chunks to avoid request size limits
  const range = `${sheetName}!A:A`;
  let saved = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const slice = rows.slice(i, i + chunkSize);
    await sheets.spreadsheets.values.append({
      spreadsheetId: sheetId,
      range,
      valueInputOption,
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: slice },
    });
    saved += slice.length;
  }

  return { success: true, saved };
}
