// utils/GoogleSheetsHelper.js
import { google } from 'googleapis';

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

  // ✅ Use the object constructor to avoid signature/arg-order issues
  const jwt = new google.auth.JWT({
    email,
    key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  await jwt.authorize(); // surfaces auth errors clearly
  return google.sheets({ version: 'v4', auth: jwt });
}

async function ensureSheetExists(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const found = meta.data.sheets?.find(s => s.properties?.title === sheetName);
  if (found) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] }
  });
}

async function ensureHeaders(sheets, spreadsheetId, sheetName, headers) {
  if (!headers?.length) return;
  const range = `${sheetName}!1:1`;
  const read = await sheets.spreadsheets.values.get({ spreadsheetId, range }).catch(() => null);
  const existing = read?.data?.values?.[0] || [];
  const same = existing.length === headers.length && existing.every((v, i) => v === headers[i]);
  if (!existing.length || !same) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
  }
}

export async function appendToGoogleSheet(
  rows,
  { sheetId, sheetName = 'Sheet1', headers, insertHeadersIfMissing = true } = {}
) {
  if (!sheetId) throw new Error('sheetId is required');
  const sheets = await getSheetsClient();

  console.log(`↗️ Google Sheets target: ${sheetId} (tab: ${sheetName})`);
  await ensureSheetExists(sheets, sheetId, sheetName);
  if (insertHeadersIfMissing && headers?.length) {
    await ensureHeaders(sheets, sheetId, sheetName, headers);
  }

  const range = `${sheetName}!A:A`;
  await sheets.spreadsheets.values.append({
    spreadsheetId: sheetId,
    range,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows }
  });

  return { success: true, saved: rows.length };
}
