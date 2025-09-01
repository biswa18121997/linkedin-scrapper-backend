
import { google } from 'googleapis';
import { JWT } from 'google-auth-library';

// -- Auth that accepts JSON/PEM/base64 --
function normalizeGooglePrivateKey(raw) {
  if (!raw) return '';
  let k = String(raw).trim();
  k = k.replace(/^['"]|['"]$/g, '');                            // strip quotes
  if (k.startsWith('{')) {                                      // full JSON -> extract private_key
    try { const obj = JSON.parse(k); k = String(obj.private_key || ''); } catch {}
  }
  if (!/BEGIN [A-Z ]*PRIVATE KEY/.test(k)) {                    // base64 -> decode
    try { const dec = Buffer.from(k, 'base64').toString('utf8'); if (/BEGIN [A-Z ]*PRIVATE KEY/.test(dec)) k = dec; } catch {}
  }
  return k.replace(/\\r\\n/g, '\n').replace(/\\n/g, '\n').trim(); // \n escapes -> real newlines
}

async function getSheetsClient() {
  const email = process.env.GOOGLE_CLIENT_EMAIL || process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const rawKey =
    process.env.GOOGLE_SERVICE_KEY ||
    process.env.GOOGLE_SERVICE_ACCOUNT_KEY ||
    process.env.GOOGLE_PRIVATE_KEY ||
    process.env.GOOGLE_SERVICE_KEY_BASE64;

  const key = normalizeGooglePrivateKey(rawKey);
  if (!email) throw new Error('GOOGLE_CLIENT_EMAIL not set');
  if (!key || !/BEGIN [A-Z ]*PRIVATE KEY/.test(key)) throw new Error('Service-account private key missing/malformed');

  const auth = new JWT({ email, key, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });
  return { sheets, auth };
}

// 0-based -> 'A','B',...'Z','AA',...
function toA1Col(idx0) {
  let n = Number(idx0);
  if (!Number.isFinite(n) || n < 0) n = 0;
  n += 1;
  let s = '';
  while (n > 0) {
    const rem = (n - 1) % 26;
    s = String.fromCharCode(65 + rem) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Ensure tab exists and has enough columns
async function ensureSheetAndColumns({ sheets, spreadsheetId, sheetName, needCols }) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId, fields: 'sheets.properties' });
  let sheet = (meta.data.sheets || []).find(s => s?.properties?.title === sheetName);

  if (!sheet) {
    const addResp = await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{
          addSheet: {
            properties: {
              title: sheetName,
              gridProperties: { rowCount: 1000, columnCount: Math.max(needCols, 26) },
            },
          },
        }],
      },
    });
    sheet = addResp.data.replies?.[0]?.addSheet?.properties;
  }

  const sheetId = sheet.properties.sheetId;
  const currentCols = sheet.properties.gridProperties.columnCount || 26;

  if (needCols > currentCols) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: { requests: [{
        appendDimension: { sheetId, dimension: 'COLUMNS', length: needCols - currentCols },
      }]},
    });
  }

  // Freeze header row
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{
      updateSheetProperties: {
        properties: { sheetId, gridProperties: { frozenRowCount: 1 } },
        fields: 'gridProperties.frozenRowCount',
      },
    }]},
  });

  return { sheetId };
}

// Add compact formatting + checkbox validation on column A
async function applyLayout({ sheets, spreadsheetId, sheetId, needCols, maxRows = 100000 }) {
  const requests = [];

  // Clip wrapping & vertical middle
  requests.push({
    repeatCell: {
      range: { sheetId },
      cell: { userEnteredFormat: { wrapStrategy: 'CLIP', verticalAlignment: 'MIDDLE' } },
      fields: 'userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment',
    },
  });

  // Header style
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: needCols },
      cell: { userEnteredFormat: { textFormat: { bold: true }, backgroundColor: { red: .95, green: .95, blue: .95 } } },
      fields: 'userEnteredFormat.textFormat.bold,userEnteredFormat.backgroundColor',
    },
  });

  // Compact row height
  requests.push({
    updateDimensionProperties: {
      range: { sheetId, dimension: 'ROWS', startIndex: 1, endIndex: maxRows },
      properties: { pixelSize: 22 },
      fields: 'pixelSize',
    },
  });

  // Auto-resize columns
  requests.push({
    autoResizeDimensions: { dimensions: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: needCols } },
  });

  // Checkbox validation in column A for all rows below header
  requests.push({
    setDataValidation: {
      range: { sheetId, startRowIndex: 1, endRowIndex: maxRows, startColumnIndex: 0, endColumnIndex: 1 },
      rule: { condition: { type: 'BOOLEAN' }, showCustomUi: true },
    },
  });

  // Optional: enable filter
  requests.push({ setBasicFilter: { filter: { range: { sheetId } } } });

  await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests } });
}

// Enforce header shape: ['Done', ...unique API keys..., 'userID']
function enforceHeaderShape(inHeaders, tickCol = 'Done', userCol = 'userID') {
  const seen = new Set();
  const api = [];
  for (const h of inHeaders || []) {
    const name = String(h || '').trim();
    if (!name) continue;
    if (name === tickCol || name === userCol) continue; // never duplicate our fixed columns
    if (!seen.has(name)) { seen.add(name); api.push(name); }
  }
  return [tickCol, ...api, userCol];
}

export async function appendToGoogleSheet(
  rowsAsObjects,
  {
    sheetId: spreadsheetId,
    sheetName,
    headers,                         // API headers (we will enforce Done/userID)
    insertHeadersIfMissing = true,   // we will always (re)write row 1 to keep alignment
    valueInputOption = 'RAW',
    tickColName = 'Done',
    userColName = 'userID',
  }
) {
  const { sheets } = await getSheetsClient();

  // 0) Force the header layout
  const finalHeaders = enforceHeaderShape(headers || [], tickColName, userColName);

  // 1) Ensure tab + columns
  const needCols = Math.max(1, finalHeaders.length);
  const { sheetId } = await ensureSheetAndColumns({ sheets, spreadsheetId, sheetName, needCols });

  // 2) CLEAR row 1 and write headers starting at A1 (prevents drift)
  const endCol = toA1Col(finalHeaders.length - 1);
  await sheets.spreadsheets.values.clear({ spreadsheetId, range: `${sheetName}!1:1` });
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1:${endCol}1`,
    valueInputOption,
    requestBody: { values: [finalHeaders] },
  });

  // 3) Build values in enforced order; ensure tick col and user col exist
  const values = (rowsAsObjects || []).map((row) =>
    finalHeaders.map((h) => {
      if (h === tickColName) return 'FALSE';        // unchecked by default
      let v = row?.[h];
      if (h === userColName && (v === undefined || v === null || v === '')) v = ''; // backend/apps script supplies the email
      if (v === undefined || v === null) v = '';
      if (typeof v === 'object') {
        try { v = JSON.stringify(v); } catch { v = String(v); }
      }
      return String(v);
    })
  );

  // 4) Append rows (if any)
  if (values.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${sheetName}!A1`,
      valueInputOption,
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values },
    });
  }

  // 5) Make the sheet compact and add checkbox validation on col A
  await applyLayout({ sheets, spreadsheetId, sheetId, needCols });
}
