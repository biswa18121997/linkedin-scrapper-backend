// // utils/GoogleSheetsHelper.js
// import { google } from "googleapis";

// // helper: 1 -> A, 2 -> B, ...
// function columnToLetter(n) {
//   let s = "";
//   while (n > 0) {
//     const m = (n - 1) % 26;
//     s = String.fromCharCode(65 + m) + s;
//     n = (n - m - 1) / 26;
//   }
//   return s;
// }

// // ---- NEW: auth that uses your env without changing it ----
// async function getSheetsClient() {
//   const SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];

//   // 1) Full JSON in env (you already have GOOGLE_SERVICE_KEY)
//   const inlineJson =
//     process.env.GOOGLE_SERVICE_KEY ||
//     process.env.GOOGLE_SHEETS_CREDENTIALS ||
//     process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;

//   if (inlineJson) {
//     // If dotenv wrapped it in quotes, JSON.parse still works
//     const creds = JSON.parse(inlineJson);
//     const jwt = new google.auth.JWT({
//       email: creds.client_email,
//       key: String(creds.private_key || "").replace(/\\n/g, "\n"),
//       scopes: SCOPES,
//     });
//     await jwt.authorize();
//     return google.sheets({ version: "v4", auth: jwt });
//   }

//   // 2) Separate vars (optional path)
//   if (process.env.GOOGLE_CLIENT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
//     const jwt = new google.auth.JWT({
//       email: process.env.GOOGLE_CLIENT_EMAIL,
//       key: String(process.env.GOOGLE_PRIVATE_KEY).replace(/\\n/g, "\n"),
//       scopes: SCOPES,
//     });
//     await jwt.authorize();
//     return google.sheets({ version: "v4", auth: jwt });
//   }

//   // 3) ADC fallback (gcloud etc.)
//   const auth = new google.auth.GoogleAuth({ scopes: SCOPES });
//   const client = await auth.getClient();
//   return google.sheets({ version: "v4", auth: client });
// }

// /**
//  * Append rows to a Google Sheet under a header row.
//  * - Writes/updates the header in row 1 starting at firstDataColIndex
//  * - Appends row values aligned to those headers
//  *
//  * @param {Array<Object>} rows - array of objects keyed by headers
//  * @param {{
//  *   sheetId: string,
//  *   sheetName: string,
//  *   headers: string[],
//  *   insertHeadersIfMissing?: boolean,
//  *   valueInputOption?: 'RAW'|'USER_ENTERED',
//  *   firstDataColIndex?: number,   // default 1; set 2 to skip checkbox col A
//  * }} opts
//  */
// export async function appendToGoogleSheet(rows, opts) {
//   const {
//     sheetId,
//     sheetName,
//     headers,
//     insertHeadersIfMissing = true,
//     valueInputOption = "RAW",
//     firstDataColIndex = 1,
//   } = opts || {};

//   if (!sheetId || !sheetName) throw new Error("sheetId and sheetName are required");
//   if (!headers || !headers.length) throw new Error("headers required");
//   if (!Array.isArray(rows) || rows.length === 0) return;

//   const sheets = await getSheetsClient();

//   const startCol = firstDataColIndex;                 // e.g., 2 means write in column B
//   const endCol = firstDataColIndex + headers.length - 1;
//   const startLetter = columnToLetter(startCol);
//   const endLetter = columnToLetter(endCol);

//   // 1) Ensure header row (row 1) is in place from startCol..endCol
//   if (insertHeadersIfMissing) {
//     await sheets.spreadsheets.values.update({
//       spreadsheetId: sheetId,
//       range: `${sheetName}!${startLetter}1:${endLetter}1`,
//       valueInputOption: "RAW",
//       requestBody: { values: [headers] },
//     });
//   }

//   // 2) Build 2D array aligned to provided headers
//   const values = rows.map((r) => headers.map((h) => (r[h] ?? "")));

//   // 3) Append starting at startCol (we never touch the checkbox column A)
//   await sheets.spreadsheets.values.append({
//   spreadsheetId: sheetId,
//   range: `${sheetName}!A1`,            // ✅ simple, always valid
//   valueInputOption,
//   insertDataOption: 'INSERT_ROWS',
//   requestBody: { values },             // 2D array of row values in header order
// });
// }


// utils/GoogleSheetsHelper.js
import { google } from 'googleapis';
import { JWT } from 'google-auth-library';

// ---- keep your auth as-is; example shown for context ----
function normalizeGooglePrivateKey(raw) {
  if (!raw) return '';
  let k = String(raw).trim();

  // strip outer quotes if present
  k = k.replace(/^['"]|['"]$/g, '');

  // if it's full JSON, extract private_key
  if (k.startsWith('{')) {
    try {
      const obj = JSON.parse(k);
      k = String(obj.private_key || '');
    } catch { /* leave as-is */ }
  }

  // if it's base64, try to decode to PEM
  if (!/BEGIN [A-Z ]*PRIVATE KEY/.test(k)) {
    try {
      const dec = Buffer.from(k, 'base64').toString('utf8');
      if (/BEGIN [A-Z ]*PRIVATE KEY/.test(dec)) k = dec;
    } catch { /* ignore */ }
  }

  // convert \n escapes to real newlines (for .env single-line PEM)
  k = k.replace(/\\r\\n/g, '\n').replace(/\\n/g, '\n').trim();
  return k;
}

// ---- keep the rest of your file unchanged; just replace this function ----
async function getSheetsClient() {
  const email =
    process.env.GOOGLE_CLIENT_EMAIL || process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;

  // accept any of these env vars for the key
  const rawKey =
    process.env.GOOGLE_SERVICE_KEY ||
    process.env.GOOGLE_SERVICE_ACCOUNT_KEY ||
    process.env.GOOGLE_PRIVATE_KEY ||
    process.env.GOOGLE_SERVICE_KEY_BASE64;

  const key = normalizeGooglePrivateKey(rawKey);

  if (!email) throw new Error('GOOGLE_CLIENT_EMAIL not set');
  if (!key || !/BEGIN [A-Z ]*PRIVATE KEY/.test(key)) {
    throw new Error('Service-account private key missing/malformed in env');
  }

  const auth = new JWT({
    email,
    key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });
  return { sheets, auth };
}

export { getSheetsClient }; // if you import it el

// 0-based -> 'A','B',...'Z','AA','AB',...
function toA1Col(idx0) {
  let n = Number(idx0);
  if (!Number.isFinite(n) || n < 0) n = 0;
  n += 1; // 1-based
  let s = '';
  while (n > 0) {
    const rem = (n - 1) % 26;
    s = String.fromCharCode(65 + rem) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Find (or create) a sheet/tab by title and ensure it has at least `needCols` columns
async function ensureSheetAndColumns({ sheets, spreadsheetId, sheetName, needCols }) {
  // read minimal metadata
  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets.properties',
  });

  let sheet = (meta.data.sheets || []).find(
    s => s?.properties?.title === sheetName
  );

  if (!sheet) {
    // create sheet with enough columns
    const addResp = await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            addSheet: {
              properties: {
                title: sheetName,
                gridProperties: {
                  rowCount: 1000,
                  columnCount: Math.max(needCols, 26),
                },
              },
            },
          },
        ],
      },
    });
    sheet = addResp.data.replies?.[0]?.addSheet?.properties;
  }

  const sheetId = sheet.properties.sheetId;
  const currentCols = sheet.properties.gridProperties.columnCount || 26;

  if (needCols > currentCols) {
    // Add columns so A1 range never exceeds grid
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            appendDimension: {
              sheetId,
              dimension: 'COLUMNS',
              length: needCols - currentCols,
            },
          },
        ],
      },
    });
  }

  return { sheetId, columnCount: Math.max(currentCols, needCols) };
}

export async function appendToGoogleSheet(
  rowsAsObjects,
  {
    sheetId: spreadsheetId,
    sheetName,
    headers, // final header order (e.g., ["Done", ...apiHeaders, "userID"])
    insertHeadersIfMissing = true,
    valueInputOption = 'RAW',
    // firstDataColIndex is no longer needed; we always anchor at A1 safely
  }
) {
  const { sheets } = await getSheetsClient();

  // 1) Make sure the tab exists and has enough columns
  const needCols = Math.max(1, headers?.length || 1);
  await ensureSheetAndColumns({ sheets, spreadsheetId, sheetName, needCols });

  // 2) Ensure header row is present (or overwrite to keep in sync)
  if (insertHeadersIfMissing && Array.isArray(headers) && headers.length > 0) {
    // compute correct A1 range for headers: A1 : <endCol>1
    const endCol = toA1Col(headers.length - 1);
    const headerRange = `${sheetName}!A1:${endCol}1`; // ✅ correct order

    // Read existing header row
    const current = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!1:1`,
    });
    const existing = current.data.values?.[0] || [];

    // If empty or different length, just update with our headers
    if (existing.length !== headers.length || existing.join('¬') !== headers.join('¬')) {
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: headerRange,
        valueInputOption,
        requestBody: { values: [headers] },
      });
    }
  }

  // 3) Build 2D array for rows in header order
  const values = (rowsAsObjects || []).map((obj) =>
    headers.map((h) => {
      let v = obj?.[h];
      if (v === undefined || v === null) v = '';
      if (typeof v === 'object') {
        try { v = JSON.stringify(v); } catch { v = String(v); }
      }
      return String(v);
    })
  );

  if (!values.length) return;

  // 4) Append rows at A1 (Google finds the next row automatically)
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A1`,            // ✅ always valid
    valueInputOption,
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values },
  });
}
