// utils/GoogleSheetsHelper.js
import { google } from "googleapis";

// helper: 1 -> A, 2 -> B, ...
function columnToLetter(n) {
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = (n - m - 1) / 26;
  }
  return s;
}

// ---- NEW: auth that uses your env without changing it ----
async function getSheetsClient() {
  const SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];

  // 1) Full JSON in env (you already have GOOGLE_SERVICE_KEY)
  const inlineJson =
    process.env.GOOGLE_SERVICE_KEY ||
    process.env.GOOGLE_SHEETS_CREDENTIALS ||
    process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;

  if (inlineJson) {
    // If dotenv wrapped it in quotes, JSON.parse still works
    const creds = JSON.parse(inlineJson);
    const jwt = new google.auth.JWT({
      email: creds.client_email,
      key: String(creds.private_key || "").replace(/\\n/g, "\n"),
      scopes: SCOPES,
    });
    await jwt.authorize();
    return google.sheets({ version: "v4", auth: jwt });
  }

  // 2) Separate vars (optional path)
  if (process.env.GOOGLE_CLIENT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
    const jwt = new google.auth.JWT({
      email: process.env.GOOGLE_CLIENT_EMAIL,
      key: String(process.env.GOOGLE_PRIVATE_KEY).replace(/\\n/g, "\n"),
      scopes: SCOPES,
    });
    await jwt.authorize();
    return google.sheets({ version: "v4", auth: jwt });
  }

  // 3) ADC fallback (gcloud etc.)
  const auth = new google.auth.GoogleAuth({ scopes: SCOPES });
  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
}

/**
 * Append rows to a Google Sheet under a header row.
 * - Writes/updates the header in row 1 starting at firstDataColIndex
 * - Appends row values aligned to those headers
 *
 * @param {Array<Object>} rows - array of objects keyed by headers
 * @param {{
 *   sheetId: string,
 *   sheetName: string,
 *   headers: string[],
 *   insertHeadersIfMissing?: boolean,
 *   valueInputOption?: 'RAW'|'USER_ENTERED',
 *   firstDataColIndex?: number,   // default 1; set 2 to skip checkbox col A
 * }} opts
 */
export async function appendToGoogleSheet(rows, opts) {
  const {
    sheetId,
    sheetName,
    headers,
    insertHeadersIfMissing = true,
    valueInputOption = "RAW",
    firstDataColIndex = 1,
  } = opts || {};

  if (!sheetId || !sheetName) throw new Error("sheetId and sheetName are required");
  if (!headers || !headers.length) throw new Error("headers required");
  if (!Array.isArray(rows) || rows.length === 0) return;

  const sheets = await getSheetsClient();

  const startCol = firstDataColIndex;                 // e.g., 2 means write in column B
  const endCol = firstDataColIndex + headers.length - 1;
  const startLetter = columnToLetter(startCol);
  const endLetter = columnToLetter(endCol);

  // 1) Ensure header row (row 1) is in place from startCol..endCol
  if (insertHeadersIfMissing) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!${startLetter}1:${endLetter}1`,
      valueInputOption: "RAW",
      requestBody: { values: [headers] },
    });
  }

  // 2) Build 2D array aligned to provided headers
  const values = rows.map((r) => headers.map((h) => (r[h] ?? "")));

  // 3) Append starting at startCol (we never touch the checkbox column A)
  await sheets.spreadsheets.values.append({
    spreadsheetId: sheetId,
    range: `${sheetName}!${startLetter}:${endLetter}`,
    valueInputOption,
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });
}
