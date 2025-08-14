// utils/GoogleSheetsHelper.js
import { google } from 'googleapis';

// Google Sheets & Service Account info (HARD-CODED)
const spreadsheetId = '1DLbb760YrOWjd_REfqgTgAFTis4hkfgMNE7QvEjxo2E'; // your sheet ID
const sheetName = 'Sheet1';

// Hardcoded service account JSON
const serviceAccount = {
  type: 'service_account',
  project_id: 'flashfire-464713',
  private_key_id: '62ed54a18fec7c1fd8b82e473ea8a307c028ba43',
  private_key: `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDAde+tgn1x8dc6
Z8Ahd7F7GbYOJ38AeF/NdA9doTFohvriuWynuEFkLihP6DTUwmn/KVR+JZbqs3Dy
MESf/OFB8PwTH4/TFlEsn+Le8+TpmRP52ue0fJX6mdA2ouSKHwJ0qEJNJaqxt1Kr
PXtFVYYqf2yiNwftPEhVv6UZvMuLhLA1Qda7R7T4lz46r/O4+Mwg9dAcRrNDKIjf
YZ8bj2H4fxzzJtoEgBSZOklTjgukRDodLWTMLsw0ZcnvpPpJC6xm32N7/VnENu8q
vBTxt+OeAMM+ln8reK4qTWdviTJimDPu7Jxv8Jm0HtBL/OW6xNMEi31MEtlDBaRG
SKYAmaqNAgMBAAECggEAJwB/ZrkX9Nxekm5uCBo6dEjclPe6C/1Y5MjNSFsfKSCZ
fRK4izCSx9t2veK/uhH/6v6UKdAySjO9Asd3ULitaXCNlM6DlfJi1tk735SrYg9s
dei/pdrNhfBfuoK+L6NjGwLyFLI6ajNNZHhcH8vaGYijGihuycu6mO3yZknVMduC
ghJz0FD0yl1FE2ZCkgcbgKUty/ogaqsFXL3vNvcWjezwDyyuibuFEJyV4nQzTFD5
oDq7/HHwg/3vsm1HPqMzwbBwfG7M5Zq3NeBSJs64ox0yPGM/hfDSHvD9CnObf1h1
Chkh7AINop8DcDgXiNLSnpJbJWCpqCLtyGzRjTOvAQKBgQDzvzJvLO5qg6BeTxfP
Ahm4MLZrqnoTSrnMAMsMuGjvWwhXmkKiDySeO7q3KmnDKCGst95QbxMX6mYbF5QK
FOKXpaNp5ip1c9rr4sLlprn+tI/dRve+YMI0IUsv5ow6RSUb9JWYWjXbF4ppoSHo
v3ic20pVYxoteekF/JK4QJv2TQKBgQDKIrt2EzhnjpSeQScI0SGMpbvsgiWEQYll
OkbZOi7+/9tQITp2oodWcTRsw+KTxAI1ebvxvb0PwNzvWkYRfpM+aV8++uZamc6h
Iyp42KuZC8se9EG/8KjxqhzMZHyiAvc4iwUJRG9TaB49eIkmBWkY5KI22pN+EU6e
9KyUmYMlQQKBgAo3emq0jG3EhKVPVWUk5mUVDaBnreQ/HpiRc/FdjXBy9V+OpLpc
PiGvyTzCN1qpxPeTYWsnrLo05gC+tULS60iF9dqLfj4cFBINGDQ+D8/AS8NvpRTC
w4Eh4B/q3vfWTB7m2ppfNaCwVOnmiiBSXkDc5Dm+BKvhT0Yj9xZoBuGFAoGAOXRN
3G3yJl08mQ7jzXnEE3o4RC1qBIGsT/2UjcIgAZMv/0Kyn23rEgLzZ8b17BJWnmSP
q1LHHmcvZUk/iVF1ANRqojgmqbH2LY8VT2wmukXD4nSDC8+X9bjonqAhXNuC8aty
LwQosIhzr/1G3mvDR7QU94qBSeAZfM0HEOXhhQECgYAbxXyUU+fpGwLgqYZb1ZBt
N8d7StmyN/oRXl2B+No7fhxqWO5OkdTTGV3tbRMqo/VVleQKjjmEB0t4T153r36l
LapH2ELwjl7qflLILFeChikw76QxBuU/hMOKPLKkq1lz1D3FWOuyJ+j2bSM42zaK
S9AvJa59CUfBsgnvYZRfAg==
-----END PRIVATE KEY-----`,
  client_email: 'flashfire@flashfire-464713.iam.gserviceaccount.com',
  client_id: '110437416262232850623',
  auth_uri: 'https://accounts.google.com/o/oauth2/auth',
  token_uri: 'https://oauth2.googleapis.com/token',
  auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
  client_x509_cert_url: 'https://www.googleapis.com/robot/v1/metadata/x509/flashfire%40flashfire-464713.iam.gserviceaccount.com',
  universe_domain: 'googleapis.com',
};

function getAuth() {
  return new google.auth.GoogleAuth({
    credentials: {
      client_email: serviceAccount.client_email,
      private_key: serviceAccount.private_key,
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

export async function appendToGoogleSheet(rows) {
  const auth = getAuth();
  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });

  const res = await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: 'USER_ENTERED',
    resource: { values: rows },
  });

  console.log(`✅ Appended ${rows.length} rows to Google Sheet`);
  return res.data;
}
