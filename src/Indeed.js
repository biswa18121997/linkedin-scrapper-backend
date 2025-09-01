import { ApifyClient } from 'apify-client';
import dotenv from 'dotenv/config';
import { appendToGoogleSheet } from '../utils/GoogleSheetsHelper.js';

function unionKeys(rows) {
  const s = new Set();
  for (const r of rows || []) if (r && typeof r === 'object') {
    for (const k of Object.keys(r)) s.add(k);
  }
  return Array.from(s);
}

export default async function Indeed(req,res,next){
  try {
  if(req.body.fetchfrom.includes('indeed')){
    let mapofTypes = {
    F: 'fulltime',
    P: 'parttime',
    C: 'contract',
    T: '',
    I: 'internship',
    V: '',
  };
  let mapofPPublishedAt = {
    r86400: '24h',
    r259200: '48h',
    r604800: '48h',
    r1209600: '72h',
    r2592000: '72h'
  }
    const client = new ApifyClient({
        token: process.env.APIFY_API_KEY,
    });
// Prepare Actor input
const input = {
    title: req.body.title,
    city: "San Francisco",
    country: "USA",
    engines: "1",
    jobtype: mapofTypes[req.body.contractType],
    distance: "15",
    remote: req.body.workType == '2' ? 'Yes' : 'No' ,
    last: mapofPPublishedAt[req.body.mapofPPublishedAt],
    max: Number(req.body.limit),
    delay: 3,
    proxy: {
        "useApifyProxy": true
    }
};
// (async () => {
  Object.keys(input).forEach((k) => input[k] === undefined && delete input[k]);
  console.log('process started..');
  const run = await client.actor("9eTAaHrnHrljnL3Tg").call(input);
    const { items } = await client.dataset(run.defaultDatasetId).listItems();
     req.body.indeedItems = items || [];
            const sheetId = req.body.sheet_id;            // <-- from frontend
            const sheetName = 'Sheet2';
            const userID = String(req.body.userID || '');
            if (sheetId) {
              const liRows = (items || []).map(obj => ({ ...obj, userID }));    
              const headers = unionKeys(liRows);
              await appendToGoogleSheet(liRows, {
                sheetId,
                sheetName,
                headers,                 // headers as key names (requirement satisfied)
                valueInputOption: 'RAW', // keep as is
                tickColName: 'Done',     // your helper expects this name for the checkbox col
                userColName: 'userID',   // last column
              });
              console.log('indeed appended to google sheets');
              req.body.indeed = items;
};
}
  next();   
  } catch (error) {
    console.log(error);
    next();
  }
}
// {
//   "city": "Atlanta",
//   "proxy": {
//     "useApifyProxy": true
//   },
//   "title": "Architect",
//   "country": "USA",
//   "engines": "1",
//   "jobtype": "fulltime",
//   "distance": "15",
//   "remote": "No",
//   "last": "all",
//   "max": 40,
//   "delay": 3
// }