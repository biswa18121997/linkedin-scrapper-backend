import { ApifyClient } from 'apify-client';
import 'dotenv/config';
import { appendToGoogleSheet } from '../utils/GoogleSheetsHelper.js';
// import Linkedin from './Linkedin';

function unionKeys(rows) {
  const s = new Set();
  for (const r of rows || []) if (r && typeof r === 'object') {
    for (const k of Object.keys(r)) s.add(k);
  }
  return Array.from(s);
}

export default async function Glassdoor(req, res) {
  let mapofTypes = {
    F: 'fulltime',
    P: 'parttime',
    C: 'contract',
    T: 'temporary',
    I: 'internship',
    V: 'volunteer',
  };
  let mapofSeniority = {
    all: 'all',
    1: 'internship',
    2: 'entrylevel',
    3: 'midseniorlevel',
    4: 'director',
    5: 'executive',
  };
  let mapOfPublish = {
    r86400: '1',
    r259200: '3',
    r604800: '7',
    r1209600: '14',
    r2592000: '30',
  };

  // Initialize the ApifyClient with API token
  const client = new ApifyClient({
    token: process.env.APIFY_API_KEY,
  });

  // Prepare Actor input
  const input = {
    keyword: req.body.title,
    maxItems: Number(req.body.limit),
    fromAge : mapOfPublish[req.body.publishedAt],
    baseUrl: 'https://www.glassdoor.com',
    includeNoSalaryJob: false,
    minSalary: 0,
    jobType: mapofTypes[req.body.contractType],
    radius: '0',
    industryType: 'ALL',
    domainType: 'ALL',
    employerSizes: 'ALL',
    applicationType: 'ALL',
    seniorityType: mapofSeniority[req.body.experienceLevel],
    remoteWorkType: req.body.workType == '1' ? false : true,
    minRating: '0',
    proxy: {
      useApifyProxy: true,
      apifyProxyGroups: ['RESIDENTIAL'],
    },
  };

  (async () => {
    // Run the Actor and wait for it to finish
    const run = await client.actor('t2FNNV3J6mvckgV2g').call(input);

    // Fetch and print Actor results from the run's dataset (if any)
    console.log('Results from dataset');
    const { items } = await client.dataset(run.defaultDatasetId).listItems();
    // items.forEach((item) => {
    //   console.dir(item);
    // });

    try {
      if (req.body.fetchfrom.includes('glassdoor')) {
        const sheetId = req.body.sheet_id; // from frontend
        const sheetName = 'Sheet3';
        const userID = String(req.body.userID || '');

        if (sheetId) {
          const rows = (items || []).map((obj) => ({ ...obj, userID }));
          const headers = unionKeys(rows); // column headers as key names (requirement)

          await appendToGoogleSheet(rows, {
            sheetId,
            sheetName,
            headers, // your helper will enforce [Done, ...headers, userID]
            valueInputOption: 'RAW',
            tickColName: 'Done',
            userColName: 'userID',
          });

          console.log('glassdoor :- appended to google sheets');

          res.status(200).json({
            messgae: 'scrape document..',
            Linkedin: req.body.linkedInItems,
            glassdoorItems: items,
          });
        } else {
          console.warn('Glassdoor: sheetId missing in request; skipping sheet append.');
          res.status(200).json({
            messgae: 'scrape document..',
            Linkedin: req.body.linkedInItems,
            glassdoorItems: items,
          });
        }
      } else {
        res.status(200).json({
          messgae: 'scrape document..',
          Linkedin: req.body.linkedInItems,
          // glassdoorItems : items
        });
      }
    } catch (e) {
      console.error('Glassdoor sheet append failed:', e?.message || e);
    }
  })();
}
