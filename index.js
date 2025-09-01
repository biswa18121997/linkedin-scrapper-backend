// index.js — Apify actors → Google Sheets with dynamic columns + checkbox + userID (fault-tolerant)
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { ApifyClient } from 'apify-client';
import { appendToGoogleSheet } from './utils/GoogleSheetsHelper.js';
import Linkedin from './src/Linkedin.js';
import Glassdoor from './src/Glassdoor.js';
import Indeed from './src/Indeed.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 8085;

app.use(cors());
app.use(express.json());

app.post('/api/fetch', Linkedin, Indeed,  Glassdoor);

const server = createServer(app);
server.requestTimeout = 0;
server.headersTimeout = 0;
server.keepAliveTimeout = 75_000;

server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
