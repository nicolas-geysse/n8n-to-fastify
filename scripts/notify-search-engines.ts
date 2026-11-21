import crypto from 'node:crypto';

const GH_PAGES_SITE = 'https://rogeriomaciel.github.io/n8n-to-fastify/';
const GH_PAGES_SITEMAP = 'https://rogeriomaciel.github.io/n8n-to-fastify/sitemap.xml';
const INDEXNOW_KEY = '4f8e2c9a1b7d4e6f9a0c3d5e8b2a1f7c';
const MAIN_SITE = 'https://rogeriomaciel.com.br/';
const MAIN_GSC_PROPERTY = 'sc-domain:rogeriomaciel.com.br';
const MAIN_SITEMAP = 'https://rogeriomaciel.com.br/sitemap.xml';

const MAIN_URLS = [
  'https://rogeriomaciel.com.br/en/n8n-to-fastify/',
  'https://rogeriomaciel.com.br/en/solutions/n8n-out-of-memory-oom-solution/',
  'https://rogeriomaciel.com.br/en/solutions/compile-n8n-rabbitmq-ack-after-success/',
  'https://rogeriomaciel.com.br/en/solutions/redact-webhook-errors-structured-tracing-fastify/',
  'https://rogeriomaciel.com.br/en/solutions/preserve-binary-data-s3-multipart-compiled-workflows/',
  'https://rogeriomaciel.com.br/en/solutions/strict-workflow-compatibility-checks-deterministic-routing/',
  'https://rogeriomaciel.com.br/en/solutions/gitops-cicd-n8n-production/',
];

function base64url(input: Buffer | string): string {
  return Buffer.from(input).toString('base64').replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
}

async function notifyIndexNow(): Promise<void> {
  const res = await fetch('https://api.indexnow.org/indexnow', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      host: 'rogeriomaciel.github.io',
      key: INDEXNOW_KEY,
      keyLocation: `${GH_PAGES_SITE}${INDEXNOW_KEY}.txt`,
      urlList: [GH_PAGES_SITE, `${GH_PAGES_SITE}llms.txt`],
    }),
  });
  console.log(`[IndexNow] status=${res.status}`);
  if (!res.ok) throw new Error(`IndexNow failed with HTTP ${res.status}`);
}

async function notifyBing(apiKey: string): Promise<void> {
  const feedRes = await fetch(
    `https://ssl.bing.com/webmaster/api.svc/json/SubmitFeed?apikey=${encodeURIComponent(apiKey)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ siteUrl: GH_PAGES_SITE, feedUrl: GH_PAGES_SITEMAP }),
    },
  );
  console.log(`[Bing SubmitFeed] status=${feedRes.status}`);
  if (!feedRes.ok) throw new Error(`Bing SubmitFeed failed with HTTP ${feedRes.status}`);

  const batchRes = await fetch(
    `https://ssl.bing.com/webmaster/api.svc/json/SubmitUrlBatch?apikey=${encodeURIComponent(apiKey)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ siteUrl: MAIN_SITE, urlList: MAIN_URLS }),
    },
  );
  console.log(`[Bing SubmitUrlBatch] status=${batchRes.status}`);
  if (!batchRes.ok) throw new Error(`Bing SubmitUrlBatch failed with HTTP ${batchRes.status}`);
}

async function getGoogleAccessToken(saJson: string): Promise<string> {
  const sa = JSON.parse(saJson) as { client_email: string; private_key: string };
  const now = Math.floor(Date.now() / 1000);
  const header = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const claim = base64url(
    JSON.stringify({
      iss: sa.client_email,
      scope: 'https://www.googleapis.com/auth/webmasters',
      aud: 'https://oauth2.googleapis.com/token',
      exp: now + 3600,
      iat: now,
    }),
  );
  const sigInput = `${header}.${claim}`;
  const signer = crypto.createSign('RSA-SHA256');
  signer.update(sigInput);
  const jwt = `${sigInput}.${base64url(signer.sign(sa.private_key))}`;

  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }),
  });
  if (!tokenRes.ok) throw new Error(`Google OAuth failed with HTTP ${tokenRes.status}`);
  const data = (await tokenRes.json()) as { access_token?: string };
  if (!data.access_token) throw new Error('Google OAuth response missing access_token');
  return data.access_token;
}

async function notifyGoogleSearchConsole(saJson: string): Promise<void> {
  const token = await getGoogleAccessToken(saJson);
  const targets: Array<[string, string]> = [
    [GH_PAGES_SITE, GH_PAGES_SITEMAP],
    [MAIN_GSC_PROPERTY, MAIN_SITEMAP],
  ];
  for (const [siteUrl, feedUrl] of targets) {
    const res = await fetch(
      `https://www.googleapis.com/webmasters/v3/sites/${encodeURIComponent(siteUrl)}/sitemaps/${encodeURIComponent(feedUrl)}`,
      { method: 'PUT', headers: { Authorization: `Bearer ${token}` } },
    );
    console.log(`[GSC SubmitSitemap] site=${siteUrl} status=${res.status}`);
    if (!res.ok) throw new Error(`GSC SubmitSitemap failed for ${siteUrl} with HTTP ${res.status}`);
  }
}

await notifyIndexNow();

const bingKey = process.env.BING_WEBMASTER_API_KEY;
if (bingKey) await notifyBing(bingKey);
else console.log('[Bing] BING_WEBMASTER_API_KEY not set; skipped');

const gscJson = process.env.GSC_SERVICE_ACCOUNT_JSON;
if (gscJson) await notifyGoogleSearchConsole(gscJson);
else console.log('[GSC] GSC_SERVICE_ACCOUNT_JSON not set; skipped');
