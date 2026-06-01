// api/google.js  ─  Vercel Serverless Function (Google Ads API 프록시)

const OAUTH_URL   = 'https://oauth2.googleapis.com/token';
const GADS_BASE   = 'https://googleads.googleapis.com/v23';

// ── Access Token 갱신 ──────────────────────────────────────────
async function refreshAccessToken(clientId, clientSecret, refreshToken) {
  const params = new URLSearchParams({
    client_id:     clientId,
    client_secret: clientSecret,
    refresh_token: refreshToken,
    grant_type:    'refresh_token',
  });
  const resp = await fetch(OAUTH_URL, {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    params.toString(),
  });
  const data = await resp.json();
  if (!resp.ok || !data.access_token)
    throw new Error('Access Token 갱신 실패: ' + (data.error_description || data.error || resp.status));
  return data.access_token;
}

// ── GAQL 쿼리 실행 ─────────────────────────────────────────────
async function runGaqlQuery(accessToken, devtok, mcc, cid, since, until) {
  const cleanCid = cid.replace(/-/g, '');
  const query = `
    SELECT
      metrics.cost_micros,
      metrics.impressions,
      metrics.clicks,
      metrics.conversions,
      metrics.conversions_value
    FROM campaign
    WHERE segments.date BETWEEN '${since}' AND '${until}'
      AND campaign.status != 'REMOVED'
  `.trim();

  const headers = {
    'Content-Type':  'application/json',
    'Authorization': `Bearer ${accessToken}`,
  };
  if (devtok) headers['developer-token']   = devtok;
  if (mcc)    headers['login-customer-id'] = mcc.replace(/-/g, '');

  const url  = `${GADS_BASE}/customers/${cleanCid}/googleAds:searchStream`;
  const resp = await fetch(url, {
    method:  'POST',
    headers,
    body:    JSON.stringify({ query }),
  });

  if (!resp.ok) {
    const errBody = await resp.json().catch(() => ({}));
    const msg = errBody?.error?.message
      || errBody?.error?.details?.[0]?.errors?.[0]?.message
      || 'Google Ads API 오류 ' + resp.status;
    throw new Error(msg);
  }

  // searchStream → NDJSON (각 줄이 JSON 배열 또는 객체)
  const text  = await resp.text();
  const rows  = [];

  // searchStream은 JSON 배열로 전체가 오거나 NDJSON으로 올 수 있음
  try {
    const parsed = JSON.parse(text);
    const chunks = Array.isArray(parsed) ? parsed : [parsed];
    for (const chunk of chunks) {
      if (chunk.results) rows.push(...chunk.results);
    }
  } catch (_) {
    // NDJSON fallback
    const lines = text.trim().split('\n').filter(Boolean);
    for (const line of lines) {
      try {
        const parsed  = JSON.parse(line);
        const results = Array.isArray(parsed) ? parsed : [parsed];
        for (const r of results) {
          if (r.results) rows.push(...r.results);
        }
      } catch (_) {}
    }
  }
  return rows;
}

// ── 집계 & 파생 지표 계산 ──────────────────────────────────────
function parseRows(rows) {
  if (!rows || !rows.length)
    return { cost:0, impressions:0, clicks:0, conversions:0, revenue:0,
             cpc:0, cpm:0, ctr:0, cvr:0, roas:0, cpa:0, aov:0 };

  let costMicros = 0, impressions = 0, clicks = 0, conversions = 0, revenue = 0;
  for (const row of rows) {
    const m = row.metrics || {};
    // Google Ads API v23: 응답은 camelCase (costMicros, conversionsValue 등)
    // 숫자형으로 내려오므로 String 변환 없이 직접 사용
    costMicros  += Number(m.costMicros   ?? m.cost_micros   ?? 0);
    impressions += Number(m.impressions  ?? 0);
    clicks      += Number(m.clicks       ?? 0);
    conversions += Number(m.conversions  ?? 0);
    revenue     += Number(m.conversionsValue ?? m.conversions_value ?? 0);
  }

  const cost = costMicros / 1_000_000;
  const ctr  = impressions ? (clicks      / impressions * 100) : 0;
  const cvr  = clicks      ? (conversions / clicks      * 100) : 0;
  const roas = cost        ? (revenue     / cost         * 100) : 0;
  const cpc  = clicks      ? Math.round(cost / clicks)          : 0;
  const cpm  = impressions ? Math.round(cost / impressions * 1000) : 0;
  const cpa  = conversions ? Math.round(cost / conversions)      : 0;
  const aov  = conversions ? Math.round(revenue / conversions)   : 0;

  return {
    cost:        Math.round(cost),
    impressions,
    clicks,
    conversions,
    revenue:     Math.round(revenue),
    cpc, cpm,
    ctr:  parseFloat(ctr.toFixed(4)),
    cvr:  parseFloat(cvr.toFixed(4)),
    roas: parseFloat(roas.toFixed(2)),
    cpa, aov,
  };
}

// ── 이전 기간 자동 계산 ────────────────────────────────────────
function calcPrevRange(since, until) {
  const curS = new Date(since + 'T00:00:00Z');
  const curE = new Date(until + 'T00:00:00Z');

  const isFirstDay = d => d.getUTCDate() === 1;
  const isLastDay  = d => {
    const next = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1));
    return d.getUTCDate() === new Date(next - 1).getUTCDate();
  };
  const fmt = d => d.toISOString().slice(0, 10);

  // 전월 전체인 경우 → 전전월 전체
  if (isFirstDay(curS) && isLastDay(curE) && curS.getUTCMonth() === curE.getUTCMonth()) {
    const s = new Date(Date.UTC(curS.getUTCFullYear(), curS.getUTCMonth() - 1, 1));
    const e = new Date(Date.UTC(curS.getUTCFullYear(), curS.getUTCMonth(), 0));
    return { prevSince: fmt(s), prevUntil: fmt(e) };
  }
  // 그 외 → 동일 일수 직전 기간
  const pe = new Date(curS.getTime() - 86400000);
  const ps = new Date(pe.getTime()   - (curE.getTime() - curS.getTime()));
  return { prevSince: fmt(ps), prevUntil: fmt(pe) };
}

// ── 메인 핸들러 ───────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST')   return res.status(405).json({ error: 'Method not allowed' });

  // body 파싱
  let body = req.body;
  if (!body || typeof body === 'string' || Object.keys(body || {}).length === 0) {
    body = await new Promise(resolve => {
      let raw = '';
      req.on('data', c => raw += c);
      req.on('end', () => { try { resolve(JSON.parse(raw)); } catch { resolve({}); } });
    });
  }

  const clientId     = String(body?.clientId     || '').trim();
  const clientSecret = String(body?.clientSecret || '').trim();
  const refreshTok   = String(body?.refreshTok   || '').trim();
  const cid          = String(body?.cid          || '').trim();
  const devtok       = String(body?.devtok       || '').trim();
  const mcc          = String(body?.mcc          || '').trim();
  const since        = String(body?.since        || '').trim();
  const until        = String(body?.until        || '').trim();

  if (!clientId || !clientSecret || !refreshTok || !cid || !since || !until)
    return res.status(400).json({ error: '필수값 누락 (clientId, clientSecret, refreshTok, cid, since, until)' });

  try {
    // Access Token 발급
    const accessToken = await refreshAccessToken(clientId, clientSecret, refreshTok);

    // 이전 기간 계산
    const { prevSince, prevUntil } = calcPrevRange(since, until);

    // 현재 / 이전 기간 병렬 조회
    const [curRows, prevRows] = await Promise.all([
      runGaqlQuery(accessToken, devtok, mcc, cid, since, until),
      runGaqlQuery(accessToken, devtok, mcc, cid, prevSince, prevUntil),
    ]);

    const curAgg  = parseRows(curRows);
    const prevAgg = parseRows(prevRows);

    return res.json({ curAgg, prevAgg, prevSince, prevUntil });

  } catch (err) {
    console.error('[google] error:', err);
    return res.status(500).json({ error: err.message });
  }
};
