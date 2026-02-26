/**
 * Naver Search Ad API Proxy - Vercel Serverless Function
 * ✅ 수정사항:
 *   1. API 도메인: api.searchad.naver.com → api.naver.com
 *   2. 서명 포맷: timestamp\nMETHOD\nuri → timestamp.METHOD.path (점 구분자)
 *   3. stats 파라미터: fields[]=... → fields=["..."] JSON 형식
 */

const crypto = require('crypto');

const API_BASE = 'https://api.naver.com';

// ─── 서명 생성 ────────────────────────────────────────────────
function makeSignature(secretKey, timestamp, method, path) {
  const message = `${timestamp}.${method}.${path}`; // 점(.) 구분자
  return crypto
    .createHmac('sha256', secretKey)
    .update(message, 'utf8')
    .digest('base64');
}

// ─── API 요청 (fetch 사용 → 리다이렉트 자동 처리) ───────────────
async function apiRequest(cid, lic, sec, method, pathWithQuery) {
  const pathOnly  = pathWithQuery.split('?')[0];
  const timestamp = Date.now().toString();
  const sig       = makeSignature(sec, timestamp, method, pathOnly);
  const url       = `${API_BASE}${pathWithQuery}`;

  console.log(`[req] ${method} ${url.slice(0, 200)}`);

  const res = await fetch(url, {
    method,
    headers: {
      'Content-Type': 'application/json; charset=UTF-8',
      'X-Timestamp' : timestamp,
      'X-API-KEY'   : lic,
      'X-Customer'  : String(cid).trim(),
      'X-Signature' : sig,
    },
  });

  const text = await res.text();
  console.log(`[res] ${res.status} ${text.slice(0, 300)}`);

  let data;
  try   { data = JSON.parse(text); }
  catch { data = { raw: text }; }

  return { status: res.status, data };
}

// ─── 날짜 ────────────────────────────────────────────────────
function parseYMD(s) {
  s = String(s);
  return new Date(`${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}T00:00:00Z`);
}
function fmtYMD(d) { return d.toISOString().slice(0,10).replace(/-/g,''); }
function fmtISO(s) { // YYYYMMDD → YYYY-MM-DD
  s = String(s);
  return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
}

// ─── stats URL ✅ 수정: JSON 형식 파라미터 ──────────────────────
function statsUrl(ids, start, end, timeUnit, breakdown) {
  if (!ids || ids.length === 0) return null;

  const idsQuery  = ids.slice(0, 200).map(id => `ids=${encodeURIComponent(id)}`).join('&');
  const fields    = encodeURIComponent('["impCnt","clkCnt","salesAmt","ccnt","convAmt"]');
  const timeRange = encodeURIComponent(`{"since":"${fmtISO(start)}","until":"${fmtISO(end)}"}`);

  let url = `/stats?${idsQuery}&fields=${fields}&timeRange=${timeRange}`;
  if (timeUnit)   url += `&timeUnit=${encodeURIComponent(timeUnit)}`;
  if (breakdown)  url += `&breakdown=${encodeURIComponent(breakdown)}`;
  return url;
}

// ─── 집계 ────────────────────────────────────────────────────
function aggRow(map, id, row) {
  if (!id) return;
  if (!map[id]) map[id] = { cost:0, imp:0, click:0, conv:0, revenue:0 };
  map[id].cost    += Number(row.salesAmt || row.cost    || 0);
  map[id].imp     += Number(row.impCnt   || 0);
  map[id].click   += Number(row.clkCnt   || row.click   || 0);
  map[id].conv    += Number(row.ccnt     || row.ctcCnt  || row.convCnt || 0);
  map[id].revenue += Number(row.convAmt  || row.revenue || 0);
}
function calcMetrics(v) {
  return { ...v,
    roas: v.cost  ? Math.round(v.revenue / v.cost * 100)             : 0,
    ctr : v.imp   ? parseFloat((v.click / v.imp * 100).toFixed(2))   : 0,
    cvr : v.click ? parseFloat((v.conv  / v.click * 100).toFixed(2)) : 0,
    cpc : v.click ? Math.round(v.cost / v.click)                     : 0,
    cpa : v.conv  ? Math.round(v.cost / v.conv)                      : 0,
    aov : v.conv  ? Math.round(v.revenue / v.conv)                   : 0, // 객단가
    rpc : v.click ? Math.round(v.revenue / v.click)                  : 0, // 클릭당매출액
  };
}


// ─── 핸들러 ──────────────────────────────────────────────────
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

  const cid       = String(body?.cid       || '').trim();
  const lic       = String(body?.lic       || '').trim();
  const sec       = String(body?.sec       || '').trim();
  const startDate = String(body?.startDate || '').trim();
  const endDate   = String(body?.endDate   || '').trim();

  console.log(`[req] cid=${cid} lic=${lic.slice(0,8)}... sec=${sec.slice(0,4)}... start=${startDate} end=${endDate}`);

  if (!cid || !lic || !sec || !startDate || !endDate)
    return res.status(400).json({ error: '필수값 누락' });

  // 이전 기간 계산
  const curS  = parseYMD(startDate), curE = parseYMD(endDate);
  const prevE = new Date(curS - 86400000);
  const prevS = new Date(prevE - (curE - curS));
  const prevStart = fmtYMD(prevS), prevEnd = fmtYMD(prevE);

  try {
    // 1. 캠페인
    const campRes = await apiRequest(cid, lic, sec, 'GET', '/ncc/campaigns');
    if (campRes.status !== 200)
      return res.status(campRes.status).json({ error: `캠페인 API 오류 (${campRes.status})`, detail: campRes.data });

    const campaigns = (campRes.data?.campaigns || campRes.data || [])
      .filter(c => !['DELETED', 'PAUSED_BY_BUDGET'].includes(c.status));
    const campIds = campaigns.map(c => c.nccCampaignId || c.campaignId || c.id).filter(Boolean);

    if (!campIds.length)
      return res.json({ days:[], convKws:[], spendKws:[], clickKws:[], allCamps:[], allGroups:[], _warn:'활성 캠페인 없음' });

    // 2. 광고그룹
    const grpRes   = await apiRequest(cid, lic, sec, 'GET', '/ncc/adgroups');
    const grpRaw   = grpRes.data;
    const adgroups = Array.isArray(grpRaw)
      ? grpRaw
      : (grpRaw?.adGroups || grpRaw?.items || grpRaw?.data || []);
    const groupIds = adgroups.map(g => g.nccAdgroupId || g.adGroupId || g.id).filter(Boolean);

    // 3. 통계 병렬 (현재+이전+최근8일)
    const curEDate = parseYMD(endDate);
    const rec8S    = new Date(curEDate - 7*86400000);
    const rec8Start = fmtYMD(rec8S);

    
    const rec21S    = new Date(curEDate - 20*86400000);
    const rec21Start = fmtYMD(rec21S);
const [dailyCurR, campCurR, grpCurR, campPrevR, daily8R, daily21R] = await Promise.all([
apiRequest(cid, lic, sec, 'GET', statsUrl(campIds, startDate, endDate, 'DAY')),
      apiRequest(cid, lic, sec, 'GET', statsUrl(campIds, startDate, endDate)),
      groupIds.length
        ? apiRequest(cid, lic, sec, 'GET', statsUrl(groupIds, startDate, endDate))
        : { data: [] },
      apiRequest(cid, lic, sec, 'GET', statsUrl(campIds, prevStart, prevEnd)),
      apiRequest(cid, lic, sec, 'GET', statsUrl(campIds, rec8Start, endDate, 'DAY')),,
      apiRequest(cid, lic, sec, 'GET', statsUrl(campIds, rec21Start, endDate, 'DAY')),
    ]);

    // 4. 일별 (period 필드 없으면 startDate로 집계)
    const dailyMap = {};
    const toSafeArr = v => Array.isArray(v) ? v : [];
    toSafeArr(dailyCurR?.data?.data || dailyCurR?.data).forEach(row => {
      const d = row.period || row.date || row.statDate || row.statDt || startDate;
      const key = String(d).replace(/-/g, '');
      if (!dailyMap[key]) dailyMap[key] = { cost:0, imp:0, click:0, conv:0, revenue:0 };
      aggRow(dailyMap, key, row);
    });
    const formattedDays = Object.entries(dailyMap)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ds, v]) => ({
        date   : `${ds.slice(4,6)}/${ds.slice(6,8)}`,
        dateRaw: ds,
        ...calcMetrics(v),
      }));

    // 5. 캠페인별
    const campCurMap = {};
    toSafeArr(campCurR?.data?.data || campCurR?.data).forEach(r => {
      aggRow(campCurMap, r.nccCampaignId || r.campaignId || r.id, r);
    });
    const allCamps = campaigns.map(c => {
      const id = c.nccCampaignId || c.campaignId || c.id;
      return { id, name: c.name, ...calcMetrics(campCurMap[id] || { cost:0, imp:0, click:0, conv:0, revenue:0 }) };
    }).sort((a, b) => b.cost - a.cost);

    // 6. 그룹별
    const grpCurMap = {};
    toSafeArr(grpCurR?.data?.data || grpCurR?.data).forEach(r => {
      aggRow(grpCurMap, r.nccAdgroupId || r.adGroupId || r.id, r);
    });
    const allGroups = adgroups.map(g => {
      const id = g.nccAdgroupId || g.adGroupId || g.id;
      return { id, name: g.name, ...calcMetrics(grpCurMap[id] || { cost:0, imp:0, click:0, conv:0, revenue:0 }) };
    }).sort((a, b) => b.cost - a.cost);

    // 6b. prevAgg (이전 기간 합산)
    const prevAggMap = {};
    const toSafeArr2 = v => Array.isArray(v) ? v : [];
    toSafeArr2(campPrevR?.data?.data || campPrevR?.data).forEach(r => aggRow(prevAggMap, 'total', r));
    const prevAgg = calcMetrics(prevAggMap['total'] || { cost:0, imp:0, click:0, conv:0, revenue:0 });

    // 6c. 최근 8일 daily (차트/일별표 고정용)
    const daily8Map = {};
    toSafeArr2(daily8R?.data?.data || daily8R?.data).forEach(row => {
      const d = row.period || row.date || row.statDate || row.statDt || rec8Start;
      const key = String(d).replace(/-/g, '');
      if (!daily8Map[key]) daily8Map[key] = { cost:0, imp:0, click:0, conv:0, revenue:0 };
      aggRow(daily8Map, key, row);
    });
    const recentDays = Object.entries(daily8Map)
      .sort(([a],[b]) => a.localeCompare(b))
      .map(([ds, v]) => ({
        date   : `${ds.slice(4,6)}/${ds.slice(6,8)}`,
        dateRaw: ds,
        ...calcMetrics(v),
      }));

    // 6d. 최근 21일 daily (주간 비교 차트용)
    const daily21Map = {};
    toSafeArr2(daily21R?.data?.data || daily21R?.data).forEach(row => {
      const d = row.period || row.date || row.statDate || row.statDt || rec21Start;
      const key = String(d).replace(/-/g, '');
      if (!daily21Map[key]) daily21Map[key] = { cost:0, imp:0, click:0, conv:0, revenue:0 };
      aggRow(daily21Map, key, row);
    });
    const recent21Days = Object.entries(daily21Map)
      .sort(([a],[b]) => a.localeCompare(b))
      .map(([ds, v]) => ({
        date   : `${ds.slice(4,6)}/${ds.slice(6,8)}`,
        dateRaw: ds,
        ...calcMetrics(v),
      }));

    // 7. 키워드 (비용 발생한 상위 10개 광고그룹에서 수집)
    const activeGroupIds = allGroups
      .filter(g => g.imp > 0 || g.cost > 0)
      .slice(0, 10)
      .map(g => g.id);
    const topGroupIds = activeGroupIds.length ? activeGroupIds : groupIds.slice(0, 5);
    const kwResponses = await Promise.all(
      topGroupIds.map(gid => apiRequest(cid, lic, sec, 'GET', `/ncc/keywords?nccAdgroupId=${gid}&returnPause=true`))
    );
    const kwRaw = kwResponses.flatMap(r => {
      const d = r.data;
      return Array.isArray(d) ? d : (d?.keywords || d?.items || d?.data || []);
    });
    const keywords = kwRaw;
    const kwRes = { status: 200, data: kwRaw }; // debug 호환용
    console.log('[kw] 키워드 수:', keywords.length);
    const kwIds    = keywords.map(k => k.nccKeywordId || k.keywordId || k.id).filter(Boolean).slice(0, 200);

    let convKws = [], spendKws = [], clickKws = [];
    if (kwIds.length > 0) {
      const [kwCurR, kwPrevR] = await Promise.all([
        apiRequest(cid, lic, sec, 'GET', statsUrl(kwIds, startDate, endDate)),
        apiRequest(cid, lic, sec, 'GET', statsUrl(kwIds, prevStart, prevEnd)),
      ]);
      const kwCurMap = {}, kwPrevMap = {};
      const extractKwId = r => r.nccKeywordId || r.keywordId || r.id;
      const toArr = v => Array.isArray(v) ? v : [];
      toArr(kwCurR?.data?.data  || kwCurR?.data ).forEach(r => aggRow(kwCurMap,  extractKwId(r), r));
      toArr(kwPrevR?.data?.data || kwPrevR?.data).forEach(r => aggRow(kwPrevMap, extractKwId(r), r));

      const kwNameMap = {};
      keywords.forEach(k => {
        kwNameMap[k.nccKeywordId || k.keywordId || k.id] = k.keyword || k.keyword_text || k.keywordName || k.name || '';
      });

      const kwList = kwIds.map(id => {
        const cur  = kwCurMap[id]  || { cost:0, click:0, conv:0, revenue:0 };
        const prev = kwPrevMap[id] || { cost:0, click:0, conv:0, revenue:0 };
        return {
          name     : kwNameMap[id] || id,
          cost     : cur.cost,  click: cur.click, conv: cur.conv, revenue: cur.revenue,
          prevCost : prev.cost, prevClick: prev.click, prevConv: prev.conv,
          roas     : cur.cost ? Math.round(cur.revenue / cur.cost * 100) : 0,
        };
      });

      convKws  = kwList.filter(k => k.conv  > 0).sort((a, b) => b.conv  - a.conv).slice(0, 200);
      spendKws = [...kwList].sort((a, b) => b.cost  - a.cost).slice(0, 10);
      clickKws = [...kwList].sort((a, b) => b.click - a.click).slice(0, 10);
    }

    return res.json({
      days: formattedDays, recentDays, recent21Days, prevAgg, convKws, spendKws, clickKws, allCamps, allGroups,
      _meta: { period: `${startDate}~${endDate}`, campCount: campaigns.length, kwCount: keywords.length },
    });

  } catch (err) {
    console.error('[naver] error:', err);
    return res.status(500).json({ error: err.message, stack: err.stack });
  }
};
