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
async function apiRequest(cid, lic, sec, method, pathWithQuery, payload) {
  const pathOnly  = pathWithQuery.split('?')[0];
  const timestamp = Date.now().toString();
  const sig       = makeSignature(sec, timestamp, method, pathOnly);

  const headers = {
    'Content-Type': 'application/json; charset=UTF-8',
    'X-Timestamp': timestamp,
    'X-API-KEY'  : lic,
    'X-Customer' : cid,
    'X-Signature': sig,
  };

  const opts = { method, headers };
  if (method !== 'GET' && method !== 'DELETE' && payload !== undefined) {
    opts.body = JSON.stringify(payload);
  }

  const url = BASE_URL + pathWithQuery;
  const r = await fetch(url, opts);
  const txt = await r.text();
  let data;
  try { data = txt ? JSON.parse(txt) : {}; } catch { data = txt; }
  return { status: r.status, data };
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


/**
 * EXPKEYWORD (Powerlink Search Term Report) 기반 전환검색어 집계
 * - 문서상 /stat-reports 로 생성 후 /stat-reports/{jobId} 로 다운로드 URL 확인
 * - 환경/계정에 따라 zip/csv 형식이 다를 수 있어, csv(텍스트) 다운로드를 우선 처리
 */
async function createStatReport(cid, lic, sec, statDtYYYYMMDD, reportTp){
  const payload = { statDt: statDtYYYYMMDD, reportTp };
  return apiRequest(cid, lic, sec, 'POST', '/stat-reports', payload);
}
async function getStatReport(cid, lic, sec, jobId){
  return apiRequest(cid, lic, sec, 'GET', `/stat-reports/${encodeURIComponent(jobId)}`);
}
async function tryDownloadText(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error('다운로드 실패('+r.status+')');
  const ct = (r.headers.get('content-type')||'').toLowerCase();
  const buf = await r.arrayBuffer();
  // zip(바이너리)면 여기서는 처리 불가 → 빈 반환
  const sig = new Uint8Array(buf).slice(0,4);
  const isZip = sig.length>=2 && sig[0]===0x50 && sig[1]===0x4B; // PK
  if(isZip) return null;
  return new TextDecoder('utf-8').decode(buf);
}
function parseCsvLines(text){
  // 매우 단순 CSV 파서 (따옴표/콤마 포함 케이스 최소 지원)
  const lines = text.split(/\r?\n/).filter(Boolean);
  if(lines.length<2) return {header:[], rows:[]};
  const split = (line)=>{
    const out=[]; let cur=''; let q=false;
    for(let i=0;i<line.length;i++){
      const ch=line[i];
      if(ch==='"'){ q=!q; continue; }
      if(ch===',' && !q){ out.push(cur); cur=''; continue; }
      cur+=ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  };
  const header = split(lines[0]);
  const rows = lines.slice(1).map(split);
  return {header, rows};
}

async function getExpKeywordTerms(cid, lic, sec, startDate, endDate){
  // startDate/endDate: YYYYMMDD
  const s = parseYMD(startDate), e = parseYMD(endDate);
  const days=[];
  for(let d=new Date(s); d<=e; d=new Date(d.getTime()+86400000)){
    days.push(fmtYMD(d));
  }
  // 너무 길면 서버리스 타임아웃 위험 → 31일 제한
  const limited = days.slice(0,31);

  const agg = {}; // term -> metrics
  for(const dt of limited){
    const crt = await createStatReport(cid, lic, sec, dt, 'EXPKEYWORD');
    if(crt.status!==201 && crt.status!==200){
      continue;
    }
    const jobId = crt.data?.reportJobId || crt.data?.reportJobId?.toString?.() || crt.data?.id || crt.data?.reportId;
    if(!jobId) continue;

    let info=null;
    for(let i=0;i<12;i++){
      const r = await getStatReport(cid, lic, sec, jobId);
      if(r.status!==200){ await sleep(250); continue; }
      const st = (r.data?.status || r.data?.reportStatus || '').toString();
      info=r.data;
      if(st==='BUILT' || st==='COMPLETED' || st==='DONE') break;
      if(st==='ERROR' || st==='FAILED') { info=null; break; }
      await sleep(350);
    }
    if(!info) continue;
    const url = info.downloadUrl || info.downloadURL || info.downloadLink;
    if(!url) continue;

    const text = await tryDownloadText(url);
    if(!text) continue; // zip이면 스킵
    const {header, rows} = parseCsvLines(text);
    const idx = (name)=>header.findIndex(h=>h.toLowerCase()===name.toLowerCase());

    const iTerm = idx('Keyword')>=0?idx('Keyword'):idx('Search term');
    const iImp  = idx('Impression')>=0?idx('Impression'):idx('impCnt');
    const iClk  = idx('Click')>=0?idx('Click'):idx('clkCnt');
    const iCost = idx('Cost')>=0?idx('Cost'):idx('salesAmt');
    const iConv = idx('Conversion count')>=0?idx('Conversion count'):idx('ccnt');
    const iRev  = idx('Sales by conversion')>=0?idx('Sales by conversion'):idx('convAmt');

    for(const r of rows){
      const term = (iTerm>=0 ? r[iTerm] : '') || '';
      if(!term) continue;
      if(!agg[term]) agg[term]={name:term,cost:0,imp:0,click:0,conv:0,revenue:0,prevConv:0,prevCost:0,prevClick:0};
      agg[term].imp   += Number(iImp>=0 ? r[iImp] : 0) || 0;
      agg[term].click += Number(iClk>=0 ? r[iClk] : 0) || 0;
      agg[term].cost  += Number(iCost>=0 ? r[iCost] : 0) || 0;
      agg[term].conv  += Number(iConv>=0 ? r[iConv] : 0) || 0;
      agg[term].revenue += Number(iRev>=0 ? r[iRev] : 0) || 0;
    }
  }
  return Object.values(agg).map(v=>({ ...v, roas: v.cost?Math.round(v.revenue/v.cost*100):0 }));
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


// 8. 전환 검색어 (EXPKEYWORD) - best effort
let convTerms = [];
try {
  const [curTerms, prevTerms] = await Promise.all([
    getExpKeywordTerms(cid, lic, sec, startDate, endDate),
    getExpKeywordTerms(cid, lic, sec, prevStart, prevEnd),
  ]);
  const prevMap = {};
  (prevTerms||[]).forEach(t=>{ prevMap[t.name]=t; });
  convTerms = (curTerms||[]).map(t=>{
    const p = prevMap[t.name] || {conv:0,cost:0,click:0};
    return { ...t, prevConv: p.conv||0, prevCost: p.cost||0, prevClick: p.click||0 };
  }).filter(t=>Number(t.conv||0)>0)
    .sort((a,b)=> (b.conv||0)-(a.conv||0))
    .slice(0,200);
} catch(e) {
  console.warn('[expkeyword] skip:', e.message);
  convTerms = [];
}

    return res.json({
      days: formattedDays, recentDays, recent21Days, prevAgg, convKws, convTerms, spendKws, clickKws, allCamps, allGroups,
      _meta: { period: `${startDate}~${endDate}`, campCount: campaigns.length, kwCount: keywords.length },
    });

  } catch (err) {
    console.error('[naver] error:', err);
    return res.status(500).json({ error: err.message, stack: err.stack });
  }
};
