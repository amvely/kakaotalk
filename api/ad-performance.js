// api/ad-performance.js
// Naver SearchAd + Meta Marketing API + Google Ads API 통합 성과 API
// 샘플 데이터를 반환하지 않습니다. 연결 가능한 실제 API 응답만 공통 스키마로 내려줍니다.

const crypto = require('crypto');

const NAVER_BASE = 'https://api.searchad.naver.com';
const META_BASE = 'https://graph.facebook.com';
const META_VERSION = process.env.META_API_VERSION || 'v20.0';
const GOOGLE_OAUTH_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_ADS_BASE = process.env.GOOGLE_ADS_BASE || 'https://googleads.googleapis.com/v23';

function toStr(v){ return v === undefined || v === null ? '' : String(v).trim(); }
function n(v){ const x = Number(v); return Number.isFinite(x) ? x : 0; }
function ymdToISO(s){ s = String(s || '').replace(/-/g,'').slice(0,8); return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`; }
function isoToYmd(s){ return String(s || '').replace(/-/g,'').slice(0,8); }
function parseYMD(s){ return new Date(`${ymdToISO(s)}T00:00:00Z`); }
function fmtYMD(d){ return d.toISOString().slice(0,10).replace(/-/g,''); }
function addDays(d, days){ const x = new Date(d); x.setUTCDate(x.getUTCDate() + days); return x; }
function prevRange(startDate, endDate){ const s=parseYMD(startDate), e=parseYMD(endDate); const pe=addDays(s,-1); const span=Math.round((e-s)/86400000); const ps=addDays(pe,-span); return [fmtYMD(ps), fmtYMD(pe)]; }
function chunk(arr, size=200){ const out=[]; for(let i=0;i<arr.length;i+=size) out.push(arr.slice(i,i+size)); return out; }
function inferSaleType(row){
  if(row.saleType === 'sales' || row.saleType === 'nonsales') return row.saleType;
  const t = `${row.type || ''} ${row.campaignName || row.campNm || row.name || ''}`.toLowerCase();
  if(t.includes('비판매') || t.includes('brand') || t.includes('브랜드') || t.includes('인지')) return 'nonsales';
  if(t.includes('판매') || t.includes('shopping') || t.includes('shop') || t.includes('전환') || t.includes('p-max') || t.includes('pmax')) return 'sales';
  return 'all';
}
function blank(){ return {cost:0, imp:0, click:0, conv:0, revenue:0, purchaseCcnt:0, purchaseConvAmt:0, cart:0}; }
function calc(v={}){
  const out = {
    cost:n(v.cost ?? v.salesAmt ?? v.spend),
    imp:n(v.imp ?? v.impCnt ?? v.impressions),
    click:n(v.click ?? v.clkCnt ?? v.clicks),
    conv:n(v.conv ?? v.ccnt ?? v.conversions),
    revenue:n(v.revenue ?? v.convAmt ?? v.conversionsValue),
    purchaseCcnt:n(v.purchaseCcnt ?? v.purchaseConversions ?? v.purchaseConv),
    purchaseConvAmt:n(v.purchaseConvAmt ?? v.purchaseRevenue ?? v.purchaseValue)
  };
  out.cart = n(v.cart ?? Math.max(0, out.conv - out.purchaseCcnt));
  out.ctr = out.imp ? out.click / out.imp * 100 : 0;
  out.cvr = out.click ? out.conv / out.click * 100 : 0;
  out.cpc = out.click ? Math.round(out.cost / out.click) : 0;
  out.cpa = out.conv ? Math.round(out.cost / out.conv) : 0;
  out.roas = out.cost ? out.revenue / out.cost * 100 : 0;
  out.aov = out.conv ? Math.round(out.revenue / out.conv) : 0;
  return out;
}
function addTo(acc, m){ acc.cost+=n(m.cost); acc.imp+=n(m.imp); acc.click+=n(m.click); acc.conv+=n(m.conv); acc.revenue+=n(m.revenue); acc.purchaseCcnt+=n(m.purchaseCcnt); acc.purchaseConvAmt+=n(m.purchaseConvAmt); acc.cart+=n(m.cart ?? Math.max(0,n(m.conv)-n(m.purchaseCcnt))); }
function aggregate(rows){ const acc=blank(); for(const r of rows||[]) addTo(acc, r); return calc(acc); }
function metricWithPrev(cur, prev){ return {...calc(cur), _prev: calc(prev || {})}; }
function responseHeaders(res){ res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Methods','POST, OPTIONS'); res.setHeader('Access-Control-Allow-Headers','Content-Type'); }
async function parseBody(req){
  if(req.body && typeof req.body === 'object' && Object.keys(req.body).length) return req.body;
  if(req.body && typeof req.body === 'string'){ try { return JSON.parse(req.body); } catch {} }
  return await new Promise(resolve=>{ let raw=''; req.on('data', c=>raw+=c); req.on('end',()=>{ try{ resolve(JSON.parse(raw || '{}')); } catch { resolve({}); } }); });
}
function joinErrors(errors){ return errors.map(e => e && e.message ? e.message : String(e)).join(' / '); }

// ─────────────────────────────────────────────────────────────────────────────
// NAVER
function makeNaverSignature(secretKey, timestamp, method, path){
  const msg = `${timestamp}.${String(method).toUpperCase()}.${path}`;
  return crypto.createHmac('sha256', secretKey).update(msg, 'utf8').digest('base64');
}
async function naverReq(cid, lic, sec, method, pathWithQuery){
  const m = String(method).toUpperCase();
  const pathOnly = pathWithQuery.split('?')[0];
  const timestamp = Date.now().toString();
  const sig = makeNaverSignature(sec, timestamp, m, pathOnly);
  const resp = await fetch(NAVER_BASE + pathWithQuery, { method:m, headers:{ 'X-Timestamp':timestamp, 'X-API-KEY':lic, 'X-Customer':cid, 'X-Signature':sig }});
  const txt = await resp.text();
  let data; try { data = txt ? JSON.parse(txt) : {}; } catch { data = txt; }
  if(!resp.ok) throw new Error(`Naver ${pathOnly} 오류 (${resp.status}) ${typeof data === 'string' ? data : JSON.stringify(data).slice(0,300)}`);
  return data;
}
function arrFrom(data){ if(Array.isArray(data)) return data; if(Array.isArray(data?.data)) return data.data; if(Array.isArray(data?.items)) return data.items; if(Array.isArray(data?.campaigns)) return data.campaigns; if(Array.isArray(data?.adgroups)) return data.adgroups; return []; }
function naverStatsUrl(ids,start,end,timeIncrement){
  const idsQuery = ids.slice(0,200).map(id=>`ids=${encodeURIComponent(id)}`).join('&');
  const fields = encodeURIComponent(JSON.stringify(['impCnt','clkCnt','salesAmt','ccnt','convAmt','purchaseCcnt','purchaseConvAmt']));
  const timeRange = encodeURIComponent(JSON.stringify({since:ymdToISO(start), until:ymdToISO(end)}));
  let url = `/stats?${idsQuery}&fields=${fields}&timeRange=${timeRange}`;
  if(timeIncrement !== undefined && timeIncrement !== null && timeIncrement !== '') url += `&timeIncrement=${encodeURIComponent(timeIncrement)}`;
  return url;
}
function flattenNaverStats(resp){
  const base = arrFrom(resp); const out=[];
  for(const item of base){
    const parentId = item.id || item.nccCampaignId || item.campaignId || item.nccAdgroupId || item.adgroupId;
    const nested = Array.isArray(item.data) ? item.data : Array.isArray(item.rows) ? item.rows : Array.isArray(item.stats) ? item.stats : Array.isArray(item.items) ? item.items : null;
    if(nested){ for(const r of nested) out.push({...r, id:r.id||parentId, statDate:r.statDate||r.date||r.period||item.statDate||item.date||item.period}); }
    else out.push(item);
  }
  return out;
}
function naverRowId(row){ return row.id || row.nccCampaignId || row.campaignId || row.nccAdgroupId || row.adgroupId; }
function naverRowDate(row){ const s=String(row.statDate||row.date||row.period||'').replace(/-/g,''); return s.length >= 8 ? s.slice(0,8) : ''; }
function naverMetric(row){
  const s = row.stat || row;
  return calc({cost:s.salesAmt||s.cost, imp:s.impCnt||s.imp, click:s.clkCnt||s.click, conv:s.ccnt||s.conv, revenue:s.convAmt||s.revenue, purchaseCcnt:s.purchaseCcnt, purchaseConvAmt:s.purchaseConvAmt});
}
function naverAggById(rows){
  const map={};
  for(const r of rows){ const id=naverRowId(r); if(!id) continue; if(!map[id]) map[id]=blank(); addTo(map[id], naverMetric(r)); }
  const out={}; for(const [k,v] of Object.entries(map)) out[k]=calc(v); return out;
}
function naverAggByDay(rows){
  const map={};
  for(const r of rows){ const dt=naverRowDate(r); if(!dt) continue; if(!map[dt]) map[dt]={dt,date:`${dt.slice(4,6)}/${dt.slice(6,8)}`,platform:'naver',...blank()}; addTo(map[dt], naverMetric(r)); }
  return Object.values(map).sort((a,b)=>a.dt.localeCompare(b.dt)).map(v=>({...v,...calc(v),platform:'naver'}));
}
async function naverFetchStats(cid, lic, sec, ids, start, end, timeIncrement){
  const rows=[];
  for(const part of chunk(ids,200)){
    if(!part.length) continue;
    rows.push(...flattenNaverStats(await naverReq(cid, lic, sec, 'GET', naverStatsUrl(part,start,end,timeIncrement))));
  }
  return rows;
}
async function naverFetchDaily(cid, lic, sec, ids, start, end){
  let rows = await naverFetchStats(cid, lic, sec, ids, start, end, 1).catch(()=>[]);
  if(naverAggByDay(rows).length) return rows;
  rows = [];
  let d=parseYMD(start), e=parseYMD(end);
  while(d<=e){
    const y=fmtYMD(d);
    const dayRows = await naverFetchStats(cid, lic, sec, ids, y, y, null).catch(()=>[]);
    dayRows.forEach(r=>{ if(!r.statDate && !r.date && !r.period) r.statDate = y; });
    rows.push(...dayRows);
    d = addDays(d,1);
  }
  return rows;
}
function naverTypeLabel(tp){ const s=String(tp||''); return ({'1':'파워링크','2':'쇼핑검색','3':'파워컨텐츠','4':'브랜드검색광고','5':'플레이스','WEB_SITE':'파워링크','SHOPPING':'쇼핑검색','POWER_CONTENTS':'파워컨텐츠','BRAND_SEARCH':'브랜드검색광고','PLACE':'플레이스','PLACE_SEARCH':'플레이스','LOCAL':'플레이스'})[s] || s || '기타'; }
async function fetchNaver(body){
  const cfg = body.naver || {};
  const cid = toStr(cfg.cid || body.naverCid || process.env.NAVER_CUSTOMER_ID);
  const lic = toStr(cfg.lic || body.naverLic || process.env.NAVER_ACCESS_LICENSE);
  const sec = toStr(cfg.sec || body.naverSec || process.env.NAVER_SECRET_KEY);
  if(!cid || !lic || !sec) return {skipped:true, reason:'네이버 API 설정 없음'};
  const start = isoToYmd(body.startDate), end = isoToYmd(body.endDate);
  const [prevStart, prevEnd] = prevRange(start,end);
  const recentStart = fmtYMD(addDays(parseYMD(end), -6));
  const campaignsRaw = arrFrom(await naverReq(cid,lic,sec,'GET','/ncc/campaigns')).filter(c => !['DELETED','PAUSED_BY_BUDGET'].includes(c.status));
  const campIds = campaignsRaw.map(c=>c.nccCampaignId||c.campaignId||c.id).filter(Boolean);
  if(!campIds.length) return {allCamps:[], allGroups:[], recentDays:[], creatives:[]};
  let adgroupsRaw=[];
  try{ adgroupsRaw = arrFrom(await naverReq(cid,lic,sec,'GET','/ncc/adgroups')).filter(g => !['DELETED'].includes(g.status)); } catch(e){}
  const groupIds = adgroupsRaw.map(g=>g.nccAdgroupId||g.adgroupId||g.id).filter(Boolean);
  const [curCampRows, prevCampRows, dailyRows, curGroupRows, prevGroupRows] = await Promise.all([
    naverFetchStats(cid,lic,sec,campIds,start,end,null),
    naverFetchStats(cid,lic,sec,campIds,prevStart,prevEnd,null),
    naverFetchDaily(cid,lic,sec,campIds,recentStart,end),
    groupIds.length ? naverFetchStats(cid,lic,sec,groupIds,start,end,null) : Promise.resolve([]),
    groupIds.length ? naverFetchStats(cid,lic,sec,groupIds,prevStart,prevEnd,null) : Promise.resolve([])
  ]);
  const curByCamp=naverAggById(curCampRows), prevByCamp=naverAggById(prevCampRows), curByGroup=naverAggById(curGroupRows), prevByGroup=naverAggById(prevGroupRows);
  const campNameById={}, campTypeById={}, campSaleById={};
  const allCamps = campaignsRaw.map(c=>{
    const rawId = c.nccCampaignId||c.campaignId||c.id;
    const name = c.name||c.campaignName||c.campNm||rawId;
    const type = /비판매/.test(name) ? '비판매' : /판매/.test(name) ? '판매' : naverTypeLabel(c.campaignTp||c.type);
    const saleType = inferSaleType({type,campaignName:name});
    campNameById[rawId]=name; campTypeById[rawId]=type; campSaleById[rawId]=saleType;
    return {platform:'naver', source:'naver', id:`naver:${rawId}`, rawId, campaignId:rawId, campaignName:name, status:c.status||c.userLock||c.campaignStatus, effectiveStatus:c.status||c.campaignStatus, type, saleType, ...metricWithPrev(curByCamp[rawId]||{}, prevByCamp[rawId]||{})};
  });
  const allGroups = adgroupsRaw.map(g=>{
    const rawId = g.nccAdgroupId||g.adgroupId||g.id;
    const campId = g.nccCampaignId||g.campaignId||g.campId;
    const name = g.name||g.adgroupName||g.groupNm||rawId;
    return {platform:'naver', source:'naver', id:`naver:${rawId}`, rawId, groupId:rawId, adgroupId:rawId, adgroupName:name, campaignId:campId, campaignKey:`naver:${campId}`, campaignName:campNameById[campId]||campId, status:g.status||g.userLock||g.adgroupStatus, effectiveStatus:g.status||g.adgroupStatus, type:campTypeById[campId]||'기타', saleType:campSaleById[campId]||'all', ...metricWithPrev(curByGroup[rawId]||{}, prevByGroup[rawId]||{})};
  });
  const recentDays = naverAggByDay(dailyRows).map(r=>({...r, source:'naver'}));
  return {allCamps, allGroups, recentDays, creatives:[]};
}

// ─────────────────────────────────────────────────────────────────────────────
// META
async function metaGraph(path, params, token){
  const url = new URL(`${META_BASE}/${META_VERSION}/${path.replace(/^\//,'')}`);
  for(const [k,v] of Object.entries(params||{})) if(v !== undefined && v !== null && v !== '') url.searchParams.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
  url.searchParams.set('access_token', token);
  const rows=[]; let next=url.toString();
  for(let guard=0; next && guard<30; guard++){
    const resp = await fetch(next);
    const json = await resp.json().catch(()=>({}));
    if(!resp.ok) throw new Error(`Meta API 오류 (${resp.status}) ${json?.error?.message || JSON.stringify(json).slice(0,300)}`);
    if(Array.isArray(json.data)) rows.push(...json.data); else if(json && !json.paging) return json;
    next = json?.paging?.next || '';
  }
  return rows;
}
function metaAccountPath(accountId){ const id=toStr(accountId).replace(/^act_/,''); return `act_${id}`; }
function cleanMetaAccountId(accountId){ return toStr(accountId).replace(/^act_/,''); }
function parseMetaAccountIds(value){
  if(Array.isArray(value)) return [...new Set(value.map(v=>cleanMetaAccountId(v)).filter(Boolean))];
  return [...new Set(String(value || '')
    .split(/[\n,;\s]+/)
    .map(v=>cleanMetaAccountId(v))
    .filter(Boolean))];
}
async function metaBusinessAccountIds(businessId, token){
  const bid = toStr(businessId).replace(/^business_/,'');
  if(!bid) return [];
  const fields = 'id,name,account_id';
  const [owned, client] = await Promise.all([
    metaGraph(`${bid}/owned_ad_accounts`, {fields, limit:500}, token).catch(()=>[]),
    metaGraph(`${bid}/client_ad_accounts`, {fields, limit:500}, token).catch(()=>[])
  ]);
  return [...new Set([...(owned||[]), ...(client||[])]
    .map(a => cleanMetaAccountId(a.account_id || a.id))
    .filter(Boolean))];
}
// Meta 전환 기준: 화면/요청에서 선택한 conversionBasis에 따라
// actions/action_values/catalog_segment_actions/catalog_segment_value 안에서
// 지정한 action_type만 합산합니다. 기본값은 실제 구매(purchase)입니다.
const META_CONVERSION_PRESETS = {
  purchase: {
    label:'구매',
    actionTypes:[
      'purchase','omni_purchase','offsite_conversion.fb_pixel_purchase','onsite_conversion.purchase',
      'app_custom_event.fb_mobile_purchase','offline_conversion.purchase'
    ],
    suffixes:['.purchase','.fb_pixel_purchase','.fb_mobile_purchase']
  },
  lead: {
    label:'리드',
    actionTypes:['lead','omni_lead','offsite_conversion.fb_pixel_lead','onsite_conversion.lead_grouped','onsite_conversion.lead','app_custom_event.fb_mobile_lead'],
    suffixes:['.lead','.fb_pixel_lead','.fb_mobile_lead']
  },
  add_to_cart: {
    label:'장바구니',
    actionTypes:['add_to_cart','omni_add_to_cart','offsite_conversion.fb_pixel_add_to_cart','onsite_conversion.add_to_cart','app_custom_event.fb_mobile_add_to_cart'],
    suffixes:['.add_to_cart','.fb_pixel_add_to_cart','.fb_mobile_add_to_cart']
  },
  initiate_checkout: {
    label:'체크아웃 시작',
    actionTypes:['initiate_checkout','omni_initiated_checkout','offsite_conversion.fb_pixel_initiate_checkout','onsite_conversion.initiate_checkout','app_custom_event.fb_mobile_initiated_checkout'],
    suffixes:['.initiate_checkout','.fb_pixel_initiate_checkout','.fb_mobile_initiated_checkout']
  },
  complete_registration: {
    label:'회원가입 완료',
    actionTypes:['complete_registration','omni_complete_registration','offsite_conversion.fb_pixel_complete_registration','app_custom_event.fb_mobile_complete_registration'],
    suffixes:['.complete_registration','.fb_pixel_complete_registration','.fb_mobile_complete_registration']
  },
  contact: {
    label:'문의',
    actionTypes:['contact','omni_contact','offsite_conversion.fb_pixel_contact'],
    suffixes:['.contact','.fb_pixel_contact']
  },
  subscribe: {
    label:'구독',
    actionTypes:['subscribe','omni_subscribe','offsite_conversion.fb_pixel_subscribe'],
    suffixes:['.subscribe','.fb_pixel_subscribe']
  }
};
function normalizeMetaActionType(v){
  return String(v || '').trim().toLowerCase();
}
function parseMetaActionTypes(value){
  if(Array.isArray(value)) return value.map(normalizeMetaActionType).filter(Boolean);
  return String(value || '')
    .split(/[\n,;]+/)
    .map(normalizeMetaActionType)
    .filter(Boolean);
}
function metaConversionConfig(body={}){
  const cfg = body.meta || {};
  const rawBasis = toStr(
    cfg.conversionBasis || cfg.conversionEvent ||
    body.metaConversionBasis || body.metaConversionEvent ||
    process.env.META_CONVERSION_BASIS || 'purchase'
  );
  const basis = normalizeMetaActionType(rawBasis || 'purchase');
  const customTypes = parseMetaActionTypes(
    cfg.conversionCustomTypes || cfg.customConversionTypes || cfg.conversionActionTypes ||
    body.metaConversionCustomTypes || body.metaCustomConversionTypes || body.metaConversionActionTypes ||
    process.env.META_CONVERSION_CUSTOM_TYPES
  );
  const preset = META_CONVERSION_PRESETS[basis] || null;
  const actionTypes = preset ? preset.actionTypes : (customTypes.length ? customTypes : parseMetaActionTypes(rawBasis));
  const safeActionTypes = actionTypes.length ? actionTypes : META_CONVERSION_PRESETS.purchase.actionTypes;
  return {
    basis: preset ? basis : (customTypes.length ? 'custom' : 'custom'),
    label: preset ? preset.label : '커스텀 전환',
    actionTypes: [...new Set(safeActionTypes.map(normalizeMetaActionType).filter(Boolean))],
    suffixes: preset ? (preset.suffixes || []) : []
  };
}
function isMetaSelectedActionType(actionType, config){
  const type = normalizeMetaActionType(actionType);
  if(!type) return false;
  const cfg = config || metaConversionConfig({});
  if((cfg.actionTypes || []).includes(type)) return true;
  return (cfg.suffixes || []).some(suffix => type.endsWith(suffix));
}
function metaActionValue(arr, config){
  if(!Array.isArray(arr)) return 0;
  let total = 0;
  for(const a of arr){
    const type = a?.action_type ?? a?.actionType;
    if(isMetaSelectedActionType(type, config)) total += n(a?.value);
  }
  return total;
}
function metaDirectKeys(prefix, config){
  const out=[];
  for(const type of (config?.actionTypes || [])){
    out.push(`${prefix}_${type}`);
    out.push(`${prefix}.${type}`);
    out.push(`${prefix}_${type.replace(/\./g,'_')}`);
  }
  return [...new Set(out)];
}
function metaDirectValue(row, keys){
  let total = 0;
  for(const k of keys){
    total += n(row?.[k]);
    if(k.includes('.')) total += n(row?.[k.replace(/\./g,'_')]);
  }
  return total;
}
function metaConversionParts(row, config){
  return {
    sharedCcnt: metaActionValue(row.catalog_segment_actions, config) + metaDirectValue(row, metaDirectKeys('catalog_segment_actions', config)),
    sharedConvAmt: metaActionValue(row.catalog_segment_value, config) + metaDirectValue(row, metaDirectKeys('catalog_segment_value', config)),
    normalCcnt: metaActionValue(row.actions, config),
    normalConvAmt: metaActionValue(row.action_values, config)
  };
}
function metaObjectiveLabel(value){
  const raw = String(value || '').trim();
  const s = raw.toUpperCase();
  const map = {
    OUTCOME_SALES:'판매 목표',
    OUTCOME_LEADS:'잠재 고객 목표',
    OUTCOME_TRAFFIC:'트래픽 목표',
    OUTCOME_ENGAGEMENT:'참여 목표',
    OUTCOME_AWARENESS:'인지도 목표',
    OUTCOME_APP_PROMOTION:'앱 홍보 목표',
    PRODUCT_CATALOG_SALES:'카탈로그 판매',
    CONVERSIONS:'전환',
    LINK_CLICKS:'트래픽',
    REACH:'도달',
    BRAND_AWARENESS:'브랜드 인지도',
    VIDEO_VIEWS:'동영상 조회',
    LEAD_GENERATION:'잠재 고객',
    MESSAGES:'메시지',
    POST_ENGAGEMENT:'게시물 참여',
    APP_INSTALLS:'앱 설치',
    EVENT_RESPONSES:'이벤트 응답',
    STORE_VISITS:'매장 방문'
  };
  return map[s] || raw || 'Meta 캠페인';
}
function metaPurchaseParts(row){
  // 일반 구매: actions = 구매수, action_values = 구매값
  // 협력/카탈로그 구매: catalog_segment_actions = 공유항목 구매수, catalog_segment_value = 공유항목 구매값
  const sharedPurchaseActionKeys = [
    'catalog_segment_actions_purchase',
    'catalog_segment_actions_omni_purchase',
    'catalog_segment_actions.offsite_conversion.fb_pixel_purchase',
    'catalog_segment_actions.onsite_conversion.purchase',
    'catalog_segment_actions.app_custom_event.fb_mobile_purchase',
    'catalog_segment_actions.offline_conversion.purchase'
  ];
  const sharedPurchaseValueKeys = [
    'catalog_segment_value_purchase',
    'catalog_segment_value_omni_purchase',
    'catalog_segment_value.offsite_conversion.fb_pixel_purchase',
    'catalog_segment_value.onsite_conversion.purchase',
    'catalog_segment_value.app_custom_event.fb_mobile_purchase',
    'catalog_segment_value.offline_conversion.purchase'
  ];
  return {
    sharedPurchaseCcnt: metaActionValue(row.catalog_segment_actions) + metaDirectValue(row, sharedPurchaseActionKeys),
    sharedPurchaseConvAmt: metaActionValue(row.catalog_segment_value) + metaDirectValue(row, sharedPurchaseValueKeys),
    normalPurchaseCcnt: metaActionValue(row.actions),
    normalPurchaseConvAmt: metaActionValue(row.action_values)
  };
}
function metaMetric(row, mode='auto', conversionConfig=metaConversionConfig({})){
  const p = metaConversionParts(row, conversionConfig);
  const hasSharedMetric = p.sharedCcnt > 0 || p.sharedConvAmt > 0;
  let selectedCcnt, selectedConvAmt, metricSource;

  // 메타 전환 기준이 '구매'인 경우에는 일반 구매(actions/action_values)와
  // 협력광고의 공유 구매(catalog_segment_actions/catalog_segment_value)를 합산합니다.
  // 리드/문의 등 비구매 기준은 동일 action_type 기준으로 normal + shared를 합산하되,
  // shared 값이 없으면 일반 actions만 사용됩니다.
  if(mode === 'normal'){
    selectedCcnt = p.normalCcnt;
    selectedConvAmt = p.normalConvAmt;
    metricSource = 'meta_standard_actions';
  }else if(mode === 'shared'){
    selectedCcnt = p.normalCcnt + p.sharedCcnt;
    selectedConvAmt = p.normalConvAmt + p.sharedConvAmt;
    metricSource = 'meta_shared_catalog_plus_actions';
  }else if(hasSharedMetric){
    selectedCcnt = p.normalCcnt + p.sharedCcnt;
    selectedConvAmt = p.normalConvAmt + p.sharedConvAmt;
    metricSource = 'meta_auto_catalog_plus_actions';
  }else{
    selectedCcnt = p.normalCcnt;
    selectedConvAmt = p.normalConvAmt;
    metricSource = 'meta_auto_standard_actions';
  }

  const out = calc({
    cost:row.spend,
    imp:row.impressions,
    click:row.clicks,
    conv:selectedCcnt,
    revenue:selectedConvAmt,
    purchaseCcnt:selectedCcnt,
    purchaseConvAmt:selectedConvAmt
  });
  out.selectedConversionCcnt = selectedCcnt;
  out.selectedConversionValue = selectedConvAmt;
  out.conversionBasis = conversionConfig.basis;
  out.conversionBasisLabel = conversionConfig.label;
  out.metricSource = metricSource;
  return out;
}
function metaMetricMode(accountId, body){
  const cfg = body.meta || {};
  const sharedIds = parseMetaAccountIds(
    cfg.collaborativeAccountIds || cfg.collabAccountIds || cfg.sharedAccountIds || cfg.catalogAccountIds ||
    body.metaCollaborativeAccountIds || body.metaCollabAccountIds || body.metaSharedAccountIds || body.metaCatalogAccountIds ||
    process.env.META_COLLABORATIVE_ACCOUNT_IDS || process.env.META_COLLAB_ACCOUNT_IDS || process.env.META_SHARED_ACCOUNT_IDS || process.env.META_CATALOG_ACCOUNT_IDS
  );
  const normalIds = parseMetaAccountIds(
    cfg.standardAccountIds || cfg.normalAccountIds || body.metaStandardAccountIds || body.metaNormalAccountIds ||
    process.env.META_STANDARD_ACCOUNT_IDS || process.env.META_NORMAL_ACCOUNT_IDS
  );
  const id = cleanMetaAccountId(accountId);
  if(sharedIds.includes(id)) return 'shared';
  if(normalIds.includes(id)) return 'normal';
  return 'auto';
}
function mapBy(rows, idKey, mode='auto', conversionConfig=metaConversionConfig({})){ const map={}; for(const r of rows||[]){ const id=r[idKey]; if(!id) continue; map[id]=metaMetric(r, mode, conversionConfig); } return map; }
async function metaInsights(account, token, level, startISO, endISO, extra={}){
  return metaGraph(`${metaAccountPath(account)}/insights`, {
    level,
    fields:'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,actions,action_values,catalog_segment_actions,catalog_segment_value,date_start,date_stop',
    time_range:{since:startISO, until:endISO},
    limit:500,
    ...extra
  }, token);
}
async function fetchMetaAccount(accountId, token, body){
  const metaAccountId = cleanMetaAccountId(accountId);
  const metricMode = metaMetricMode(metaAccountId, body);
  const conversionConfig = metaConversionConfig(body);
  const start = ymdToISO(body.startDate), end = ymdToISO(body.endDate);
  const [prevStartY, prevEndY] = prevRange(body.startDate, body.endDate);
  const prevStart = ymdToISO(prevStartY), prevEnd = ymdToISO(prevEndY);
  const recentStart = ymdToISO(fmtYMD(addDays(parseYMD(body.endDate), -6)));
  const [curCamp, prevCamp, curAdset, prevAdset, recent, ads, adInsights, metaCampaigns, metaAdsets] = await Promise.all([
    metaInsights(metaAccountId, token, 'campaign', start, end),
    metaInsights(metaAccountId, token, 'campaign', prevStart, prevEnd),
    metaInsights(metaAccountId, token, 'adset', start, end),
    metaInsights(metaAccountId, token, 'adset', prevStart, prevEnd),
    metaInsights(metaAccountId, token, 'account', recentStart, end, {time_increment:1}),
    metaGraph(`${metaAccountPath(metaAccountId)}/ads`, {fields:'id,name,campaign_id,adset_id,effective_status,status,configured_status,creative{id,name,thumbnail_url,image_url,image_hash,video_id,title,body,object_story_spec}', limit:500}, token).catch(()=>[]),
    metaInsights(metaAccountId, token, 'ad', start, end).catch(()=>[]),
    metaGraph(`${metaAccountPath(metaAccountId)}/campaigns`, {fields:'id,status,effective_status,configured_status,objective,buying_type', limit:500}, token).catch(()=>[]),
    metaGraph(`${metaAccountPath(metaAccountId)}/adsets`, {fields:'id,campaign_id,status,effective_status,configured_status', limit:500}, token).catch(()=>[])
  ]);
  const prevCampMap=mapBy(prevCamp,'campaign_id', metricMode, conversionConfig), prevAdsetMap=mapBy(prevAdset,'adset_id', metricMode, conversionConfig), adMetricMap=mapBy(adInsights,'ad_id', metricMode, conversionConfig);
  const metaCampStatus={}; for(const c of metaCampaigns||[]) metaCampStatus[c.id]={status:c.status,effectiveStatus:c.effective_status,configuredStatus:c.configured_status,objective:c.objective,objectiveLabel:metaObjectiveLabel(c.objective),buyingType:c.buying_type,type:metaObjectiveLabel(c.objective||c.buying_type||'Meta 캠페인')};
  const metaAdsetStatus={}; for(const a of metaAdsets||[]) metaAdsetStatus[a.id]={status:a.status,effectiveStatus:a.effective_status,configuredStatus:a.configured_status};
  const campSaleById={}, campNameById={};
  const allCamps = curCamp.map(r=>{
    const rawId=r.campaign_id; const name=r.campaign_name||rawId; const metaType=(metaCampStatus[rawId]?.type||metaCampStatus[rawId]?.objective||'Meta 캠페인'); const saleType=inferSaleType({type:metaType,campaignName:name});
    campSaleById[rawId]=saleType; campNameById[rawId]=name;
    return {platform:'meta', source:'meta', accountId:metaAccountId, id:`meta:${metaAccountId}:${rawId}`, rawId, campaignId:rawId, campaignName:name, ...(metaCampStatus[rawId]||{}), type:metaType, saleType, metricMode, conversionBasis:conversionConfig.basis, conversionBasisLabel:conversionConfig.label, ...metricWithPrev(metaMetric(r, metricMode, conversionConfig), prevCampMap[rawId]||{})};
  });
  const allGroups = curAdset.map(r=>{
    const gid=r.adset_id, cid=r.campaign_id; const name=r.adset_name||gid; const saleType=campSaleById[cid] || inferSaleType({campaignName:r.campaign_name || name});
    return {platform:'meta', source:'meta', accountId:metaAccountId, id:`meta:${metaAccountId}:${gid}`, rawId:gid, groupId:gid, adgroupId:gid, adgroupName:name, campaignId:cid, campaignKey:`meta:${metaAccountId}:${cid}`, campaignName:r.campaign_name||campNameById[cid]||cid, ...(metaAdsetStatus[gid]||{}), type:(metaCampStatus[cid]?.type||metaCampStatus[cid]?.objective||'Meta 캠페인'), saleType, metricMode, conversionBasis:conversionConfig.basis, conversionBasisLabel:conversionConfig.label, ...metricWithPrev(metaMetric(r, metricMode, conversionConfig), prevAdsetMap[gid]||{})};
  });
  const recentDays = recent.map(r=>({platform:'meta', source:'meta', accountId:metaAccountId, dt:isoToYmd(r.date_start), date:isoToYmd(r.date_start).slice(4,6)+'/'+isoToYmd(r.date_start).slice(6,8), saleType:'all', metricMode, ...metaMetric(r, metricMode, conversionConfig)}));
  const adInfo = {};
  for(const a of ads||[]) adInfo[a.id] = a;
  const creatives = (adInsights||[]).map(r=>{
    const a=adInfo[r.ad_id] || {};
    const cr=a.creative || {};
    const cid=r.campaign_id || a.campaign_id;
    const saleType=campSaleById[cid] || inferSaleType({campaignName:r.campaign_name || campNameById[cid]});
    return {platform:'meta', source:'meta', accountId:metaAccountId, id:`meta:${metaAccountId}:${r.ad_id}`, rawId:r.ad_id, creativeId: cr.id || r.ad_id, creativeName: cr.name || a.name || r.ad_name || r.ad_id, status:a.status,effectiveStatus:a.effective_status,configuredStatus:a.configured_status, creativeType: cr.video_id ? 'video' : 'image', thumbnailUrl: cr.thumbnail_url, imageUrl: cr.image_url, videoId: cr.video_id, campaignId:cid, campaignKey:`meta:${metaAccountId}:${cid}`, campaignName:r.campaign_name || campNameById[cid] || cid, adgroupId:r.adset_id || a.adset_id, type:(metaCampStatus[cid]?.type||metaCampStatus[cid]?.objective||'Meta 캠페인'), saleType, metricMode, conversionBasis:conversionConfig.basis, conversionBasisLabel:conversionConfig.label, ...metricWithPrev(metaMetric(r, metricMode, conversionConfig), {})};
  });
  return {allCamps, allGroups, recentDays, creatives};
}
async function fetchMeta(body){
  const cfg = body.meta || {};
  const token = toStr(cfg.token || body.metaToken || process.env.META_ACCESS_TOKEN);
  const businessId = toStr(cfg.businessId || body.metaBusinessId || process.env.META_BUSINESS_ID);
  const normalAccountIds = parseMetaAccountIds(cfg.accountIds || cfg.accountId || body.metaAccountIds || body.metaAccountId || process.env.META_AD_ACCOUNT_IDS || process.env.META_AD_ACCOUNT_ID);
  const collabAccountIds = parseMetaAccountIds(cfg.collaborativeAccountIds || cfg.collabAccountIds || cfg.sharedAccountIds || cfg.catalogAccountIds || body.metaCollaborativeAccountIds || body.metaCollabAccountIds || body.metaSharedAccountIds || body.metaCatalogAccountIds || process.env.META_COLLABORATIVE_ACCOUNT_IDS || process.env.META_COLLAB_ACCOUNT_IDS || process.env.META_SHARED_ACCOUNT_IDS || process.env.META_CATALOG_ACCOUNT_IDS);
  let accountIds = [...new Set([...normalAccountIds, ...collabAccountIds])];
  if(!accountIds.length && businessId && token) accountIds = await metaBusinessAccountIds(businessId, token);
  accountIds = [...new Set([...accountIds, ...collabAccountIds])];
  if(!accountIds.length || !token) return {skipped:true, reason:'Meta API 설정 없음'};
  const settled = await Promise.allSettled(accountIds.map(accountId => fetchMetaAccount(accountId, token, body)));
  const conversionConfig = metaConversionConfig(body);
  const merged = {allCamps:[], allGroups:[], recentDays:[], creatives:[], errors:[], metaAccounts:accountIds, metaCollaborativeAccounts:collabAccountIds, metaConversionBasis:conversionConfig.basis, metaConversionBasisLabel:conversionConfig.label, metaConversionActionTypes:conversionConfig.actionTypes};
  for(let i=0;i<settled.length;i++){
    const r = settled[i];
    const accountId = accountIds[i];
    if(r.status === 'fulfilled'){
      merged.allCamps.push(...(r.value.allCamps||[]));
      merged.allGroups.push(...(r.value.allGroups||[]));
      merged.recentDays.push(...(r.value.recentDays||[]));
      merged.creatives.push(...(r.value.creatives||[]));
    }else{
      merged.errors.push({platform:'meta', accountId, message:r.reason?.message || String(r.reason)});
    }
  }
  if(!merged.allCamps.length && merged.errors.length) throw new Error(`Meta 전체 광고계정 조회 실패: ${merged.errors.map(e=>`act_${e.accountId}: ${e.message}`).join(' / ')}`);
  return merged;
}

// ─────────────────────────────────────────────────────────────────────────────
// GOOGLE ADS
async function googleRefreshAccessToken(clientId, clientSecret, refreshToken){
  const params = new URLSearchParams({client_id:clientId, client_secret:clientSecret, refresh_token:refreshToken, grant_type:'refresh_token'});
  const resp = await fetch(GOOGLE_OAUTH_URL, {method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body:params.toString()});
  const data = await resp.json().catch(()=>({}));
  if(!resp.ok || !data.access_token) throw new Error(`Google Access Token 갱신 실패: ${data.error_description || data.error || resp.status}`);
  return data.access_token;
}
async function googleSearch(accessToken, devtok, mcc, cid, query){
  const cleanCid = String(cid).replace(/-/g,'');
  const headers = {'Content-Type':'application/json','Authorization':`Bearer ${accessToken}`};
  if(devtok) headers['developer-token']=devtok;
  if(mcc) headers['login-customer-id']=String(mcc).replace(/-/g,'');
  const resp = await fetch(`${GOOGLE_ADS_BASE}/customers/${cleanCid}/googleAds:searchStream`, {method:'POST', headers, body:JSON.stringify({query})});
  const text = await resp.text();
  if(!resp.ok){ let err={}; try{err=JSON.parse(text)}catch{}; throw new Error(`Google Ads API 오류 (${resp.status}) ${err?.error?.message || text.slice(0,300)}`); }
  const rows=[];
  try{
    const parsed=JSON.parse(text); const chunks=Array.isArray(parsed)?parsed:[parsed];
    for(const c of chunks) if(c.results) rows.push(...c.results);
  }catch{
    for(const line of text.trim().split('\n').filter(Boolean)){ try{ const p=JSON.parse(line); const chunks=Array.isArray(p)?p:[p]; for(const c of chunks) if(c.results) rows.push(...c.results); }catch{} }
  }
  return rows;
}
function googleMetrics(row){
  const m=row.metrics || {};
  const cost = n(m.costMicros ?? m.cost_micros) / 1000000;
  const conv = n(m.conversions);
  const revenue = n(m.conversionsValue ?? m.conversions_value);
  return calc({cost, imp:m.impressions, click:m.clicks, conv, revenue, purchaseCcnt:conv, purchaseConvAmt:revenue});
}
function googleMapBy(rows, path){ const map={}; for(const r of rows||[]){ const id=path(r); if(id) map[id]=googleMetrics(r); } return map; }
async function fetchGoogle(body){
  const cfg = body.google || {};
  const clientId = toStr(cfg.clientId || body.googleClientId || process.env.GOOGLE_CLIENT_ID);
  const clientSecret = toStr(cfg.clientSecret || body.googleClientSecret || process.env.GOOGLE_CLIENT_SECRET);
  const refreshTok = toStr(cfg.refreshTok || body.googleRefreshTok || process.env.GOOGLE_REFRESH_TOKEN);
  const cid = toStr(cfg.cid || body.googleCid || process.env.GOOGLE_CUSTOMER_ID);
  const devtok = toStr(cfg.devtok || body.googleDevtok || process.env.GOOGLE_DEVELOPER_TOKEN);
  const mcc = toStr(cfg.mcc || body.googleMcc || process.env.GOOGLE_MCC_ID);
  if(!clientId || !clientSecret || !refreshTok || !cid) return {skipped:true, reason:'Google Ads API 설정 없음'};
  const token = await googleRefreshAccessToken(clientId, clientSecret, refreshTok);
  const start = ymdToISO(body.startDate), end = ymdToISO(body.endDate);
  const [prevStartY, prevEndY] = prevRange(body.startDate, body.endDate);
  const prevStart = ymdToISO(prevStartY), prevEnd = ymdToISO(prevEndY);
  const recentStart = ymdToISO(fmtYMD(addDays(parseYMD(body.endDate), -6)));
  const campaignQuery = (s,e) => `
    SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE segments.date BETWEEN '${s}' AND '${e}' AND campaign.status != 'REMOVED'
  `.trim();
  const adgroupQuery = (s,e) => `
    SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type, ad_group.id, ad_group.name, ad_group.status, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
    FROM ad_group
    WHERE segments.date BETWEEN '${s}' AND '${e}' AND campaign.status != 'REMOVED' AND ad_group.status != 'REMOVED'
  `.trim();
  const dailyQuery = `
    SELECT segments.date, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE segments.date BETWEEN '${recentStart}' AND '${end}' AND campaign.status != 'REMOVED'
  `.trim();
  const creativeQuery = `
    SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type, ad_group.id, ad_group.name, ad_group.status, asset.id, asset.name, asset.type, asset.image_asset.full_size.url, asset.youtube_video_asset.youtube_video_id, asset.text_asset.text, metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
    FROM ad_group_ad_asset_view
    WHERE segments.date BETWEEN '${start}' AND '${end}'
  `.trim();
  const [curCamp, prevCamp, curGroup, prevGroup, dailyRows, creativeRows] = await Promise.all([
    googleSearch(token, devtok, mcc, cid, campaignQuery(start,end)),
    googleSearch(token, devtok, mcc, cid, campaignQuery(prevStart,prevEnd)),
    googleSearch(token, devtok, mcc, cid, adgroupQuery(start,end)),
    googleSearch(token, devtok, mcc, cid, adgroupQuery(prevStart,prevEnd)),
    googleSearch(token, devtok, mcc, cid, dailyQuery).catch(()=>[]),
    googleSearch(token, devtok, mcc, cid, creativeQuery).catch(()=>[])
  ]);
  const prevCampMap=googleMapBy(prevCamp, r=>r.campaign?.id), prevGroupMap=googleMapBy(prevGroup, r=>r.adGroup?.id || r.ad_group?.id);
  const campSaleById={}, campNameById={};
  const allCamps = curCamp.map(r=>{ const id=String(r.campaign?.id || ''); const name=r.campaign?.name || id; const saleType=inferSaleType({campaignName:name}); campSaleById[id]=saleType; campNameById[id]=name; return {platform:'google', source:'google', id:`google:${id}`, campaignId:id, campaignName:name, status:r.campaign?.status, effectiveStatus:r.campaign?.status, type:(r.campaign?.advertisingChannelType||r.campaign?.advertising_channel_type||r.campaign?.advertisingChannelSubType||r.campaign?.advertising_channel_sub_type||'Google 캠페인'), saleType, ...metricWithPrev(googleMetrics(r), prevCampMap[id]||{})}; });
  const allGroups = curGroup.map(r=>{ const gid=String(r.adGroup?.id || r.ad_group?.id || ''); const cid=String(r.campaign?.id || ''); const name=r.adGroup?.name || r.ad_group?.name || gid; const saleType=campSaleById[cid] || inferSaleType({campaignName:r.campaign?.name || name}); return {platform:'google', source:'google', id:`google:${gid}`, groupId:gid, adgroupId:gid, adgroupName:name, campaignId:cid, campaignKey:`google:${cid}`, campaignName:r.campaign?.name || campNameById[cid] || cid, status:r.adGroup?.status || r.ad_group?.status || r.campaign?.status, effectiveStatus:r.adGroup?.status || r.ad_group?.status || r.campaign?.status, type:(r.campaign?.advertisingChannelType||r.campaign?.advertising_channel_type||r.campaign?.advertisingChannelSubType||r.campaign?.advertising_channel_sub_type||'Google 캠페인'), saleType, ...metricWithPrev(googleMetrics(r), prevGroupMap[gid]||{})}; });
  const dailyMap={};
  for(const r of dailyRows){ const dt=isoToYmd(r.segments?.date); if(!dt) continue; if(!dailyMap[dt]) dailyMap[dt]={platform:'google', source:'google', dt, date:`${dt.slice(4,6)}/${dt.slice(6,8)}`, saleType:'all', ...blank()}; addTo(dailyMap[dt], googleMetrics(r)); }
  const recentDays=Object.values(dailyMap).sort((a,b)=>a.dt.localeCompare(b.dt)).map(v=>({...v,...calc(v)}));
  const creatives = creativeRows.map(r=>{ const cid=String(r.campaign?.id || ''); const aid=String(r.asset?.id || ''); const saleType=campSaleById[cid] || inferSaleType({campaignName:r.campaign?.name}); return {platform:'google', source:'google', id:`google:${aid}`, creativeId:aid, creativeName:r.asset?.name || r.asset?.textAsset?.text || aid, creativeType:String(r.asset?.type || '').toLowerCase() || (r.asset?.imageAsset ? 'image' : 'asset'), imageUrl:r.asset?.imageAsset?.fullSize?.url || r.asset?.image_asset?.full_size?.url, thumbnailUrl:r.asset?.imageAsset?.fullSize?.url || r.asset?.image_asset?.full_size?.url, videoId:r.asset?.youtubeVideoAsset?.youtubeVideoId || r.asset?.youtube_video_asset?.youtube_video_id, campaignId:cid, campaignName:r.campaign?.name || campNameById[cid] || cid, adgroupId:String(r.adGroup?.id || r.ad_group?.id || ''), adgroupName:r.adGroup?.name || r.ad_group?.name || '', type:(r.campaign?.advertisingChannelType||r.campaign?.advertising_channel_type||r.campaign?.advertisingChannelSubType||r.campaign?.advertising_channel_sub_type||'Google 캠페인'), saleType, ...metricWithPrev(googleMetrics(r), {})}; });
  return {allCamps, allGroups, recentDays, creatives};
}

function mergePayload(parts){
  const out={allCamps:[], allGroups:[], recentDays:[], creatives:[], errors:[], skipped:[]};
  for(const p of parts){
    if(!p) continue;
    if(p.skipped){ out.skipped.push(p.reason); continue; }
    out.allCamps.push(...(p.allCamps||[]));
    out.allGroups.push(...(p.allGroups||[]));
    out.recentDays.push(...(p.recentDays||[]));
    out.creatives.push(...(p.creatives||[]));
    out.errors.push(...(p.errors||[]));
  }
  out.curAgg = aggregate(out.allCamps);
  out.prevAgg = aggregate(out.allCamps.map(c=>c._prev||{}));
  return out;
}

module.exports = async function handler(req,res){
  responseHeaders(res);
  if(req.method === 'OPTIONS') return res.status(200).end();
  if(req.method !== 'POST') return res.status(405).json({error:'Method not allowed'});
  const body = await parseBody(req);
  const startDate = isoToYmd(body.startDate), endDate = isoToYmd(body.endDate);
  if(!/^\d{8}$/.test(startDate) || !/^\d{8}$/.test(endDate)) return res.status(400).json({error:'startDate, endDate는 YYYYMMDD 또는 YYYY-MM-DD 형식이어야 합니다.'});
  body.startDate = startDate; body.endDate = endDate;
  const enabled = {
    naver: !!(toStr(body.naver?.cid || process.env.NAVER_CUSTOMER_ID) && toStr(body.naver?.lic || process.env.NAVER_ACCESS_LICENSE) && toStr(body.naver?.sec || process.env.NAVER_SECRET_KEY)),
    meta: !!(toStr(body.meta?.accountIds || body.meta?.accountId || body.meta?.collaborativeAccountIds || body.meta?.collabAccountIds || body.meta?.sharedAccountIds || body.meta?.catalogAccountIds || body.metaBusinessId || body.meta?.businessId || body.metaCollaborativeAccountIds || body.metaCollabAccountIds || body.metaSharedAccountIds || body.metaCatalogAccountIds || process.env.META_AD_ACCOUNT_IDS || process.env.META_AD_ACCOUNT_ID || process.env.META_COLLABORATIVE_ACCOUNT_IDS || process.env.META_COLLAB_ACCOUNT_IDS || process.env.META_SHARED_ACCOUNT_IDS || process.env.META_CATALOG_ACCOUNT_IDS || process.env.META_BUSINESS_ID) && toStr(body.meta?.token || process.env.META_ACCESS_TOKEN)),
    google: !!(toStr(body.google?.clientId || process.env.GOOGLE_CLIENT_ID) && toStr(body.google?.clientSecret || process.env.GOOGLE_CLIENT_SECRET) && toStr(body.google?.refreshTok || process.env.GOOGLE_REFRESH_TOKEN) && toStr(body.google?.cid || process.env.GOOGLE_CUSTOMER_ID))
  };
  if(!enabled.naver && !enabled.meta && !enabled.google) return res.status(400).json({error:'API 연결 정보가 없습니다. 화면의 API 설정 또는 Vercel 환경변수에 네이버/메타/구글 자격 정보를 입력하세요.'});
  const tasks = [];
  if(enabled.naver) tasks.push(fetchNaver(body).then(v=>({platform:'naver', value:v})).catch(e=>({platform:'naver', error:e})));
  if(enabled.meta) tasks.push(fetchMeta(body).then(v=>({platform:'meta', value:v})).catch(e=>({platform:'meta', error:e})));
  if(enabled.google) tasks.push(fetchGoogle(body).then(v=>({platform:'google', value:v})).catch(e=>({platform:'google', error:e})));
  const results = await Promise.all(tasks);
  const payload = mergePayload(results.map(r=>r.value));
  for(const r of results){ if(r.error) payload.errors.push({platform:r.platform, message:r.error.message}); }
  if(!payload.allCamps.length && payload.errors.length) return res.status(502).json({...payload, error:`연결된 API에서 데이터를 가져오지 못했습니다. ${joinErrors(payload.errors)}`});
  const metaConvCfg = metaConversionConfig(body);
  payload.meta = {startDate, endDate, generatedAt:new Date().toISOString(), enabled, metaConversionBasis:metaConvCfg.basis, metaConversionBasisLabel:metaConvCfg.label, metaConversionActionTypes:metaConvCfg.actionTypes};
  return res.json(payload);
};
