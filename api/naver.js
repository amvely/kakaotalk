// api/naver.js ─ Vercel Serverless Function for Naver SearchAd dashboard
const crypto = require('crypto');
const API_BASE = 'https://api.searchad.naver.com';

function makeSignature(secretKey, timestamp, method, path) {
  const message = `${timestamp}.${String(method).toUpperCase()}.${path}`;
  return crypto.createHmac('sha256', secretKey).update(message, 'utf8').digest('base64');
}
async function apiRequest(cid, lic, sec, method, pathWithQuery) {
  const m = String(method).toUpperCase();
  const pathOnly = pathWithQuery.split('?')[0];
  const timestamp = Date.now().toString();
  const sig = makeSignature(sec, timestamp, m, pathOnly);
  const r = await fetch(API_BASE + pathWithQuery, { method:m, headers:{ 'X-Timestamp':timestamp, 'X-API-KEY':lic, 'X-Customer':cid, 'X-Signature':sig }});
  const txt = await r.text();
  let data; try { data = txt ? JSON.parse(txt) : {}; } catch { data = txt; }
  return { status:r.status, data };
}
const ymdToISO = s => `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
const parseYMD = s => new Date(`${ymdToISO(String(s))}T00:00:00Z`);
const fmtYMD = d => d.toISOString().slice(0,10).replace(/-/g,'');
function addDays(d,n){ const x=new Date(d); x.setUTCDate(x.getUTCDate()+n); return x; }
function prevRange(startDate,endDate){ const s=parseYMD(startDate), e=parseYMD(endDate); const pe=addDays(s,-1); const span=Math.round((e-s)/86400000); const ps=addDays(pe,-span); return [fmtYMD(ps),fmtYMD(pe)]; }
function chunk(arr,n=200){ const out=[]; for(let i=0;i<arr.length;i+=n) out.push(arr.slice(i,i+n)); return out; }
function statsUrl(ids,start,end,timeIncrement){
  const idsQuery=ids.slice(0,200).map(id=>`ids=${encodeURIComponent(id)}`).join('&');
  const fields=encodeURIComponent(JSON.stringify(['impCnt','clkCnt','salesAmt','ccnt','convAmt','purchaseCcnt','purchaseConvAmt']));
  const timeRange=encodeURIComponent(JSON.stringify({since:ymdToISO(start),until:ymdToISO(end)}));
  let url=`/stats?${idsQuery}&fields=${fields}&timeRange=${timeRange}`;
  if(timeIncrement) url += `&timeIncrement=${timeIncrement}`;
  return url;
}
function arrFrom(data){ if(Array.isArray(data)) return data; if(Array.isArray(data?.data)) return data.data; if(Array.isArray(data?.items)) return data.items; if(Array.isArray(data?.campaigns)) return data.campaigns; if(Array.isArray(data?.adgroups)) return data.adgroups; return []; }
function flattenStats(resp){
  const base=arrFrom(resp?.data ?? resp); const out=[];
  for(const item of base){
    const parentId=item.id||item.nccCampaignId||item.campaignId||item.nccAdgroupId||item.adgroupId;
    const nested = Array.isArray(item.data) ? item.data : Array.isArray(item.rows) ? item.rows : Array.isArray(item.stats) ? item.stats : Array.isArray(item.items) ? item.items : null;
    if(nested){ for(const r of nested){ out.push({ ...r, id:r.id||parentId, statDate:r.statDate||r.date||r.period||item.statDate||item.date||item.period }); } }
    else out.push(item);
  }
  return out;
}
function getStat(row){ return row.stat || row; }
function rowId(row){ return row.id||row.nccCampaignId||row.campaignId||row.nccAdgroupId||row.adgroupId||row.nccAdgroupId; }
function rowDate(row){ const s=String(row.statDate||row.date||row.period||'').replace(/-/g,''); return s.length>=8?s.slice(0,8):''; }
function blank(){ return {cost:0,imp:0,click:0,conv:0,revenue:0,purchaseCcnt:0,purchaseConvAmt:0}; }
function addMetric(obj,row){ const s=getStat(row); obj.cost+=Number(s.salesAmt||s.cost||0); obj.imp+=Number(s.impCnt||s.imp||0); obj.click+=Number(s.clkCnt||s.click||0); obj.conv+=Number(s.ccnt||s.ctcCnt||s.convCnt||s.conv||0); obj.revenue+=Number(s.convAmt||s.revenue||0); obj.purchaseCcnt+=Number(s.purchaseCcnt||0); obj.purchaseConvAmt+=Number(s.purchaseConvAmt||0); }
function calc(v){ v={...blank(),...(v||{})}; const cart=Math.max(0,v.conv-v.purchaseCcnt); return {...v,cart,ctr:v.imp?v.click/v.imp*100:0,cvr:v.click?v.conv/v.click*100:0,roas:v.cost?v.revenue/v.cost*100:0,cpc:v.click?Math.round(v.cost/v.click):0,cpa:v.conv?Math.round(v.cost/v.conv):0,aov:v.conv?Math.round(v.revenue/v.conv):0}; }
function aggRows(rows){ const v=blank(); rows.forEach(r=>addMetric(v,r)); return calc(v); }
function aggById(rows){ const map={}; rows.forEach(r=>{ const id=rowId(r); if(!id) return; if(!map[id]) map[id]=blank(); addMetric(map[id],r); }); const out={}; Object.entries(map).forEach(([k,v])=>out[k]=calc(v)); return out; }
function aggByDay(rows){ const map={}; rows.forEach(r=>{ const d=rowDate(r); if(!d) return; if(!map[d]) map[d]=blank(); addMetric(map[d],r); }); return Object.entries(map).sort(([a],[b])=>a.localeCompare(b)).map(([dt,v])=>({...calc(v),dt,dateRaw:dt,date:`${dt.slice(4,6)}/${dt.slice(6,8)}`})); }
async function fetchStatsMany(cid,lic,sec,ids,start,end,timeIncrement){
  const rows=[]; for(const part of chunk(ids,200)){ if(!part.length) continue; const r=await apiRequest(cid,lic,sec,'GET',statsUrl(part,start,end,timeIncrement)); if(r.status===200) rows.push(...flattenStats(r)); else throw new Error(`통계 API 오류 (${r.status}) ${JSON.stringify(r.data).slice(0,300)}`); } return rows;
}

async function fetchStatsManyDaily(cid,lic,sec,ids,start,end){
  const rows=[];
  let d=parseYMD(start), endD=parseYMD(end);
  while(d<=endD){
    const y=fmtYMD(d);
    const dayRows=await fetchStatsMany(cid,lic,sec,ids,y,y,null).catch(()=>[]);
    dayRows.forEach(r=>{ if(!r.statDate && !r.date && !r.period) r.statDate=y; });
    rows.push(...dayRows);
    d=addDays(d,1);
  }
  return rows;
}

function mediaNameOf(row){
  const v = row.mediaNm || row.mediaName || row.media || row.mediaType || row.platform || row.device || row.network || row.channel || row.publisher || row.placement;
  return v ? String(v) : '네이버 검색광고';
}
function aggByMedia(rows){
  const map={};
  rows.forEach(r=>{ const k=mediaNameOf(r); if(!map[k]) map[k]=blank(); addMetric(map[k],r); });
  return Object.entries(map).map(([name,v])=>({name,media:name,...calc(v)})).sort((a,b)=>(b.cost||0)-(a.cost||0));
}

function campTypeLabel(tp){ const s=String(tp||''); return ({'1':'파워링크','2':'쇼핑광고','3':'파워컨텐츠','4':'브랜드검색광고','WEB_SITE':'파워링크','SHOPPING':'쇼핑광고','POWER_CONTENTS':'파워컨텐츠','BRAND_SEARCH':'브랜드검색광고','쇼핑검색':'쇼핑광고','브랜드검색':'브랜드검색광고'})[s] || s || '기타'; }

module.exports = async function handler(req,res){
  res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Methods','POST, OPTIONS'); res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(req.method==='OPTIONS') return res.status(200).end(); if(req.method!=='POST') return res.status(405).json({error:'Method not allowed'});
  let body=req.body; if(!body || typeof body==='string' || Object.keys(body||{}).length===0){ body=await new Promise(resolve=>{ let raw=''; req.on('data',c=>raw+=c); req.on('end',()=>{ try{resolve(JSON.parse(raw))}catch{resolve({})} }); }); }
  const cid=String(body?.cid||'').trim(), lic=String(body?.lic||'').trim(), sec=String(body?.sec||'').trim(), startDate=String(body?.startDate||'').trim(), endDate=String(body?.endDate||'').trim();
  if(!cid||!lic||!sec||!startDate||!endDate) return res.status(400).json({error:'필수값 누락 (cid, lic, sec, startDate, endDate)'});
  try{
    const [prevStart,prevEnd]=prevRange(startDate,endDate); const recentStart=fmtYMD(addDays(parseYMD(endDate),-20));
    const campRes=await apiRequest(cid,lic,sec,'GET','/ncc/campaigns'); if(campRes.status!==200) return res.status(campRes.status).json({error:`캠페인 API 오류 (${campRes.status})`,detail:campRes.data});
    const campaigns=arrFrom(campRes.data).filter(c=>!['DELETED','PAUSED_BY_BUDGET'].includes(c.status));
    const campIds=campaigns.map(c=>c.nccCampaignId||c.campaignId||c.id).filter(Boolean);
    if(!campIds.length) return res.json({curAgg:calc(),prevAgg:calc(),days:[],recentDays:[],recent21Days:[],allCamps:[],allGroups:[]});
    let adgroups=[]; try{ const gr=await apiRequest(cid,lic,sec,'GET','/ncc/adgroups'); if(gr.status===200) adgroups=arrFrom(gr.data).filter(g=>!['DELETED'].includes(g.status)); }catch(e){}
    const groupIds=adgroups.map(g=>g.nccAdgroupId||g.adgroupId||g.id).filter(Boolean);
    const [curCampRows,prevCampRows,recentRowsRaw,curGroupRows,prevGroupRows]=await Promise.all([
      fetchStatsMany(cid,lic,sec,campIds,startDate,endDate,null),
      fetchStatsMany(cid,lic,sec,campIds,prevStart,prevEnd,null),
      fetchStatsMany(cid,lic,sec,campIds,recentStart,endDate,1).catch(()=>[]),
      groupIds.length?fetchStatsMany(cid,lic,sec,groupIds,startDate,endDate,null):Promise.resolve([]),
      groupIds.length?fetchStatsMany(cid,lic,sec,groupIds,prevStart,prevEnd,null):Promise.resolve([]),
    ]);
    let recentRows = recentRowsRaw;
    // Some SearchAd accounts/endpoints do not return per-day rows with timeIncrement=1.
    // When that happens, build daily rows by requesting one day at a time so the dashboard
    // can still render 최근 7일간 성과 and 일간 지표 추이 from API data only.
    if(!aggByDay(recentRows).length){
      recentRows = await fetchStatsManyDaily(cid,lic,sec,campIds,recentStart,endDate);
    }
    const curByCamp=aggById(curCampRows), prevByCamp=aggById(prevCampRows), curByGroup=aggById(curGroupRows), prevByGroup=aggById(prevGroupRows);
    const campNameById={}, campTypeById={};
    const allCamps=campaigns.map(c=>{ const id=c.nccCampaignId||c.campaignId||c.id; const name=c.name||c.campaignName||c.campNm||id; const type=campTypeLabel(c.campaignTp||c.type); campNameById[id]=name; campTypeById[id]=type; return {id,nccCampaignId:id,campId:id,campaignId:id,campNm:name,name,campaignName:name,campTp:c.campaignTp||c.type,type,...(curByCamp[id]||calc()),_prev:(prevByCamp[id]||calc())}; });
    const allGroups=adgroups.map(g=>{ const id=g.nccAdgroupId||g.adgroupId||g.id; const campId=g.nccCampaignId||g.campaignId||g.campId; const groupNm=g.name||g.adgroupName||g.groupNm||id; return {id,nccAdgroupId:id,groupId:id,adgroupId:id,groupNm,name:groupNm,campId,campaignId:campId,nccCampaignId:campId,campNm:campNameById[campId]||campId,_campName:campNameById[campId]||campId,_type:campTypeById[campId]||'기타',type:campTypeById[campId]||'기타',...(curByGroup[id]||calc()),_prev:(prevByGroup[id]||calc())}; });
    const recent21Days=aggByDay(recentRows); const days=recent21Days.filter(d=>d.dt>=startDate && d.dt<=endDate); const recentDays=recent21Days.slice(-7);
    const allMedia=aggByMedia(curGroupRows.length ? curGroupRows : curCampRows);
    return res.json({curAgg:aggRows(curCampRows),prevAgg:aggRows(prevCampRows),days,recentDays,recent21Days,allCamps,allGroups,allMedia,prevStart,prevEnd});
  }catch(err){ console.error('[naver]',err); return res.status(500).json({error:err.message}); }
};
