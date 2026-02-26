<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>네이버광고 · 카카오톡 보고서</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
  --g:#03C75A; --g2:rgba(3,199,90,.10); --g3:rgba(3,199,90,.06);
  --b:#1EC6FF;  --y:#FFC800; --r:#FF4747; --p:#845AFF;
  --rad:12px; --rad2:9px;
}
[data-theme="dark"] {
  --bg:#08090c; --s1:#0f1117; --s2:#161b26; --s3:#1e2535;
  --border:#252d3d; --border2:#1a2030;
  --text:#e2e8f4; --text2:#94a3b8; --text3:#475569;
  --topbar-bg:rgba(8,9,12,.96);
  --header-bg:linear-gradient(135deg,#060d0e,#0a1a12 50%,#060d0e);
  --page-shadow:0 20px 70px rgba(0,0,0,.7);
  --tbl-hover:rgba(255,255,255,.018); --tbl-stripe:rgba(255,255,255,.015);
  --modal-bg:var(--s2);
  --up:#03C75A; --dn:#f87171;
}
[data-theme="light"] {
  --bg:#f0f4f2; --s1:#fff; --s2:#f5faf7; --s3:#ebf5f0;
  --border:#d1e0d9; --border2:#e2ede8;
  --text:#0d1f14; --text2:#3d6651; --text3:#89a898;
  --topbar-bg:rgba(255,255,255,.96);
  --header-bg:linear-gradient(135deg,#e8f9ef,#f0fdf6 50%,#e8f9ef);
  --page-shadow:0 8px 40px rgba(3,199,90,.08);
  --tbl-hover:rgba(3,199,90,.025); --tbl-stripe:rgba(3,199,90,.018);
  --modal-bg:#fff;
  --up:#029b46; --dn:#dc2626;
}

*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);color:var(--text);font-family:'Noto Sans KR',sans-serif;min-height:100vh;font-size:13px;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--s1);}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}

/* ══ SCREEN SELECT ══ */
#screen-select{min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:50px 20px 80px;}
.app-brand{display:flex;align-items:center;gap:9px;margin-bottom:40px;}
.brand-icon{width:38px;height:38px;background:var(--g);border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:18px;}
.brand-name{font-size:19px;font-weight:800;letter-spacing:-.04em;}
.brand-name span{color:var(--g);}
.sel-top{width:100%;max-width:720px;display:flex;align-items:center;gap:12px;margin-bottom:10px;}
.sel-heading{font-size:22px;font-weight:800;letter-spacing:-.04em;}
.sel-spacer{flex:1;}
.btn-add-adv{background:var(--g);border:none;color:#fff;padding:8px 16px;border-radius:9px;font-size:12px;font-weight:800;font-family:inherit;cursor:pointer;transition:opacity .15s;white-space:nowrap;}
.btn-add-adv:hover{opacity:.85;}
.sel-sub{font-size:12px;color:var(--text2);margin-bottom:28px;}
.search-wrap{width:100%;max-width:720px;position:relative;margin-bottom:20px;}
.search-icon{position:absolute;left:14px;top:50%;transform:translateY(-50%);color:var(--text3);}
#adv-search{width:100%;background:var(--s2);border:1.5px solid var(--border);color:var(--text);padding:12px 14px 12px 40px;border-radius:var(--rad);font-size:14px;font-family:inherit;outline:none;transition:border-color .2s;}
#adv-search:focus{border-color:var(--g);}
#adv-search::placeholder{color:var(--text3);}
.adv-grid{width:100%;max-width:720px;display:grid;grid-template-columns:repeat(3,1fr);gap:10px;}
@media(max-width:600px){.adv-grid{grid-template-columns:1fr 1fr;}}
.adv-card{background:var(--s2);border:1.5px solid var(--border);border-radius:var(--rad);padding:16px 18px;cursor:pointer;transition:all .15s;position:relative;overflow:hidden;}
.adv-card:hover{border-color:var(--g);transform:translateY(-2px);box-shadow:0 8px 28px rgba(3,199,90,.12);}
.adv-card.hidden{display:none;}
.adv-emoji{font-size:24px;margin-bottom:8px;display:block;}
.adv-name{font-size:14px;font-weight:700;margin-bottom:2px;}
.adv-cat{font-size:11px;color:var(--text2);}
.adv-actions{position:absolute;top:10px;right:10px;}
.adv-action-btn{background:var(--s3);border:1px solid var(--border);color:var(--text3);width:24px;height:24px;border-radius:6px;cursor:pointer;font-size:11px;display:flex;align-items:center;justify-content:center;transition:all .12s;}
.adv-action-btn:hover{color:var(--text);border-color:var(--text2);}

/* ══ TOPBAR (탭) ══ */
/* ══ PERIOD BAR ══ */
.period-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:space-between;
  max-width:1180px;margin:14px auto 0;padding:0 16px;}
.period-left{display:flex;flex-wrap:wrap;gap:6px;align-items:center;}
.period-btn{background:var(--s2);border:1.5px solid var(--border);color:var(--text2);
  padding:7px 10px;border-radius:9px;font-size:11px;font-weight:800;font-family:inherit;cursor:pointer;
  transition:all .13s;user-select:none;}
.period-btn:hover{border-color:var(--g);color:var(--text);}
.period-btn.active{background:var(--g2);border-color:rgba(3,199,90,.35);color:var(--g);}
.period-right{display:flex;flex-wrap:wrap;gap:6px;align-items:center;}
.period-date{background:var(--s2);border:1.5px solid var(--border);color:var(--text);
  padding:6px 9px;border-radius:9px;font-size:11px;font-family:'JetBrains Mono',monospace;outline:none;}
.period-apply{background:var(--g);border:none;color:#fff;padding:7px 12px;border-radius:9px;
  font-size:11px;font-weight:900;font-family:inherit;cursor:pointer;transition:opacity .13s;}
.period-apply:hover{opacity:.86;}
.period-sep{font-size:11px;color:var(--text3);font-family:'JetBrains Mono',monospace;}

#screen-report{display:none;}
.topbar{position:sticky;top:0;z-index:200;background:var(--topbar-bg);backdrop-filter:blur(16px);border-bottom:1px solid var(--border2);display:flex;align-items:stretch;height:50px;}
.topbar-left{display:flex;align-items:center;gap:10px;padding:0 16px;border-right:1px solid var(--border2);flex-shrink:0;}
.btn-back{background:none;border:1px solid var(--border);color:var(--text2);padding:4px 10px;border-radius:7px;cursor:pointer;font-family:inherit;font-size:11px;transition:all .13s;}
.btn-back:hover{color:var(--text);border-color:var(--text2);}
.topbar-adv{font-weight:700;font-size:13px;white-space:nowrap;}
.topbar-adv em{color:var(--g);font-style:normal;font-size:10px;margin-left:5px;}
.topbar-tabs{display:flex;align-items:stretch;flex:1;overflow-x:auto;}
.tab-btn{display:flex;align-items:center;gap:5px;padding:0 22px;font-size:12px;font-weight:600;color:var(--text3);cursor:pointer;border:none;border-bottom:2.5px solid transparent;background:none;font-family:inherit;white-space:nowrap;transition:all .14s;}
.tab-btn:hover{color:var(--text2);background:var(--s2);}
.tab-btn.active{color:var(--g);border-bottom-color:var(--g);font-weight:700;}
.topbar-right{display:flex;align-items:center;padding:0 14px;border-left:1px solid var(--border2);flex-shrink:0;}
.capture-btn{background:var(--g);border:none;color:#fff;padding:6px 14px;border-radius:7px;font-size:11px;font-weight:800;font-family:inherit;cursor:pointer;transition:opacity .13s;}
.capture-btn:hover{opacity:.86;}

/* ══ TAB PANELS ══ */
.tab-panel{display:none;}
.tab-panel.active{display:block;}

/* ══ REPORT PAGE ══ */
#tab-report{padding:20px 16px 60px;max-width:1210px;margin:0 auto;}
#report-page{background:var(--s1);border-radius:16px;border:1px solid var(--border2);overflow:hidden;box-shadow:var(--page-shadow);}
.rpt-header{background:var(--header-bg);padding:20px 24px 16px;border-bottom:1px solid var(--border2);position:relative;overflow:hidden;}
.rpt-header::after{content:'';position:absolute;top:-50px;right:-50px;width:200px;height:200px;background:radial-gradient(circle,rgba(3,199,90,.08),transparent 70%);pointer-events:none;}
.rh-row{display:flex;justify-content:space-between;align-items:flex-start;}
.rh-title{font-size:18px;font-weight:900;letter-spacing:-.04em;}
.rh-title em{color:var(--g);font-style:normal;}
.rh-badge{display:inline-flex;align-items:center;gap:4px;background:var(--g2);border:1px solid rgba(3,199,90,.2);color:var(--g);font-size:9px;font-weight:700;padding:2px 8px;border-radius:20px;margin-top:6px;letter-spacing:.06em;}
.rh-right{text-align:right;}
.rh-period{font-size:16px;font-weight:800;color:var(--g);font-family:'JetBrains Mono',monospace;}
.rh-date{font-size:10px;color:var(--text3);margin-top:2px;font-family:'JetBrains Mono',monospace;}
.rh-compare{font-size:9px;color:var(--text3);margin-top:1px;}
.rpt-body{padding:18px 20px 22px;}
.sec-label{font-size:9.5px;font-weight:700;color:var(--text3);letter-spacing:.12em;text-transform:uppercase;display:flex;align-items:center;gap:8px;margin-bottom:10px;margin-top:18px;}
.sec-label:first-child{margin-top:0;}
.sec-label::after{content:'';flex:1;height:1px;background:var(--border2);}
.sec-label-hint{font-weight:400;font-size:9px;letter-spacing:0;text-transform:none;}

/* Scorecards */
.scorecards{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;}
.sc{background:var(--s2);border:1px solid var(--border2);border-radius:var(--rad2);padding:11px 13px;position:relative;overflow:hidden;}
.sc::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:var(--sc-c,var(--border));}
.sc.cg{--sc-c:var(--g);}.sc.cb{--sc-c:var(--b);}.sc.cy{--sc-c:var(--y);}.sc.cr{--sc-c:var(--r);}
.sc-lbl{font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px;}
.sc-val{font-family:'JetBrains Mono',monospace;font-size:17px;font-weight:600;color:var(--text);letter-spacing:-.02em;line-height:1;margin-bottom:5px;}
.sc-cmp{display:flex;align-items:center;gap:5px;}
.sc-prev{font-family:'JetBrains Mono',monospace;font-size:9.5px;color:var(--text3);}
.sc-diff{font-family:'JetBrains Mono',monospace;font-size:9.5px;font-weight:700;}
.up{color:var(--up);}.dn{color:var(--dn);}


/* Scorecards v2 (image-like) */
.sc2{padding:12px 14px;}
.sc2-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:6px;}
.sc2-lbl{font-size:10px;font-weight:700;color:var(--text3);letter-spacing:.06em;text-transform:uppercase;}
.sc2-diff{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:900;line-height:1;}
.sc2-val{font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:700;color:var(--text);letter-spacing:-.02em;margin-bottom:4px;}
.sc2-sub{font-family:'JetBrains Mono',monospace;font-size:9.5px;color:var(--text3);}
/* Chart */
.chart-card{background:var(--s2);border:1px solid var(--border2);border-radius:var(--rad2);padding:14px 16px;}
.chart-inner{display:flex;gap:14px;align-items:stretch;}
.chart-canvas-wrap{flex:1;min-width:0;display:flex;flex-direction:column;}
#chart-canvas{width:100%;display:block;flex:1;}
.chart-metrics{flex-shrink:0;width:176px;display:flex;flex-direction:column;}
.cm-header{font-size:9px;color:var(--text3);font-weight:600;text-transform:uppercase;letter-spacing:.08em;padding-bottom:6px;border-bottom:1px solid var(--border2);margin-bottom:5px;}
.cm-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px;flex:1;}
.cm-btn{display:flex;align-items:center;gap:5px;padding:5px 7px;border-radius:6px;cursor:pointer;transition:background .12s;border:1px solid transparent;user-select:none;}
.cm-btn:hover{background:var(--s3);}
.cm-btn.sel1{background:rgba(3,199,90,.1);border-color:rgba(3,199,90,.25);}
.cm-btn.sel2{background:rgba(30,198,255,.1);border-color:rgba(30,198,255,.25);}
.cm-dot{width:8px;height:8px;border-radius:2px;flex-shrink:0;}
.cm-dot.d2{border-radius:50%;}
.cm-name{font-size:10px;color:var(--text2);white-space:nowrap;}
.cm-btn.sel1 .cm-name{color:var(--g);font-weight:700;}
.cm-btn.sel2 .cm-name{color:var(--b);font-weight:700;}
.cm-footer{font-size:9px;color:var(--text3);margin-top:5px;padding-top:5px;border-top:1px solid var(--border2);}

/* Tables */
.tables-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.tbl-card{background:var(--s2);border:1px solid var(--border2);border-radius:var(--rad2);overflow:hidden;}
.tbl-hd{padding:9px 13px;background:var(--s3);border-bottom:1px solid var(--border2);font-size:10.5px;font-weight:700;color:var(--text2);display:flex;align-items:center;justify-content:space-between;}
.tbl-hd small{font-size:9px;color:var(--text3);font-weight:400;}
.tbl-hd-actions{display:flex;align-items:center;gap:6px;}
.tbl-wrap{overflow-x:auto;}
table.dt{width:100%;border-collapse:collapse;}
table.dt th{font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.05em;padding:6px 9px;background:var(--tbl-stripe);border-bottom:1px solid var(--border2);text-align:right;white-space:nowrap;}
table.dt th:first-child{text-align:left;}
table.dt td{font-family:'JetBrains Mono',monospace;font-size:10px;padding:6px 9px;border-bottom:1px solid var(--border2);color:var(--text2);text-align:right;white-space:nowrap;}
table.dt td:first-child{font-family:'Noto Sans KR',sans-serif;font-size:10.5px;font-weight:500;color:var(--text);text-align:left;}
table.dt tr:last-child td{border-bottom:none;}
table.dt tr:hover td{background:var(--tbl-hover);}
table.dt tr.row-hl td{background:rgba(3,199,90,.07);color:var(--g);font-weight:600;}
table.dt tr.row-hl td:first-child{color:var(--g);}
table.dt tr.avg-row td{background:rgba(255,200,0,.07);font-weight:600;color:var(--y);border-top:1px solid rgba(255,200,0,.2);border-bottom:none;}
table.dt tr.avg-row td:first-child{color:var(--y);}
#tbl-compare-body td.td-cur, #tbl-compare-body th.th-cur{background:rgba(3,199,90,.05);}

.cmp-up{color:var(--up)!important;font-weight:700;}
.cmp-dn{color:var(--dn)!important;font-weight:700;}

/* Camp/Group */
.camp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.camp-settings-btn{background:none;border:1px solid var(--border);color:var(--text3);padding:3px 9px;border-radius:6px;font-size:9px;cursor:pointer;font-family:inherit;transition:all .12s;}
.camp-settings-btn:hover{border-color:var(--g);color:var(--g);}

/* Conv KW */
.conv-kw-wrap{background:var(--s2);border:1px solid var(--border2);border-radius:var(--rad2);overflow:hidden;}
.conv-kw-hd{padding:9px 14px;background:var(--s3);border-bottom:1px solid var(--border2);font-size:10.5px;font-weight:700;color:var(--g);display:flex;align-items:center;gap:6px;}
.conv-kw-hd small{font-size:9px;color:var(--text3);font-weight:400;margin-left:auto;}

/* Spend/Click TOP */
.spend-click-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.sk-card{background:var(--s2);border:1px solid var(--border2);border-radius:var(--rad2);overflow:hidden;}
.sk-hd{padding:9px 14px;background:var(--s3);border-bottom:1px solid var(--border2);font-size:10.5px;font-weight:700;display:flex;align-items:center;gap:5px;}
.sk-hd.spend-hd{color:var(--y);}.sk-hd.click-hd{color:var(--b);}
.sk-hd small{font-size:9px;color:var(--text3);font-weight:400;margin-left:auto;}
.sk-table{width:100%;border-collapse:collapse;}
.sk-table th{font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.04em;padding:5px 10px;background:var(--tbl-stripe);border-bottom:1px solid var(--border2);text-align:right;white-space:nowrap;}
.sk-table th:first-child,.sk-table th:nth-child(2){text-align:left;}
.sk-table td{font-family:'JetBrains Mono',monospace;font-size:10px;padding:5px 10px;border-bottom:1px solid var(--border2);color:var(--text2);text-align:right;white-space:nowrap;}
.sk-table td:nth-child(2){font-family:'Noto Sans KR',sans-serif;font-size:10.5px;color:var(--text);text-align:left;max-width:130px;overflow:hidden;text-overflow:ellipsis;}
.sk-table td:first-child{text-align:center;font-size:9.5px;}
.sk-table tr:last-child td{border-bottom:none;}
.sk-table tr:hover td{background:var(--tbl-hover);}
.sk-val-spend{color:var(--y)!important;font-weight:600;}
.sk-val-click{color:var(--b)!important;font-weight:600;}
.rank-top{color:var(--y)!important;font-weight:800;}
.rank-top-b{color:var(--b)!important;font-weight:800;}

/* Report footer */
.rpt-footer{padding:12px 24px;border-top:1px solid var(--border2);background:var(--s2);display:flex;justify-content:space-between;align-items:center;}
.rf-brand{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:.07em;}
.rf-brand em{color:var(--g);font-style:normal;}
.rf-ts{font-size:9px;color:var(--text3);font-family:'JetBrains Mono',monospace;}

/* ══ KAKAO TAB ══ */
#tab-kakao{max-width:860px;margin:0 auto;padding:24px 16px 60px;display:none;}
#tab-kakao.active{display:grid;grid-template-columns:1.15fr 0.85fr;gap:24px;align-items:start;}
@media(max-width:800px){#tab-kakao.active{grid-template-columns:1fr;}}

/* 입력 패널 */
.kk-input-panel{background:var(--s1);border:1px solid var(--border2);border-radius:var(--rad);overflow:hidden;}
.kk-panel-hd{padding:11px 16px;background:var(--s3);border-bottom:1px solid var(--border2);font-size:11px;font-weight:700;color:var(--text2);display:flex;align-items:center;gap:6px;}
.kk-panel-body{padding:16px;display:flex;flex-direction:column;gap:14px;}
.kk-field{display:flex;flex-direction:column;gap:5px;}
.kk-label{font-size:9.5px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;}
.kk-label span{font-weight:400;text-transform:none;letter-spacing:0;font-size:9px;margin-left:4px;color:var(--text3);}
textarea.kk-inp, input.kk-inp{
  width:100%;padding:9px 12px;
  border:1px solid var(--border);border-radius:8px;
  background:var(--s2);color:var(--text);
  font-family:'Noto Sans KR',sans-serif;font-size:12px;line-height:1.65;
  resize:vertical;outline:none;transition:border-color .15s,background .15s;
}
input.kk-inp{resize:none;}
textarea.kk-inp:focus, input.kk-inp:focus{border-color:var(--g);background:var(--s1);}

/* 키워드 칩 */
.kw-chips{display:flex;flex-wrap:wrap;gap:5px;margin-top:3px;}
.kw-chip{background:var(--g3);border:1px solid rgba(3,199,90,.2);color:var(--g);border-radius:20px;padding:3px 10px;font-size:10.5px;font-weight:600;cursor:pointer;transition:all .12s;font-family:'Noto Sans KR',sans-serif;}
.kw-chip:hover{background:var(--g2);border-color:var(--g);}
.kw-chip.selected{background:var(--g);color:#fff;border-color:var(--g);}

/* 카카오 프리뷰 */
.kk-preview-panel{background:var(--s1);border:1px solid var(--border2);border-radius:var(--rad);overflow:hidden;position:sticky;top:58px;}
.kk-preview-hd{padding:10px 14px;background:#FEE500;font-size:11px;font-weight:800;color:#3C1E1E;display:flex;align-items:center;gap:6px;}
.chat-bg{background:#baccd9;padding:14px 12px;min-height:320px;}
.chat-row{display:flex;gap:8px;align-items:flex-start;}
.chat-avatar{width:36px;height:36px;background:#FEE500;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;}
.chat-sender{font-size:10px;font-weight:700;color:rgba(0,0,0,.5);margin-bottom:4px;}
.chat-bubble{background:#fff;border-radius:4px 14px 14px 14px;padding:12px 14px;max-width:92%;box-shadow:0 1px 4px rgba(0,0,0,.1);font-size:12px;line-height:1.8;white-space:pre-wrap;word-break:break-all;color:#1a1a1a;font-family:'Noto Sans KR',sans-serif;}
.btn-copy{width:100%;background:#FEE500;color:#3C1E1E;border:none;padding:13px;font-family:'Noto Sans KR',sans-serif;font-size:13px;font-weight:800;cursor:pointer;transition:opacity .15s;display:flex;align-items:center;justify-content:center;gap:7px;}
.btn-copy:hover{opacity:.88;}

/* ══ MODALS ══ */
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(5px);z-index:500;align-items:center;justify-content:center;padding:20px;}
.modal-bg.show{display:flex;}
.modal{background:var(--modal-bg);border:1px solid var(--border);border-radius:16px;padding:22px;width:100%;max-width:500px;box-shadow:0 24px 80px rgba(0,0,0,.3);max-height:90vh;overflow-y:auto;}
.modal-title{font-size:16px;font-weight:800;margin-bottom:4px;}
.modal-sub{font-size:11px;color:var(--text2);margin-bottom:16px;}
.sec-note{font-size:10.5px;background:var(--g3);border:1px solid rgba(3,199,90,.15);border-radius:8px;padding:8px 12px;margin-bottom:14px;color:var(--text2);line-height:1.5;}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}
.fg{margin-bottom:10px;}
.fg-label{font-size:10px;font-weight:700;color:var(--text2);margin-bottom:5px;display:block;text-transform:uppercase;letter-spacing:.04em;}
.fg-input{width:100%;background:var(--s3);border:1px solid var(--border);border-radius:8px;padding:8px 11px;color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;outline:none;transition:border-color .15s;}
.fg-input.name-inp{font-family:'Noto Sans KR',sans-serif;}
.fg-input:focus{border-color:var(--g);}
.fg-hint{font-size:9.5px;color:var(--text3);margin-top:4px;}
.emoji-row{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:4px;}
.emoji-opt{width:32px;height:32px;border:1.5px solid var(--border);border-radius:7px;background:var(--s3);cursor:pointer;font-size:16px;display:flex;align-items:center;justify-content:center;transition:all .12s;}
.emoji-opt:hover,.emoji-opt.sel{border-color:var(--g);background:var(--g2);}
.modal-btns{display:flex;justify-content:flex-end;gap:8px;margin-top:16px;}
.btn-cancel{background:none;border:1px solid var(--border);color:var(--text2);padding:8px 14px;border-radius:8px;cursor:pointer;font-family:inherit;font-size:12px;}
.btn-save{background:var(--g);border:none;color:#fff;padding:8px 18px;border-radius:8px;cursor:pointer;font-family:inherit;font-size:12px;font-weight:700;transition:opacity .15s;}
.btn-save:hover{opacity:.86;}
.btn-del{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.25);color:var(--r);padding:8px 14px;border-radius:8px;cursor:pointer;font-family:inherit;font-size:12px;margin-right:auto;}
.camp-modal-list{display:flex;flex-direction:column;gap:6px;max-height:300px;overflow-y:auto;margin-bottom:14px;}
.camp-item{display:flex;align-items:center;gap:10px;background:var(--s3);border:1px solid var(--border);border-radius:8px;padding:8px 12px;cursor:pointer;transition:all .13s;}
.camp-item:hover{border-color:var(--text2);}
.camp-item.selected{border-color:var(--g);background:var(--g2);}
.camp-item input[type=checkbox]{accent-color:var(--g);width:14px;height:14px;flex-shrink:0;cursor:pointer;}
.camp-item-name{font-size:12px;font-weight:500;flex:1;}
.camp-item-metric{font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--text3);}

/* Loading / Toast */
#loading{display:none;position:fixed;inset:0;background:rgba(8,9,12,.7);z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:12px;}
#loading.show{display:flex;}
.spin{width:32px;height:32px;border:2.5px solid var(--border);border-top-color:var(--g);border-radius:50%;animation:spin .7s linear infinite;}
@keyframes spin{to{transform:rotate(360deg);}}
#toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--g);color:#fff;padding:9px 20px;border-radius:50px;font-size:12px;font-weight:700;opacity:0;transition:all .3s;pointer-events:none;z-index:99999;white-space:nowrap;}
#toast.show{opacity:1;transform:translateX(-50%) translateY(0);}
</style>
</head>
<body data-theme="dark">

<!-- ═══ SCREEN 1: 광고주 선택 ═══ -->
<div id="screen-select">
  <div class="app-brand">
    <div class="brand-icon">📨</div>
    <div class="brand-name">카톡<span>보고</span></div>
  </div>
  <div class="sel-top">
    <div class="sel-heading">광고주 선택</div>
    <div class="sel-spacer"></div>
    <button class="btn-add-adv" onclick="openAdvModal(null)">+ 광고주 추가</button>
  </div>
  <div class="sel-sub">광고주를 선택하면 네이버광고 성과 데이터 기반 카카오톡 보고서를 자동 생성합니다</div>
  <div class="search-wrap">
    <span class="search-icon">🔍</span>
    <input type="text" id="adv-search" placeholder="광고주명으로 검색…" oninput="filterAdv(this.value)" autocomplete="off">
  </div>
  <div class="adv-grid" id="adv-grid"></div>
</div>

<!-- ═══ SCREEN 2: 리포트 ═══ -->
<div id="screen-report">

  <!-- 탑바 -->
  <div class="topbar">
    <div class="topbar-left">
      <button class="btn-back" onclick="goBack()">← 목록</button>
      <div class="topbar-adv" id="ctrl-adv-name"></div>
    </div>
    <div class="topbar-tabs">
      <button class="tab-btn active" onclick="switchTab('report',this)">📊 성과 리포트</button>
      <button class="tab-btn" onclick="switchTab('kakao',this)">💬 카카오톡 보고서</button>
    </div>
    <div class="topbar-right">
      <button class="capture-btn" onclick="captureReport()">📸 캡처</button>
    </div>
  </div>

  <!-- ─── 탭: 성과 리포트 ─── -->
  <div class="tab-panel active" id="tab-report">
    <!-- 기간 선택 -->
    <div class="period-bar" id="period-bar">
      <div class="period-left">
        <button class="period-btn" id="btn-p-d1" onclick="setReportPeriod('D1')">어제</button>
        <button class="period-btn" id="btn-p-d7" onclick="setReportPeriod('D7')">최근 7일</button>
        <button class="period-btn" id="btn-p-pm" onclick="setReportPeriod('PM')">전월</button>
        <button class="period-btn" id="btn-p-tm" onclick="setReportPeriod('TM')">당월(누계)</button>
      </div>
      <div class="period-right">
        <input class="period-date" id="custom-start" type="date">
        <span class="period-sep">~</span>
        <input class="period-date" id="custom-end" type="date">
        <button class="period-apply" onclick="applyReportCustomRange()">조회</button>
      </div>
    </div>

    <div id="report-wrap">
      <div id="report-page">
        <div class="rpt-header">
          <div class="rh-row">
            <div>
              <div class="rh-title" id="rh-title"></div>
              <div class="rh-badge">● 성과 요약 리포트</div>
            </div>
            <div class="rh-right">
              <div class="rh-period" id="rh-period"></div>
              <div class="rh-date" id="rh-date"></div>
              <div class="rh-compare" id="rh-compare"></div>
            </div>
          </div>
        </div>

        <div class="rpt-body">
          <div class="sec-label">핵심 성과 지표</div>
          <div class="scorecards" id="scorecards"></div>

          <div class="sec-label">추이 차트 <span class="sec-label-hint">(최대 2개 선택)</span></div>
          <div class="chart-card">
            <div class="chart-inner">
              <div class="chart-canvas-wrap">
                <canvas id="chart-canvas" height="190"></canvas>
              </div>
              <div class="chart-metrics">
                <div class="cm-header">지표 선택 (최대 2개)</div>
                <div class="cm-grid" id="cm-grid"></div>
                <div class="cm-footer">● 실선 &nbsp;◌ 점선</div>
              </div>
            </div>
          </div>

          <div class="sec-label">일별 광고 성과 / 항목별 증감 비교</div>
          <div class="tables-row">
            <div class="tbl-card">
              <div class="tbl-hd">📅 일별 광고 성과 <small>최근 8일</small></div>
              <div class="tbl-wrap">
                <table class="dt">
                  <thead><tr><th>날짜</th><th>광고비</th><th>클릭수</th><th>전환수</th><th>매출액</th><th>ROAS</th></tr></thead>
                  <tbody id="tbl-daily-body"></tbody>
                </table>
              </div>
            </div>
            <div class="tbl-card">
              <div class="tbl-hd">📊 항목별 증감 비교 <small id="cmp-header-label"></small></div>
              <div class="tbl-wrap">
                <table class="dt">
                  <thead><tr><th>지표</th><th id="th-prev">이전 기간</th><th id="th-cur">기준</th><th>증감</th><th>증감률</th></tr></thead>
                  <tbody id="tbl-compare-body"></tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="sec-label">주요 캠페인 / 광고그룹 성과</div>
          <div class="camp-grid">
            <div class="tbl-card">
              <div class="tbl-hd">🎯 캠페인
                <div class="tbl-hd-actions"><small>광고비 순</small><button class="camp-settings-btn" onclick="openCampModal('camp')">⚙ 선택</button></div>
              </div>
              <div class="tbl-wrap">
                <table class="dt">
                  <thead><tr><th>캠페인</th><th>광고비</th><th>클릭수</th><th>전환수</th><th>매출액</th><th>ROAS</th></tr></thead>
                  <tbody id="tbl-camp-body"></tbody>
                </table>
              </div>
            </div>
            <div class="tbl-card">
              <div class="tbl-hd">📦 광고그룹
                <div class="tbl-hd-actions"><small>광고비 순</small><button class="camp-settings-btn" onclick="openCampModal('group')">⚙ 선택</button></div>
              </div>
              <div class="tbl-wrap">
                <table class="dt">
                  <thead><tr><th>광고그룹</th><th>광고비</th><th>클릭수</th><th>전환수</th><th>매출액</th><th>ROAS</th></tr></thead>
                  <tbody id="tbl-group-body"></tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="sec-label">전환 키워드 <span class="sec-label-hint" id="conv-total-lbl"></span></div>
          <div class="conv-kw-wrap">
            <div class="conv-kw-hd">🏆 전환 키워드 <small id="conv-count-lbl"></small></div>
            <div class="tbl-wrap">
              <table class="dt">
                <thead><tr><th style="text-align:left;width:20px">#</th><th style="text-align:left">키워드</th><th>이전 전환</th><th>전환수</th><th>비율</th><th>광고비</th><th>매출액</th><th>ROAS</th></tr></thead>
                <tbody id="conv-kw-tbody"></tbody>
              </table>
            </div>
          </div>

          
          <div class="sec-label">요일별 성과 비교 <span class="sec-label-hint">(금주 vs 지난주, 지표 선택)</span></div>
          <div class="chart-card">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
              <span style="font-size:10px;color:var(--text3);font-weight:700;letter-spacing:.08em;text-transform:uppercase">지표</span>
              <select id="week-metric" class="fg-input" style="max-width:220px" onchange="renderWeekCompare()">
                <option value="cost">광고비</option>
                <option value="imp">노출수</option>
                <option value="click">클릭수</option>
                <option value="conv">전환수</option>
                <option value="revenue">매출액</option>
                <option value="cpc">CPC</option>
                <option value="ctr">클릭율(CTR)</option>
                <option value="cvr">전환율(CVR)</option>
                <option value="roas">ROAS</option>
                <option value="cpa">전환당비용(CPA)</option>
                <option value="aov">객단가</option>
                <option value="rpc">클릭당매출액</option>
              </select>
              <span id="week-range-label" style="margin-left:auto;font-size:10px;color:var(--text3);font-family:'JetBrains Mono',monospace"></span>
            </div>
            <canvas id="week-chart-canvas" height="190"></canvas>
            <div style="display:flex;gap:10px;justify-content:flex-end;margin-top:6px;font-size:10px;color:var(--text3);font-family:'JetBrains Mono',monospace">
              <span>■ 금주</span><span>□ 지난주</span>
            </div>
          </div>
</div>
        </div>

        <div class="rpt-footer">
          <div class="rf-brand">NAVER SEARCHAD · <em>카톡보고 생성기</em></div>
          <div class="rf-ts" id="rf-ts"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- ─── 탭: 카카오톡 보고서 ─── -->
  
  <div class="tab-panel" id="tab-kakao">

    <!-- 좌측: 기간선택 + 보고서 내용 입력 (통합) -->
    <div class="kk-input-panel">
      <div class="kk-panel-hd">✍️ 카카오톡 보고서 설정 <span style="margin-left:auto;font-size:10px;color:var(--text3);font-weight:600" id="kakao-period-label"></span></div>
      <div class="kk-panel-body" style="gap:14px">

        <!-- 기간 선택 (카카오톡 보고서 전용) -->
        <div class="kk-field">
          <label class="kk-label">기간 선택 <span>(어제/7일/전월/당월 또는 직접 선택)</span></label>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="camp-settings-btn" id="btn-k-d1" onclick="setKakaoPeriod('D1')">어제</button>
            <button class="camp-settings-btn" id="btn-k-d7" onclick="setKakaoPeriod('D7')">최근 7일</button>
            <button class="camp-settings-btn" id="btn-k-pm" onclick="setKakaoPeriod('PM')">전월</button>
            <button class="camp-settings-btn" id="btn-k-tm" onclick="setKakaoPeriod('TM')">당월</button>
          </div>
          <div style="display:flex;gap:8px;margin-top:8px;align-items:center;flex-wrap:wrap">
            <input class="kk-inp" id="kakao-start" type="date" style="max-width:180px">
            <span style="color:var(--text3);font-size:11px">~</span>
            <input class="kk-inp" id="kakao-end" type="date" style="max-width:180px">
            <button class="btn-add-adv" style="padding:8px 14px" onclick="fetchKakaoCustom()">조회</button>
          </div>
        </div>

        <div class="kk-field" id="kw-chips-wrap" style="display:none">
          <label class="kk-label">💰 전환 키워드 <span>클릭하면 보고서에 포함</span></label>
          <div class="kw-chips" id="kw-chips"></div>
        </div>

        <div class="kk-field">
          <label class="kk-label">마케터 코멘트 <span>분석 인사이트</span></label>
          <textarea class="kk-inp" id="comment" rows="3" placeholder="성과에 대한 분석 내용을 입력하세요."></textarea>
        </div>

        <div class="kk-field">
          <label class="kk-label">금일 운영 계획</label>
          <textarea class="kk-inp" id="plan" rows="2" placeholder="오늘 진행할 최적화 작업을 입력하세요."></textarea>
        </div>

        <div class="kk-field">
          <label class="kk-label">하단 마무리 멘트 <span>(비워두면 기본값 사용)</span></label>
          <input class="kk-inp" id="msg-closing" type="text" placeholder="항상 최선을 다하겠습니다. 감사합니다! 🙏">
        </div>

      </div>
    </div>

    <!-- 우측: 미리보기 -->
    <div class="kk-preview-panel">
      <div class="kk-preview-hd">🟡 카카오톡 보고 미리보기</div>
      <div class="chat-bg">
        <div class="chat-row">
          <div class="chat-avatar">📊</div>
          <div>
            <div class="chat-sender">마케터</div>
            <div class="chat-bubble" id="kakao-preview">데이터를 불러오면 보고 문구가 자동으로 생성됩니다.</div>
          </div>
        </div>
      </div>
      <button class="btn-copy" onclick="copyKakao()">📋 카카오톡 문구 복사하기</button>
    </div>

  </div>


</div><!-- /screen-report -->

<!-- ═══ ADVERTISER MODAL ═══ -->
<div class="modal-bg" id="modal-adv">
  <div class="modal">
    <div class="modal-title" id="modal-adv-title">광고주 추가</div>
    <div class="modal-sub">네이버 광고 API 키를 입력하세요</div>
    <div class="sec-note">🛡 API 키는 브라우저 로컬스토리지에 암호화 저장됩니다.</div>
    <div style="margin-bottom:10px">
      <div class="fg-label">이모지 선택</div>
      <div class="emoji-row" id="emoji-row"></div>
    </div>
    <div class="form-row">
      <div class="fg">
        <label class="fg-label">광고주명 *</label>
        <input type="text" class="fg-input name-inp" id="inp-name" placeholder="예: 스타일마켓">
      </div>
      <div class="fg">
        <label class="fg-label">업종/카테고리</label>
        <input type="text" class="fg-input name-inp" id="inp-cat" placeholder="예: 패션/의류">
      </div>
    </div>
    <div class="fg">
      <label class="fg-label">Customer ID *</label>
      <input type="text" class="fg-input" id="inp-cid" placeholder="네이버 광고 고객 ID">
      <div class="fg-hint">네이버 검색광고 계정 → 고객센터 → Customer ID</div>
    </div>
    <div class="fg">
      <label class="fg-label">Access License *</label>
      <input type="text" class="fg-input" id="inp-lic" placeholder="API 접근 라이선스 키">
    </div>
    <div class="fg">
      <label class="fg-label">Secret Key *</label>
      <input type="password" class="fg-input" id="inp-sec" placeholder="비밀키">
      <div class="fg-hint">⚠ Secret Key는 저장 즉시 마스킹됩니다</div>
    </div>
    <div class="modal-btns">
      <button class="btn-del" id="btn-del-adv" onclick="deleteAdv()" style="display:none">삭제</button>
      <button class="btn-cancel" onclick="closeAdvModal()">취소</button>
      <button class="btn-save" onclick="saveAdv()">저장</button>
    </div>
  </div>
</div>

<!-- ═══ CAMPAIGN MODAL ═══ -->
<div class="modal-bg" id="modal-camp">
  <div class="modal" style="max-width:460px">
    <div class="modal-title" id="camp-modal-title">캠페인 선택</div>
    <div class="modal-sub" id="camp-modal-sub"></div>
    <div style="font-size:10px;color:var(--text3);margin-bottom:10px" id="camp-modal-hint"></div>
    <div class="camp-modal-list" id="camp-modal-list"></div>
    <div class="modal-btns">
      <button class="btn-cancel" onclick="closeCampModal()">취소</button>
      <button class="btn-save" onclick="saveCampSelection()">적용</button>
    </div>
  </div>
</div>

<div id="loading"><div class="spin"></div><div style="font-size:12px;color:var(--text2)">리포트 불러오는 중…</div></div>
<div id="toast"></div>

<script>
const _s=document.createElement('script');
_s.src='https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';
document.head.appendChild(_s);

// ══ STORAGE ══
const STORE_KEY='adreport_adv_v2';
function loadAdv(){try{const r=localStorage.getItem(STORE_KEY);return r?JSON.parse(decodeURIComponent(escape(atob(r)))):[]}catch{return[]}}
function saveAdv2(list){localStorage.setItem(STORE_KEY,btoa(unescape(encodeURIComponent(JSON.stringify(list)))))}
let advertisers=loadAdv();

// ══ STATE ══
let currentAdv=null;

// ✅ 성과리포트(탭1) 기간 상태
let reportPeriod='D1', reportData=null, reportPeriodMeta=null;
let reportCustomRange=null; // {start:Date,end:Date}

// ✅ 카카오톡 보고서(탭2) 기간 상태
let kakaoPeriod='D1', kakaoData=null, kakaoPeriodMeta=null;
let kakaoCustomRange=null; // {start:Date,end:Date}
let selectedCamps=null, selectedGroups=null, campModalType='camp', tempCampSel=[], filterZeroImp=true;
let brandExcludeTerms='', selectedKws=new Set();

const CHART_METRICS=[
  {key:'cost',   label:'광고비', color:'#03C75A'},
  {key:'imp',    label:'노출수', color:'#1EC6FF'},
  {key:'click',  label:'클릭수', color:'#FFC800'},
  {key:'conv',   label:'전환수', color:'#845AFF'},
  {key:'revenue',label:'매출액', color:'#FF6B35'},
  {key:'cpc',    label:'CPC',   color:'#FB923C'},
  {key:'ctr',    label:'CTR',   color:'#34D399'},
  {key:'cvr',    label:'CVR',   color:'#E879F9'},
  {key:'roas',   label:'ROAS',  color:'#FACC15'},
  {key:'cpa',    label:'전환당비용', color:'#F87171'},
];
let chartSel=['cost','conv'];

// ══ UTILS ══
function pad(n){return String(n).padStart(2,'0')}
function sum(arr,k){return arr.reduce((a,b)=>a+(b[k]||0),0)}
function addDays(d,n){const r=new Date(d);r.setDate(r.getDate()+n);return r}
function dateYMD(d){return String(d.getFullYear())+pad(d.getMonth()+1)+pad(d.getDate())}
function dispDate(d){const W=['일','월','화','수','목','금','토'];return d.getFullYear()+'.'+pad(d.getMonth()+1)+'.'+pad(d.getDate())+' ('+W[d.getDay()]+')'}
function dispRange(s,e){const f=d=>d.getFullYear()+'.'+pad(d.getMonth()+1)+'.'+pad(d.getDate());return f(s)+' ~ '+f(e)}
function shortRange(s,e){return pad(s.getMonth()+1)+'.'+pad(s.getDate())+'~'+pad(e.getMonth()+1)+'.'+pad(e.getDate())}
function showLoading(){document.getElementById('loading').classList.add('show')}
function hideLoading(){document.getElementById('loading').classList.remove('show')}
function showToast(msg){const t=document.getElementById('toast');t.textContent=msg;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),3500)}

function aggDays(days){
  if(!days||!days.length) return{cost:0,imp:0,click:0,conv:0,revenue:0,ctr:0,cvr:0,roas:0,cpc:0,cpa:0};
  const cost=sum(days,'cost'),imp=sum(days,'imp'),click=sum(days,'click'),conv=sum(days,'conv'),revenue=sum(days,'revenue');
  return{cost,imp,click,conv,revenue,
    ctr:imp?click/imp*100:0, cvr:click?conv/click*100:0,
    roas:cost?revenue/cost*100:0, cpc:click?Math.round(cost/click):0,
    cpa:conv?Math.round(cost/conv):0};
}

// ══ PERIOD ══

function getPeriodDates(period, range){
  const today=new Date(), yest=addDays(today,-1);
  let curS,curE,ps,pe,periodLabel,dateStr2,compareStr,curLabel,prevLabel;

  // 사용자 지정 기간
  if(period==='CUSTOM' && range?.start && customRange?.end){
    curS=new Date(range.start); curE=new Date(range.end);
    curS.setHours(0,0,0,0); curE.setHours(0,0,0,0);
    if(curE < curS){ const t=curS; curS=curE; curE=t; }

    // ✅ 이전 기간 = 선택 시작일 바로 전날부터 동일 기간 길이
    const span=Math.round((curE-curS)/86400000); // 0이면 1일
    pe=addDays(curS,-1);
    ps=addDays(pe,-span);

    periodLabel='기간 선택';
    dateStr2=dispRange(curS,curE);
    compareStr='이전 기간: '+dispRange(ps,pe);
    curLabel=shortRange(curS,curE);
    prevLabel=shortRange(ps,pe);

    return{curS,curE,ps,pe,periodLabel,dateStr2,compareStr,curLabel,prevLabel};
  }

  if(period==='D1'){
    curS=curE=yest; pe=addDays(curS,-1); ps=pe;
    periodLabel='어제 리포트'; dateStr2=dispDate(yest);
    compareStr='이전 기간: '+dispDate(pe);
    curLabel=pad(curS.getMonth()+1)+'.'+pad(curS.getDate());
    prevLabel=pad(ps.getMonth()+1)+'.'+pad(ps.getDate());

  } else if(period==='D7'){
    curE=yest; curS=addDays(yest,-6);
    // ✅ 이전 기간 = 바로 직전 7일
    pe=addDays(curS,-1); ps=addDays(pe,-6);

    periodLabel='최근 7일'; dateStr2=dispRange(curS,curE);
    compareStr='이전 기간: '+dispRange(ps,pe);
    curLabel=shortRange(curS,curE); prevLabel=shortRange(ps,pe);

  } else if(period==='PM'){
    const y=today.getMonth()===0?today.getFullYear()-1:today.getFullYear();
    const m=today.getMonth()===0?11:today.getMonth()-1;
    curS=new Date(y,m,1); curE=new Date(y,m+1,0);

    // ✅ 이전 기간 = 전전월(전월과 동일한 "월" 단위)
    const py=m===0?y-1:y; const pm2=m===0?11:m-1;
    ps=new Date(py,pm2,1); pe=new Date(py,pm2+1,0);

    periodLabel='전월 리포트'; dateStr2=dispRange(curS,curE);
    compareStr='이전 기간: '+dispRange(ps,pe);
    curLabel=(m+1)+'월'; prevLabel=(pm2+1)+'월';

  } else { // TM: 당월(누계)
    curS=new Date(today.getFullYear(),today.getMonth(),1);
    curE=yest;

    // ✅ 이전 기간 = 선택 시작일 바로 전날부터 동일 기간 길이
    pe=addDays(curS,-1);
    const span=Math.round((curE-curS)/86400000);
    ps=addDays(pe,-span);

    periodLabel='당월 리포트'; dateStr2=dispRange(curS,curE)+' (월 누계)';
    compareStr='이전 기간: '+dispRange(ps,pe);
    curLabel=(today.getMonth()+1)+'월누계'; prevLabel=shortRange(ps,pe);
  }
  return{curS,curE,ps,pe,periodLabel,dateStr2,compareStr,curLabel,prevLabel};
}

// ══ PERIOD CONTROLS ══
function syncReportPeriodButtons(){
  const ids=[['D1','btn-p-d1'],['D7','btn-p-d7'],['PM','btn-p-pm'],['TM','btn-p-tm']];
  ids.forEach(([p,id])=>{
    const el=document.getElementById(id);
    if(!el) return;
    el.classList.toggle('active', reportPeriod===p);
  });
  // CUSTOM이면 버튼 전체 비활성 느낌
  if(reportPeriod==='CUSTOM'){
    ids.forEach(([p,id])=>{
      const el=document.getElementById(id);
      if(el) el.classList.remove('active');
    });
  }
}

function setReportPeriod(p){
  reportPeriod=p;
  reportCustomRange=null;
  syncReportPeriodButtons();
  loadReportData();
}

function applyReportCustomRange(){
  const s=document.getElementById('custom-start')?.value;
  const e=document.getElementById('custom-end')?.value;
  if(!s||!e){ showToast('기간을 선택해주세요'); return; }
  const sd=new Date(s+'T00:00:00'); const ed=new Date(e+'T00:00:00');
  reportCustomRange={start:sd,end:ed};
  reportPeriod='CUSTOM';
  syncReportPeriodButtons();
  loadReportData();
}


// ══ KAKAO PERIOD CONTROLS (탭2 전용) ══
function syncKakaoPeriodButtons(){
  const ids=[['D1','btn-k-d1'],['D7','btn-k-d7'],['PM','btn-k-pm'],['TM','btn-k-tm']];
  ids.forEach(([p,id])=>{
    const el=document.getElementById(id);
    if(!el) return;
    el.classList.toggle('active', kakaoPeriod===p);
  });
  if(kakaoPeriod==='CUSTOM'){
    ids.forEach(([p,id])=>{
      const el=document.getElementById(id);
      if(el) el.classList.remove('active');
    });
  }
}

function setKakaoPeriod(p){
  kakaoPeriod=p;
  kakaoCustomRange=null;
  syncKakaoPeriodButtons();
  loadKakaoData();
}

function applyKakaoCustomRange(){
  const s=document.getElementById('kakao-start')?.value;
  const e=document.getElementById('kakao-end')?.value;
  if(!s||!e){ showToast('기간을 선택해주세요'); return; }
  const sd=new Date(s+'T00:00:00'); const ed=new Date(e+'T00:00:00');
  kakaoCustomRange={start:sd,end:ed};
  kakaoPeriod='CUSTOM';
  syncKakaoPeriodButtons();
  loadKakaoData();
}


// ══ TAB SWITCH ══
function switchTab(name, btn){
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');

  // ✅ 탭별로 기간/데이터를 독립적으로 관리
  if(name==='kakao'){
    syncKakaoPeriodButtons();
    // 아직 데이터가 없으면 기본(어제)로 최초 1회 로드
    if(currentAdv && !kakaoData){
      loadKakaoData();
      return;
    }
    if(kakaoData && kakaoPeriodMeta){
      renderKakaoFromData();
    }
  }
}


// ══ SCREEN SELECT ══
function renderAdvGrid(filter=''){
  const fl=filter.toLowerCase().trim();
  const grid=document.getElementById('adv-grid');
  if(!advertisers.length){
    grid.innerHTML=`<div style="grid-column:1/-1;text-align:center;padding:40px;color:var(--text3)">광고주가 없습니다. "+ 광고주 추가"를 눌러 추가하세요.</div>`;
    return;
  }
  grid.innerHTML=advertisers.map(a=>{
    const hide=fl&&!a.name.toLowerCase().includes(fl)&&!(a.cat||'').toLowerCase().includes(fl);
    return `<div class="adv-card ${hide?'hidden':''}" onclick="selectAdv('${a.id}')">
      <div class="adv-actions" onclick="event.stopPropagation()">
        <button class="adv-action-btn" onclick="openAdvModal('${a.id}')">✏</button>
      </div>
      <span class="adv-emoji">${a.emoji}</span>
      <div class="adv-name">${a.name}</div>
      <div class="adv-cat">${a.cat||''}</div>
    </div>`;
  }).join('');
}
function filterAdv(v){renderAdvGrid(v)}

async function selectAdv(id){
  currentAdv=advertisers.find(a=>a.id===id);
  if(!currentAdv) return;
  selectedCamps=null; selectedGroups=null; selectedKws.clear();
  document.getElementById('screen-select').style.display='none';
  document.getElementById('screen-report').style.display='block';
  document.getElementById('ctrl-adv-name').innerHTML=`${currentAdv.emoji} ${currentAdv.name}<em>${currentAdv.cat||''}</em>`;
  // 기본 기간: 어제
  reportPeriod='D1';
  // 날짜 입력 기본값 세팅
  const y=new Date();
  const ys=addDays(y,-1);
  const toISO=d=>d.toISOString().slice(0,10);
  const cs=document.getElementById('custom-start'); const ce=document.getElementById('custom-end');
  if(cs&&ce){ cs.value=toISO(ys); ce.value=toISO(ys); }
  syncReportPeriodButtons();
  await loadReportData();
}
function goBack(){
  document.getElementById('screen-select').style.display='flex';
  document.getElementById('screen-report').style.display='none';
}
function showEmptyState(msg){
  document.getElementById('report-page').innerHTML=`<div style="padding:80px;text-align:center;color:var(--text3)"><div style="font-size:40px;margin-bottom:16px">📭</div><div style="font-size:15px;font-weight:700;color:var(--text2);margin-bottom:8px">${msg}</div><div style="font-size:12px">API 키와 Customer ID를 확인하거나 다른 기간을 선택해주세요</div></div>`;
}

// ══ API ══
const API_BASE=window.location.origin;
async function fetchNaverData(adv,startDate,endDate){
  const resp=await fetch(API_BASE+'/api/naver',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({cid:adv.cid,lic:adv.lic,sec:adv.sec,startDate,endDate}),
  });
  if(!resp.ok){const e=await resp.json().catch(()=>({}));throw new Error(e.error||'API 오류('+resp.status+')');}
  return resp.json();
}

async function loadReportData(){
  showLoading();
  try{
    reportPeriodMeta=getPeriodDates(reportPeriod, reportCustomRange);
    const{curS,curE}=reportPeriodMeta;
    const data=await fetchNaverData(currentAdv,dateYMD(curS),dateYMD(curE));
    const hasData=(data.allCamps?.length>0)||(data.days?.length>0)||(data.recentDays?.length>0);
    if(hasData){
      reportData={
        days:data.days||[], recentDays:data.recentDays||[],
        prevAgg:data.prevAgg||{}, convKws:data.convKws||[],
        spendKws:data.spendKws||[], clickKws:data.clickKws||[],
        allCamps:data.allCamps||[], allGroups:data.allGroups||[],
      };
      renderReport();
    } else {
      showToast('⚠ 해당 기간에 광고 데이터가 없습니다');
      showEmptyState('해당 기간에 데이터가 없습니다');
    }
  } catch(e){
    console.error(e); showToast('❌ '+e.message); showEmptyState('API 오류: '+e.message);
  } finally{hideLoading();}
}


async function loadKakaoData(){
  if(!currentAdv) return;
  showLoading();
  try{
    kakaoPeriodMeta=getPeriodDates(kakaoPeriod, kakaoCustomRange);
    const{curS,curE,ps,pe,curLabel,prevLabel}=kakaoPeriodMeta;

    // 카카오 요구 포맷: [YYYY.MM.DD ~ YYYY.MM.DD]
    const f=d=>d.getFullYear()+'.'+pad(d.getMonth()+1)+'.'+pad(d.getDate());
    kakaoPeriodMeta.dateStr2 = `${f(curS)} ~ ${f(curE)}`;
    kakaoPeriodMeta.prevLabel = `${f(ps)} ~ ${f(pe)}`;
    kakaoPeriodMeta.curLabel  = `${f(curS)} ~ ${f(curE)}`;

    // 라벨 갱신(카카오 패널 상단)
    const lbl=document.getElementById('kakao-period-label');
    if(lbl) lbl.textContent = `[${kakaoPeriodMeta.dateStr2}]`;

    const data=await fetchNaverData(currentAdv,dateYMD(curS),dateYMD(curE));
    const hasData=(data.allCamps?.length>0)||(data.days?.length>0);
    if(!hasData){
      document.getElementById('kakao-preview').textContent='데이터가 없습니다. 기간을 변경해 주세요.';
      hideLoading(); return;
    }

    // 집계값
    const cur=aggDays(data.days);
    const prev=data.prevAgg?.cost!==undefined?data.prevAgg:aggDays([]);
    updateKakaoPreview(cur,prev);

    // 키워드 칩
    renderKakaoKwChips(data.convKws||[]);
  }catch(e){
    console.error(e);
    document.getElementById('kakao-preview').textContent='데이터를 불러오지 못했습니다: '+e.message;
  }finally{
    hideLoading();
  }
}



function renderKakaoKwChips(convKws){
  const wrap=document.getElementById('kw-chips-wrap');
  const box=document.getElementById('kw-chips');
  if(!wrap||!box) return;
  const list=(convKws||[]).filter(k=>Number(k.conv||0)>0);
  if(!list.length){
    wrap.style.display='none';
    box.innerHTML='';
    selectedKws.clear();
    return;
  }
  wrap.style.display='block';
  // 상위 12개만 칩으로 노출
  const top=list.slice(0,12);
  box.innerHTML=top.map(k=>{
    const name=(k.name||'').toString();
    const safe=name.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const sel=selectedKws.has(name);
    return `<span class="kw-chip ${sel?'selected':''}" onclick="toggleKakaoKwChip(${JSON.stringify(name)})">${safe}</span>`;
  }).join('');
}

function toggleKakaoKwChip(name){
  if(selectedKws.has(name)) selectedKws.delete(name);
  else selectedKws.add(name);
  // 칩 렌더 갱신
  const chips=document.querySelectorAll('#kw-chips .kw-chip');
  chips.forEach(ch=>{
    if(ch.textContent===name) ch.classList.toggle('selected', selectedKws.has(name));
  });
  // 미리보기 문구 재생성
  if(lastKakaoCur && lastKakaoPrev) updateKakaoPreview(lastKakaoCur,lastKakaoPrev);
}
function renderKakaoFromData(){
  if(!kakaoData || !currentAdv) return;

  const cur=aggDays(kakaoData.days);
  const prev=kakaoData.prevAgg?.cost!==undefined?kakaoData.prevAgg:{cost:0,imp:0,click:0,conv:0,revenue:0,ctr:0,cvr:0,roas:0,cpc:0,cpa:0};

  // ✅ 키워드 칩은 카카오 데이터 기준으로 렌더
  selectedKws.clear();
  const topKws=(kakaoData.convKws||[]).slice(0,15);
  const kwWrap=document.getElementById('kw-chips-wrap');
  if(topKws.length){
    kwWrap.style.display='flex';
    document.getElementById('kw-chips').innerHTML=topKws.map(k=>
      `<span class="kw-chip" onclick="toggleKwChip(this,'${k.name.replace(/'/g,"\\'")}')">${k.name} <small style="opacity:.6">${k.conv}건</small></span>`
    ).join('');
  } else {
    kwWrap.style.display='none';
  }

  updateKakaoPreview(cur,prev);
}

// ══ RENDER REPORT ══
function renderReport(){
  if(!reportData||!currentAdv) return;
  const{days,recentDays,recent21Days,prevAgg,convKws,convTerms,allCamps,allGroups}=reportData;
  const pm=reportPeriodMeta||getPeriodDates(reportPeriod, reportCustomRange);
  const{periodLabel,dateStr2,compareStr,curLabel,prevLabel}=pm;

  document.getElementById('rh-title').innerHTML=`${currentAdv.emoji} ${currentAdv.name} <em>${currentAdv.cat||''}</em>`;
  document.getElementById('rh-period').textContent=periodLabel;
  document.getElementById('rh-date').textContent=dateStr2;
  document.getElementById('rh-compare').textContent=compareStr;
  document.getElementById('th-prev').textContent=prevLabel;
  document.getElementById('th-cur').textContent=curLabel;
  document.getElementById('cmp-header-label').textContent=prevLabel+' → '+curLabel;

  const cur=aggDays(days);
  const prev=prevAgg?.cost!==undefined?prevAgg:{cost:0,imp:0,click:0,conv:0,revenue:0,ctr:0,cvr:0,roas:0,cpc:0,cpa:0,aov:0,rpc:0};

  renderScorecards(cur,prev);

  // 추이 차트: 최근 8일 고정 (없으면 days로)
  const chartDays=(recentDays&&recentDays.length>=2)?recentDays:days;
  renderChartMetrics(); drawChart(chartDays);

  // 일별 표: 최신일 상단 + 최근 7일 + 평균
  const{curE:hlDate}=pm;
  const hlStr=pad(hlDate.getMonth()+1)+'/'+pad(hlDate.getDate());
  renderDailyTable((recentDays&&recentDays.length>0)?recentDays:days, hlStr);

  // 항목별 증감 비교
  renderCompareTable(cur,prev,curLabel,prevLabel);

  // 캠페인/그룹: 0 성과 제외(그룹), 필터 적용
  const activeCamps=(allCamps||[]).filter(c=>filterZeroImp?c.imp>0:true);
  let activeGroups=(allGroups||[]).filter(g=>((g.cost||0)+(g.imp||0)+(g.click||0)+(g.conv||0)+(g.revenue||0))>0);
  activeGroups=activeGroups.filter(g=>filterZeroImp?g.imp>0:true);

  const camps=selectedCamps?activeCamps.filter(c=>selectedCamps.includes(c.id)):activeCamps.slice(0,10);
  const groups=selectedGroups?activeGroups.filter(g=>selectedGroups.includes(g.id)):activeGroups.slice(0,10);
  renderCampTable(camps); renderGroupTable(groups);

  // 전환 검색어(우선 convTerms), 없으면 기존 키워드(convKws)
  const convList=(convTerms&&convTerms.length)?convTerms:(convKws||[]);
  renderConvKw(convList||[]);

  // 요일별 비교 차트
  renderWeekCompare();
}


// ══ SCORECARDS ══

function renderScorecards(cur,prev){
  // 카드 포맷(첨부 이미지 스타일): 지표명 / 증감률(큰 텍스트) / 현재값 / 이전·현재 요약
  const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR');
  const N=n=>Math.round(n).toLocaleString('ko-KR');
  const F2=v=>Number(v||0).toFixed(2);
  const pct=(c,p)=> (p===0? (c===0?0:100) : ((c-p)/p*100));
  const diffTxt=(k,c,p)=>{
    if(['ctr','cvr','roas'].includes(k)){
      // %p
      return (F2(c-p))+'%p';
    }
    return (Math.round(pct(c,p)))+'%';
  };
  const valTxt=(k,v)=>{
    if(['cost','revenue'].includes(k)) return W(v);
    if(['cpc','cpa','aov','rpc'].includes(k)) return '₩'+Math.round(v).toLocaleString('ko-KR');
    if(['ctr','cvr'].includes(k)) return F2(v)+'%';
    if(k==='roas') return Math.round(v)+'%';
    return N(v);
  };
  const order=[
    {k:'cost',    t:'광고비'},
    {k:'imp',     t:'노출수'},
    {k:'click',   t:'클릭수'},
    {k:'conv',    t:'전환수'},
    {k:'revenue', t:'매출액'},
    {k:'cpc',     t:'CPC'},
    {k:'rpc',     t:'클릭당매출액'},
    {k:'aov',     t:'객단가'},
    {k:'ctr',     t:'클릭율'},
    {k:'cvr',     t:'전환율'},
    {k:'roas',    t:'ROAS'},
    {k:'cpa',     t:'전환당비용'},
  ];
  const wrap=document.getElementById('scorecards');
  if(!wrap) return;

  wrap.innerHTML=order.map((d,i)=>{
    const c=Number(cur[d.k]||0), p=Number(prev[d.k]||0);
    const up = (['ctr','cvr','roas'].includes(d.k)) ? (c-p)>=0 : (c-p)>=0;
    const cls = up ? 'up' : 'dn';
    const line = up ? 'var(--g)' : 'var(--r)';
    return `
      <div class="sc sc2" style="--sc-c:${line}">
        <div class="sc2-top">
          <div class="sc2-lbl">${d.t}</div>
          <div class="sc2-diff ${cls}">${diffTxt(d.k,c,p)}</div>
        </div>
        <div class="sc2-val">${valTxt(d.k,c)}</div>
        <div class="sc2-sub">이전 ${valTxt(d.k,p)} / 현재 ${valTxt(d.k,c)}</div>
      </div>
    `;
  }).join('');
}


// ══ CHART ══
function renderChartMetrics(){
  document.getElementById('cm-grid').innerHTML=CHART_METRICS.map(m=>{
    const idx=chartSel.indexOf(m.key);
    const cls=idx===0?'sel1':idx===1?'sel2':'';
    const dotStyle=idx>=0?`background:${m.color}`:`background:var(--border)`;
    return `<div class="cm-btn ${cls}" onclick="toggleChartMetric('${m.key}')">
      <div class="cm-dot ${idx===1?'d2':''}" style="${dotStyle}"></div>
      <div class="cm-name">${m.label}</div>
    </div>`;
  }).join('');
}
function toggleChartMetric(key){
  const idx=chartSel.indexOf(key);
  if(idx>=0) chartSel.splice(idx,1);
  else{if(chartSel.length>=2) chartSel.shift(); chartSel.push(key);}
  renderChartMetrics();
  if(reportData){const cd=reportData.recentDays?.length>=2?reportData.recentDays:reportData.days;drawChart(cd);}
}
function fmtAx(v,key){
  if(['ctr','cvr'].includes(key)) return v.toFixed(1)+'%';
  if(key==='roas') return Math.round(v)+'%';
  if(key==='cpc') return '₩'+Math.round(v/100)*100;
  if(v>=1000000) return (v/1000000).toFixed(1)+'M';
  if(v>=10000) return (v/10000).toFixed(0)+'만';
  if(v>=1000) return (v/1000).toFixed(0)+'K';
  return Math.round(v).toString();
}
function drawChart(chartDays){
  const days = Array.isArray(chartDays) ? chartDays : [];
  const canvas=document.getElementById('chart-canvas');
  const ctx=canvas.getContext('2d');
  const dpr=window.devicePixelRatio||1;
  const W0=canvas.parentElement.getBoundingClientRect().width;
  const H0=190;
  canvas.width=W0*dpr; canvas.height=H0*dpr;
  canvas.style.width=W0+'px'; canvas.style.height=H0+'px';
  ctx.scale(dpr,dpr);
  const yW=44;
  const PAD={t:22,r:chartSel.length>=2?yW:12,b:26,l:yW};
  const cW=W0-PAD.l-PAD.r,cH=H0-PAD.t-PAD.b,n=days.length;
  ctx.clearRect(0,0,W0,H0);
  if(n<2) return;
  const dark=document.body.getAttribute('data-theme')!=='light';
  const gridC=dark?'rgba(255,255,255,0.05)':'rgba(0,0,0,0.06)';
  const xLabelC=dark?'#475569':'#94a3b8';
  for(let i=0;i<=4;i++){const y=PAD.t+(cH/4)*i;ctx.strokeStyle=gridC;ctx.lineWidth=1;ctx.setLineDash([]);ctx.beginPath();ctx.moveTo(PAD.l,y);ctx.lineTo(PAD.l+cW,y);ctx.stroke();}
  chartSel.forEach((key,si)=>{
    const m=CHART_METRICS.find(x=>x.key===key);if(!m)return;
    const vals=days.map(d=>d[key]);
    const minV=Math.min(...vals),maxV=Math.max(...vals)||1,range=maxV-minV||1;
    const lo=minV-range*.1,hi=maxV+range*.1,span=hi-lo||1;
    const pts=vals.map((v,i)=>({x:PAD.l+i*(cW/(n-1)),y:PAD.t+cH-((v-lo)/span)*cH,v}));
    ctx.font='8.5px JetBrains Mono,monospace';
    for(let ti=0;ti<=5;ti++){
      const ratio=ti/5,tickVal=lo+(hi-lo)*ratio,yPos=PAD.t+cH-ratio*cH;
      if(si===0){ctx.fillStyle=m.color+'cc';ctx.textAlign='right';ctx.fillText(fmtAx(tickVal,key),PAD.l-4,yPos+3);ctx.strokeStyle=m.color+'44';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(PAD.l-1,yPos);ctx.lineTo(PAD.l+3,yPos);ctx.stroke();}
      else{ctx.fillStyle=m.color+'cc';ctx.textAlign='left';ctx.fillText(fmtAx(tickVal,key),PAD.l+cW+4,yPos+3);ctx.strokeStyle=m.color+'44';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(PAD.l+cW-3,yPos);ctx.lineTo(PAD.l+cW+1,yPos);ctx.stroke();}
    }
    ctx.font='bold 9px Noto Sans KR,sans-serif';
    if(si===0){ctx.fillStyle=m.color;ctx.textAlign='left';ctx.fillText('● '+m.label,PAD.l,PAD.t-8);}
    else{ctx.fillStyle=m.color;ctx.textAlign='right';ctx.fillText(m.label+' ●',PAD.l+cW,PAD.t-8);}
    if(si===0){const g=ctx.createLinearGradient(0,PAD.t,0,PAD.t+cH);g.addColorStop(0,m.color+'28');g.addColorStop(1,m.color+'02');ctx.beginPath();ctx.moveTo(pts[0].x,PAD.t+cH);pts.forEach(p=>ctx.lineTo(p.x,p.y));ctx.lineTo(pts[n-1].x,PAD.t+cH);ctx.closePath();ctx.fillStyle=g;ctx.fill();}
    ctx.beginPath();pts.forEach((p,i)=>i===0?ctx.moveTo(p.x,p.y):ctx.lineTo(p.x,p.y));
    ctx.strokeStyle=m.color;ctx.lineWidth=si===0?2:1.5;ctx.lineJoin='round';ctx.lineCap='round';ctx.setLineDash(si===0?[]:[4,3]);ctx.stroke();ctx.setLineDash([]);
    ctx.font='bold 8.5px JetBrains Mono,monospace';
    pts.forEach(p=>{
      ctx.beginPath();ctx.arc(p.x,p.y,si===0?3.5:3,0,Math.PI*2);ctx.fillStyle=m.color;ctx.fill();
      if(si===0){ctx.beginPath();ctx.arc(p.x,p.y,1.5,0,Math.PI*2);ctx.fillStyle=dark?'#0f1117':'#fff';ctx.fill();}
      const labelStr=fmtAx(p.v,key),tw=ctx.measureText(labelStr).width,labelY=p.y-7;
      const bg=dark?'rgba(15,17,23,0.75)':'rgba(255,255,255,0.82)';
      ctx.fillStyle=bg;ctx.textAlign='center';
      if(ctx.roundRect){ctx.beginPath();ctx.roundRect(p.x-tw/2-2,labelY-9,tw+4,11,3);ctx.fill();}
      ctx.fillStyle=m.color;ctx.fillText(labelStr,p.x,labelY);
    });
  });
  ctx.fillStyle=xLabelC;ctx.font='9px JetBrains Mono,monospace';ctx.textAlign='center';
  days.forEach((d,i)=>ctx.fillText(d.date,PAD.l+i*(cW/(n-1)),H0-4));
}

// ══ DAILY TABLE ══
function renderDailyTable(days,hlStr){
  const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR'),M=n=>Math.round(n).toLocaleString('ko-KR');

  // 최신일(보통 어제) 상단 → 그 아래 최근 7일 → 마지막은 평균
  const sorted=[...(days||[])].sort((a,b)=>String(b.dateRaw||'').localeCompare(String(a.dateRaw||'')));

  const rows=sorted.map(d=>{
    const isHL=d.date===hlStr;
    return `<tr class="${isHL?'row-hl':''}">
      <td>${d.date}</td>
      <td>${W(d.cost)}</td>
      <td>${M(d.click)}</td>
      <td>${M(d.conv)}</td>
      <td>${W(d.revenue)}</td>
      <td>${Math.round(d.roas)}%</td>
    </tr>`;
  }).join('');

  const n=sorted.length||1;
  const avg={
    cost: sorted.reduce((a,b)=>a+(b.cost||0),0)/n,
    click: sorted.reduce((a,b)=>a+(b.click||0),0)/n,
    conv: sorted.reduce((a,b)=>a+(b.conv||0),0)/n,
    revenue: sorted.reduce((a,b)=>a+(b.revenue||0),0)/n,
    roas: sorted.reduce((a,b)=>a+(b.roas||0),0)/n,
  };

  const avgRow = `<tr class="avg-row">
    <td>일 평균</td>
    <td>${W(avg.cost)}</td>
    <td>${M(avg.click)}</td>
    <td>${M(avg.conv)}</td>
    <td>${W(avg.revenue)}</td>
    <td>${Math.round(avg.roas)}%</td>
  </tr>`;

  document.getElementById('tbl-daily-body').innerHTML = rows + avgRow;
}


// ══ COMPARE TABLE ══
function renderCompareTable(cur,prev,curLabel,prevLabel){
  const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR'),M=n=>Math.round(n).toLocaleString('ko-KR');
  const P=v=>Number(v||0).toFixed(2)+'%';
  const R=v=>Math.round(v||0)+'%';

  const defs=[
    {label:'광고비',          c:cur.cost,    p:prev.cost,    fmt:W, isMoney:true},
    {label:'노출수',          c:cur.imp,     p:prev.imp,     fmt:M},
    {label:'클릭수',          c:cur.click,   p:prev.click,   fmt:M},
    {label:'전환수',          c:cur.conv,    p:prev.conv,    fmt:M},
    {label:'매출액',          c:cur.revenue, p:prev.revenue, fmt:W, isMoney:true},
    {label:'CPC',             c:cur.cpc,     p:prev.cpc,     fmt:W, isMoney:true},
    {label:'클릭당매출액',    c:cur.rpc,     p:prev.rpc,     fmt:W, isMoney:true},
    {label:'객단가',          c:cur.aov,     p:prev.aov,     fmt:W, isMoney:true},
    {label:'클릭율(CTR)',     c:cur.ctr,     p:prev.ctr,     fmt:P, isPoint:true},
    {label:'전환율(CVR)',     c:cur.cvr,     p:prev.cvr,     fmt:P, isPoint:true},
    {label:'ROAS',            c:cur.roas,    p:prev.roas,    fmt:R, isPoint:true},
    {label:'전환당비용(CPA)', c:cur.cpa,     p:prev.cpa,     fmt:W, isMoney:true},
  ];

  const pct=(c,p)=>p?((c-p)/p*100):0;
  const pctStr=(c,p)=>p?Math.abs(pct(c,p)).toFixed(1)+'%':'';
  const ptStr=(c,p,dec)=>p===0?'':Math.abs(c-p).toFixed(dec)+'%p';

  document.getElementById('tbl-compare-body').innerHTML=defs.map(d=>{
    const c=Number(d.c||0), p=Number(d.p||0);
    const diff=c-p;
    const isUp=diff>=0;
    const diffText=d.isPoint
      ? `${isUp?'▲':'▼'}${ptStr(c,p,d.label.includes('ROAS')?0:2)}`
      : `${isUp?'▲':'▼'}${pctStr(c,p)}`;
    const rateText=d.isPoint ? '' : pctStr(c,p);
    const absText=d.isPoint ? ptStr(c,p,d.label.includes('ROAS')?0:2) : pctStr(c,p);

    return `<tr>
      <td>${d.label}</td>
      <td>${d.fmt(p)}</td>
      <td class="td-cur">${d.fmt(c)}</td>
      <td class="${isUp?'cmp-up':'cmp-dn'}">${d.isPoint?((diff>=0?'+':'-')+absText):((diff>=0?'+':'-')+absText)}</td>
      <td class="${isUp?'cmp-up':'cmp-dn'}">${d.isPoint?diffText:diffText}</td>
    </tr>`;
  }).join('');
}


// ══ CAMP/GROUP TABLE ══
function renderCampTable(list){const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR'),M=n=>Math.round(n).toLocaleString('ko-KR');document.getElementById('tbl-camp-body').innerHTML=list.map(c=>`<tr><td>${c.name}</td><td>${W(c.cost)}</td><td>${M(c.click)}</td><td>${M(c.conv)}</td><td>${W(c.revenue)}</td><td>${c.roas}%</td></tr>`).join('');}
function renderGroupTable(list){const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR'),M=n=>Math.round(n).toLocaleString('ko-KR');document.getElementById('tbl-group-body').innerHTML=list.map(g=>`<tr><td>${g.name}</td><td>${W(g.cost)}</td><td>${M(g.click)}</td><td>${M(g.conv)}</td><td>${W(g.revenue)}</td><td>${g.roas}%</td></tr>`).join('');}

// ══ CAMP MODAL ══
function openCampModal(type){
  campModalType=type;
  const isCamp=type==='camp';
  let allList=isCamp?reportData.allCamps:reportData.allGroups;

  // 광고그룹: 모든 성과 0인 항목은 모달/필터에서 제외
  if(!isCamp){
    allList = (allList||[]).filter(g => (g.cost||0)+(g.imp||0)+(g.click||0)+(g.conv||0)+(g.revenue||0) > 0);
  }

  const curSel=isCamp?selectedCamps:selectedGroups;
  document.getElementById('camp-modal-title').textContent=isCamp?'캠페인 선택':'광고그룹 선택';
  document.getElementById('camp-modal-sub').textContent=`리포트에 표시할 ${isCamp?'캠페인':'광고그룹'}을 선택하세요`;
  document.getElementById('camp-modal-hint').textContent='선택하지 않으면 상위 10개가 자동으로 표시됩니다.';
  const visibleList=filterZeroImp?allList.filter(x=>x.imp>0):allList;
  tempCampSel=curSel?[...curSel]:visibleList.map(x=>x.id);
  const toggleHtml=`<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;padding:8px 12px;background:var(--s3);border-radius:8px">
    <label style="position:relative;width:32px;height:18px;cursor:pointer;flex-shrink:0">
      <input type="checkbox" ${filterZeroImp?'checked':''} onchange="toggleZeroImpFilter(this.checked)" style="opacity:0;position:absolute">
      <div style="position:absolute;inset:0;background:${filterZeroImp?'var(--g)':'var(--border)'};border-radius:20px;transition:background .25s"></div>
      <div style="position:absolute;top:2px;left:${filterZeroImp?'14':'2'}px;width:14px;height:14px;background:#fff;border-radius:50%;transition:left .25s;box-shadow:0 1px 3px rgba(0,0,0,.3)"></div>
    </label>
    <span style="font-size:11px;color:var(--text2)">노출 0 제외 <span style="color:var(--text3)">(${allList.length-visibleList.length}개 숨김)</span></span>
  </div>`;
  document.getElementById('camp-modal-list').innerHTML=toggleHtml+visibleList.map(item=>{
    const checked=tempCampSel.includes(item.id);
    return `<div class="camp-item ${checked?'selected':''}" onclick="toggleCampItem('${item.id}',this)">
      <input type="checkbox" ${checked?'checked':''} onclick="event.stopPropagation();toggleCampItem('${item.id}',this.closest('.camp-item'))">
      <div class="camp-item-name">${item.name}</div>
      <div class="camp-item-metric">₩${Math.round(item.cost).toLocaleString('ko-KR')} / ROAS ${item.roas}%</div>
    </div>`;
  }).join('');
  document.getElementById('modal-camp').classList.add('show');
}
function toggleZeroImpFilter(val){filterZeroImp=val;openCampModal(campModalType);}
function toggleCampItem(id,el){
  const idx=tempCampSel.indexOf(id);
  if(idx>=0){tempCampSel.splice(idx,1);el.classList.remove('selected');el.querySelector('input').checked=false;}
  else{tempCampSel.push(id);el.classList.add('selected');el.querySelector('input').checked=true;}
}
function closeCampModal(){document.getElementById('modal-camp').classList.remove('show');}
function saveCampSelection(){
  if(campModalType==='camp') selectedCamps=tempCampSel.length?[...tempCampSel]:null;
  else selectedGroups=tempCampSel.length?[...tempCampSel]:null;
  closeCampModal();
  const{allCamps,allGroups}=reportData;
  const activeCamps=allCamps.filter(c=>filterZeroImp?c.imp>0:true);
  const activeGroups=allGroups.filter(g=>filterZeroImp?g.imp>0:true);
  const camps=selectedCamps?activeCamps.filter(c=>selectedCamps.includes(c.id)):activeCamps.slice(0,10);
  const groups=selectedGroups?activeGroups.filter(g=>selectedGroups.includes(g.id)):activeGroups.slice(0,10);
  renderCampTable(camps);renderGroupTable(groups);
  showToast('✅ 적용되었습니다');
}

// ══ CONV KEYWORDS ══
function renderConvKw(list){
  const total=list.reduce((a,b)=>a+b.conv,0);
  document.getElementById('conv-total-lbl').textContent=`총 ${list.length}개 키워드`;
  document.getElementById('conv-count-lbl').textContent=`전체 ${total}건`;
  const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR'),M=n=>Math.round(n).toLocaleString('ko-KR');
  document.getElementById('conv-kw-tbody').innerHTML=list.length===0
    ?'<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text3)">전환 발생 키워드 없음</td></tr>'
    :list.map((kw,i)=>{
      const rank=i+1,pct=total?(kw.conv/total*100).toFixed(1):'0.0';
      const convDiff=kw.conv-kw.prevConv,convUp=convDiff>=0;
      return `<tr>
        <td style="text-align:right;font-family:'JetBrains Mono',monospace;font-size:9.5px;color:${rank<=3?'var(--g)':'var(--text3)'}">${rank}</td>
        <td style="font-size:11px;font-weight:500">${kw.name}</td>
        <td style="color:var(--text3)">${M(kw.prevConv)}</td>
        <td style="color:var(--g);font-weight:700">${M(kw.conv)} <span style="font-size:9px;color:${convUp?'var(--up)':'var(--dn)'}">${convUp?'▲':'▼'}${Math.abs(convDiff)}</span></td>
        <td style="color:var(--text3)">(${pct}%)</td>
        <td style="color:var(--y)">${W(kw.cost)}</td>
        <td style="color:var(--b)">${W(kw.revenue)}</td>
        <td style="color:${kw.roas>=300?'var(--g)':kw.roas>=150?'var(--y)':'var(--dn)'};font-weight:600">${kw.roas}%</td>
      </tr>`;
    }).join('');
}

// ══ BRAND FILTER ══
function applyBrandFilter(list){
  if(!brandExcludeTerms.trim()) return list;
  const terms=brandExcludeTerms.split(',').map(t=>t.trim().toLowerCase()).filter(Boolean);
  return list.filter(kw=>!terms.some(t=>kw.name.toLowerCase().includes(t)));
}
function setBrandExclude(val){ brandExcludeTerms=val; }

// ══ SPEND / CLICK TOP10 ══
function renderSpendTop(list){
  const W=n=>'₩'+Math.round(n).toLocaleString('ko-KR');
  const totalCost=list.reduce((a,b)=>a+b.cost,0);
  const orig=reportData?.spendKws?.length||0;
  const filt=orig-list.length;
  const cEl=document.getElementById('brand-filter-count');
  if(cEl) cEl.textContent=filt>0?`${filt}개 제외됨`:'';
  document.getElementById('spend-tbody').innerHTML=list.length===0
    ?'<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--text3)">키워드 데이터 없음</td></tr>'
    :list.map((kw,i)=>{
      const diff=kw.cost-kw.prevCost,pct=kw.prevCost?(diff/kw.prevCost*100):0,isUp=diff>=0;
      const sp=totalCost?(kw.cost/totalCost*100).toFixed(1):'0.0';
      return `<tr><td class="${i<3?'rank-top':''}">${i+1}</td><td>${kw.name}</td><td>${W(kw.prevCost)}</td><td class="sk-val-spend">${W(kw.cost)}</td><td class="${isUp?'cmp-up':'cmp-dn'}">${isUp?'▲':'▼'}${Math.abs(pct).toFixed(1)}%</td><td>${sp}%</td></tr>`;
    }).join('');
}
function renderClickTop(list){
  const M=n=>Math.round(n).toLocaleString('ko-KR');
  const totalClick=list.reduce((a,b)=>a+b.click,0);
  document.getElementById('click-tbody').innerHTML=list.length===0
    ?'<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--text3)">키워드 데이터 없음</td></tr>'
    :list.map((kw,i)=>{
      const diff=kw.click-kw.prevClick,pct=kw.prevClick?(diff/kw.prevClick*100):0,isUp=diff>=0;
      const sp=totalClick?(kw.click/totalClick*100).toFixed(1):'0.0';
      return `<tr><td class="${i<3?'rank-top-b':''}">${i+1}</td><td>${kw.name}</td><td>${M(kw.prevClick)}</td><td class="sk-val-click">${M(kw.click)}</td><td class="${isUp?'cmp-up':'cmp-dn'}">${isUp?'▲':'▼'}${Math.abs(pct).toFixed(1)}%</td><td>${sp}%</td></tr>`;
    }).join('');
}

// ══ 카카오 보고서 ══
function toggleKwChip(el,name){
  if(selectedKws.has(name)){selectedKws.delete(name);el.classList.remove('selected');}
  else{selectedKws.add(name);el.classList.add('selected');}
  if(reportData&&reportPeriodMeta){
    const cur=aggDays(reportData.days);
    const prev=reportData.prevAgg?.cost!==undefined?reportData.prevAgg:{cost:0,imp:0,click:0,conv:0,revenue:0,ctr:0,cvr:0,roas:0,cpc:0,cpa:0};
    updateKakaoPreview(cur,prev);
  }
}

function updateKakaoPreview(cur,prev){
  lastKakaoCur=cur; lastKakaoPrev=prev;
  const advName=currentAdv?.name||'업체명';
  const closing =document.getElementById('msg-closing').value.trim() ||'항상 최선을 다하겠습니다. 감사합니다! 🙏';
  const comment =document.getElementById('comment').value.trim()     ||'성과 분석 내용이 없습니다.';
  const plan    =document.getElementById('plan').value.trim()        ||'상시 모니터링 및 입찰가 최적화 진행 예정입니다.';

  const pm=kakaoPeriodMeta||getPeriodDates(kakaoPeriod, kakaoCustomRange);

  const W=n=>Math.round(Number(n||0)).toLocaleString('ko-KR');
  const pctChange=(c,p)=>{
    c=Number(c||0); p=Number(p||0);
    if(!p) return '';
    const pct=(c-p)/p*100;
    return ` (${pct>=0?'▲':'▼'}${Math.abs(pct).toFixed(1)}%)`;
  };
  const pointChange=(c,p,dec=2)=>{
    c=Number(c||0); p=Number(p||0);
    if(p===0) return '';
    const d=c-p;
    return ` (${d>=0?'▲':'▼'}${Math.abs(d).toFixed(dec)}%p)`;
  };

  let kwLine='';
  if(selectedKws.size){
    kwLine = `

■ 주요 전환 키워드
` + [...selectedKws].map((k,i)=>`${i+1}. ${k}`).join('\n');
  }

  const msg=
`[${pm.dateStr2}]
${advName} 네이버광고 성과 보고

■ 주요 성과 (${pm.prevLabel} → ${pm.curLabel})
- 광고비: ${W(cur.cost)}원${pctChange(cur.cost,prev.cost)}
- 노출수: ${W(cur.imp)}회${pctChange(cur.imp,prev.imp)}
- 클릭수: ${W(cur.click)}회${pctChange(cur.click,prev.click)}
- 전환수: ${W(cur.conv)}건${pctChange(cur.conv,prev.conv)}
- 매출액: ${W(cur.revenue)}원${pctChange(cur.revenue,prev.revenue)}
- CPC: ${W(cur.cpc)}원${pctChange(cur.cpc,prev.cpc)}
- 클릭율(CTR): ${Number(cur.ctr||0).toFixed(2)}%${pointChange(cur.ctr,prev.ctr,2)}
- 전환율(CVR): ${Number(cur.cvr||0).toFixed(2)}%${pointChange(cur.cvr,prev.cvr,2)}
- ROAS: ${Math.round(Number(cur.roas||0))}%${pointChange(cur.roas,prev.roas,0)}
- 전환당비용(CPA): ${W(cur.cpa)}원${pctChange(cur.cpa,prev.cpa)}
- 객단가: ${W(cur.aov)}원${pctChange(cur.aov,prev.aov)}
- 클릭당매출액: ${W(cur.rpc)}원${pctChange(cur.rpc,prev.rpc)}${kwLine}

■ 마케터 코멘트
${comment}

■ 금일 운영 계획
${plan}

${closing}`;

  document.getElementById('kakao-preview').textContent=msg;
}



function copyKakao(){
  const text=document.getElementById('kakao-preview').textContent;
  const btn=document.querySelector('.btn-copy');
  navigator.clipboard.writeText(text).then(()=>{
    btn.textContent='✅ 복사 완료!';
    setTimeout(()=>{btn.innerHTML='📋 카카오톡 문구 복사하기';},2200);
  }).catch(()=>{
    const ta=document.createElement('textarea');ta.value=text;document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);
    showToast('✅ 복사되었습니다');
  });
}

// 실시간 미리보기 갱신
['msg-closing','comment','plan'].forEach(id=>{
  document.getElementById(id).addEventListener('input',()=>{
    if(reportData&&reportPeriodMeta){
      const cur=aggDays(reportData.days);
      const prev=reportData.prevAgg?.cost!==undefined?reportData.prevAgg:{cost:0,imp:0,click:0,conv:0,revenue:0,ctr:0,cvr:0,roas:0,cpc:0,cpa:0};
      updateKakaoPreview(cur,prev);
    }
  });
});

// ══ ADVERTISER MODAL ══
const EMOJIS=['👗','🍕','💄','🏠','📱','🐾','🍀','🎵','🏋','🛒','🚗','🌿','🎮','📚','✈','🏥'];
let editingAdvId=null, selectedEmoji='👗';

function openAdvModal(id){
  editingAdvId=id; selectedEmoji=id?(advertisers.find(a=>a.id===id)?.emoji||'👗'):'👗';
  document.getElementById('modal-adv-title').textContent=id?'광고주 수정':'광고주 추가';
  document.getElementById('btn-del-adv').style.display=id?'block':'none';
  renderEmojiRow();
  if(id){
    const a=advertisers.find(a=>a.id===id);
    if(a){document.getElementById('inp-name').value=a.name;document.getElementById('inp-cat').value=a.cat||'';document.getElementById('inp-cid').value=a.cid;document.getElementById('inp-lic').value=a.lic;document.getElementById('inp-sec').value='';}
  } else {
    ['inp-name','inp-cat','inp-cid','inp-lic','inp-sec'].forEach(i=>document.getElementById(i).value='');
  }
  document.getElementById('modal-adv').classList.add('show');
}
function selectEmoji(e){selectedEmoji=e;renderEmojiRow();}
function renderEmojiRow(){document.getElementById('emoji-row').innerHTML=EMOJIS.map(e=>`<span class="emoji-opt ${e===selectedEmoji?'sel':''}" onclick="selectEmoji('${e}')">${e}</span>`).join('');}
function closeAdvModal(){document.getElementById('modal-adv').classList.remove('show');}

function saveAdv(){
  const name=document.getElementById('inp-name').value.trim();
  if(!name){showToast('⚠ 광고주명을 입력해주세요');return;}
  const cat=document.getElementById('inp-cat').value.trim();
  const cid=document.getElementById('inp-cid').value.trim();
  const lic=document.getElementById('inp-lic').value.trim();
  const sec=document.getElementById('inp-sec').value.trim();
  if(editingAdvId){
    const idx=advertisers.findIndex(a=>a.id===editingAdvId);
    if(idx>=0) advertisers[idx]={...advertisers[idx],emoji:selectedEmoji,name,cat,cid,lic,sec:sec||advertisers[idx].sec};
  } else {
    advertisers.push({id:'adv_'+Date.now(),emoji:selectedEmoji,name,cat,cid,lic,sec});
  }
  saveAdv2(advertisers);closeAdvModal();
  renderAdvGrid(document.getElementById('adv-search').value);
  showToast('✅ 저장되었습니다');
}
function deleteAdv(){
  if(!editingAdvId||!confirm('정말 삭제하시겠습니까?'))return;
  advertisers=advertisers.filter(a=>a.id!==editingAdvId);
  saveAdv2(advertisers);closeAdvModal();renderAdvGrid();
  showToast('🗑 삭제되었습니다');
}

// ══ CAPTURE ══
async function captureReport(){
  const btn=document.querySelector('.capture-btn');
  btn.textContent='⏳ 캡처 중…'; btn.disabled=true;
  try{
    const el=document.getElementById('report-page');
    const canvas=await html2canvas(el,{scale:2.5,backgroundColor:'#0f1117',useCORS:true,logging:false,scrollY:-window.scrollY});
    const link=document.createElement('a');
    const now=new Date();
    link.download=`NaverAD_${currentAdv?.name||'report'}_${now.getFullYear()}${pad(now.getMonth()+1)}${pad(now.getDate())}.png`;
    link.href=canvas.toDataURL('image/png'); link.click();
    canvas.toBlob(async blob=>{
      try{await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]);showToast('📸 저장+클립보드 복사!');}
      catch{showToast('📸 이미지 저장 완료!');}
    });
  } catch(e){showToast('⚠ 캡처 실패: '+e.message);}
  btn.textContent='📸 캡처'; btn.disabled=false;
}

// ══ INIT ══
window.onload=()=>{
  renderAdvGrid();
  window.addEventListener('resize',()=>{
    if(reportData){const cd=reportData.recentDays?.length>=2?reportData.recentDays:reportData.days;drawChart(cd);}
  });
};

// ══ WEEKDAY COMPARE (금주 vs 지난주) ══
function getMonday(d){
  const r=new Date(d);
  const day=(r.getDay()+6)%7; // 월=0
  r.setDate(r.getDate()-day);
  r.setHours(0,0,0,0);
  return r;
}
function ymd(d){return d.getFullYear()+pad(d.getMonth()+1)+pad(d.getDate());}
function fmtBracket(s,e){
  const f=x=>x.getFullYear()+'.'+pad(x.getMonth()+1)+'.'+pad(x.getDate());
  return `[${f(s)} ~ ${f(e)}]`;
}
function renderWeekCompare(){
  if(!reportData) return;
  const metricKey=document.getElementById('week-metric')?.value||'cost';
  const data=(reportData.recent21Days||[]);

  const end=reportPeriodMeta?.curE||getPeriodDates(reportPeriod, reportCustomRange).curE; // 보통 어제
  const mon=getMonday(end);
  const thisStart=new Date(mon);
  const thisEnd=new Date(thisStart); thisEnd.setDate(thisEnd.getDate()+6);
  const lastStart=new Date(thisStart); lastStart.setDate(lastStart.getDate()-7);
  const lastEnd=new Date(thisEnd); lastEnd.setDate(lastEnd.getDate()-7);

  document.getElementById('week-range-label').textContent = `${fmtBracket(thisStart,thisEnd)} vs ${fmtBracket(lastStart,lastEnd)}`;

  const map={};
  data.forEach(d=>{ map[String(d.dateRaw)]=d; });

  const labels=['월','화','수','목','금','토','일'];
  const thisVals=[], lastVals=[];
  for(let i=0;i<7;i++){
    const ds=new Date(thisStart); ds.setDate(ds.getDate()+i);
    const ls=new Date(lastStart); ls.setDate(ls.getDate()+i);
    const a=map[ymd(ds)]||{};
    const b=map[ymd(ls)]||{};
    thisVals.push(Number(a[metricKey]||0));
    lastVals.push(Number(b[metricKey]||0));
  }

  drawWeekChart(labels,thisVals,lastVals,metricKey);
}

function drawWeekChart(labels,thisVals,lastVals,metricKey){
  const canvas=document.getElementById('week-chart-canvas');
  if(!canvas) return;
  const ctx=canvas.getContext('2d');
  const dpr=window.devicePixelRatio||1;
  const W0=canvas.parentElement.getBoundingClientRect().width;
  const H0=190;
  canvas.width=W0*dpr; canvas.height=H0*dpr;
  canvas.style.width=W0+'px'; canvas.style.height=H0+'px';
  ctx.setTransform(dpr,0,0,dpr,0,0);

  ctx.clearRect(0,0,W0,H0);
  const dark=document.body.getAttribute('data-theme')!=='light';
  const gridC=dark?'rgba(255,255,255,0.05)':'rgba(0,0,0,0.06)';
  const textC=dark?'#94a3b8':'#475569';

  const PAD={t:18,r:14,b:26,l:44};
  const cW=W0-PAD.l-PAD.r, cH=H0-PAD.t-PAD.b;

  // y scale
  const maxV=Math.max(...thisVals,...lastVals,1);
  const hi=maxV*1.15;

  // grid
  for(let i=0;i<=4;i++){
    const y=PAD.t+(cH/4)*i;
    ctx.strokeStyle=gridC; ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(PAD.l,y); ctx.lineTo(PAD.l+cW,y); ctx.stroke();
  }

  // bars
  const n=labels.length;
  const groupW=cW/n;
  const barW=Math.min(18, groupW/3);
  const gap=6;

  function yOf(v){ return PAD.t + cH - (v/hi)*cH; }

  for(let i=0;i<n;i++){
    const x0=PAD.l + i*groupW + groupW/2;
    const v1=thisVals[i], v2=lastVals[i];

    // 금주(채움)
    ctx.fillStyle=dark?'rgba(3,199,90,0.35)':'rgba(3,199,90,0.22)';
    const y1=yOf(v1);
    ctx.fillRect(x0-barW-gap/2, y1, barW, PAD.t+cH-y1);

    // 지난주(외곽)
    ctx.strokeStyle=dark?'rgba(255,255,255,0.25)':'rgba(0,0,0,0.18)';
    ctx.lineWidth=1.5;
    const y2=yOf(v2);
    ctx.strokeRect(x0+gap/2, y2, barW, PAD.t+cH-y2);

    // x label
    ctx.fillStyle=textC;
    ctx.font="10px 'JetBrains Mono', monospace";
    ctx.textAlign='center';
    ctx.fillText(labels[i], x0, H0-8);
  }

  // y labels (0, mid, max)
  ctx.fillStyle=textC;
  ctx.textAlign='right';
  ctx.font="10px 'JetBrains Mono', monospace";
  const fmt = (v)=>{
    if(metricKey==='ctr'||metricKey==='cvr') return (v).toFixed(1)+'%';
    if(metricKey==='roas') return Math.round(v)+'%';
    return Math.round(v).toLocaleString('ko-KR');
  };
  ctx.fillText(fmt(hi), PAD.l-6, PAD.t+10);
  ctx.fillText(fmt(hi/2), PAD.l-6, PAD.t+cH/2+4);
  ctx.fillText('0', PAD.l-6, PAD.t+cH+2);
}

// resize 대응
window.addEventListener('resize', ()=>{
  if(document.getElementById('tab-report')?.classList.contains('active')){
    try{ renderWeekCompare(); }catch(e){}
  }
});

</script>
</body>
</html>
