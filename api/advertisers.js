// api/advertisers.js
// 광고주 목록만 반환 (API 키는 절대 노출하지 않음)
module.exports = function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const raw = process.env.ADVERTISER_KEYS_JSON;
  if (!raw) {
    return res.status(200).json([]);
  }

  let list;
  try {
    list = JSON.parse(raw);
  } catch (e) {
    return res.status(500).json({ error: 'ADVERTISER_KEYS_JSON 파싱 실패. JSON 형식을 확인하세요.' });
  }

  if (!Array.isArray(list)) {
    return res.status(500).json({ error: 'ADVERTISER_KEYS_JSON은 배열이어야 합니다.' });
  }

  // id, name, emoji, cat 만 반환 — cid/lic/sec 절대 노출 금지
  const safe = list.map(({ id, name, emoji, cat }) => ({ id, name, emoji: emoji || '📊', cat: cat || '' }));
  return res.status(200).json(safe);
};
