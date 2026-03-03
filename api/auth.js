// api/auth.js
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  let body = req.body;
  if (!body || typeof body === 'string' || Object.keys(body || {}).length === 0) {
    body = await new Promise(resolve => {
      let raw = '';
      req.on('data', c => raw += c);
      req.on('end', () => { try { resolve(JSON.parse(raw)); } catch { resolve({}); } });
    });
  }

  const pw = process.env.SITE_PASSWORD || '';
  if (!pw) return res.status(200).json({ ok: true }); // 환경변수 미설정 시 통과
  if ((body.password || '') === pw) return res.status(200).json({ ok: true });
  return res.status(401).json({ ok: false, error: '비밀번호가 틀렸습니다' });
};
