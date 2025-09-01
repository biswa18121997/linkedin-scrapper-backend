// keepAlive.js
export function withHeartbeat(req, res, next) {
  // Start a chunked response early so proxies don't idle-timeout
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  // If you run behind nginx or similar:
  res.setHeader('X-Accel-Buffering', 'no');

  // Kick the stream so headers flush
  try { res.write(' '); } catch {}

  // Send a single space every 15s to keep the connection alive
  const iv = setInterval(() => {
    try { res.write(' '); } catch { clearInterval(iv); }
  }, 15000);

  const cleanup = () => clearInterval(iv);
  res.on('close', cleanup);
  res.on('finish', cleanup);

  next();
}
