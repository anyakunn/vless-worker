// Simple VLESS + WebSocket Worker (fixed version)
import { connect } from 'cloudflare:sockets';

// ===================== CONFIG =====================
const userID = 'bb001d2c-24f1-468c-bdb9-3834f4d2f7bc';  // ← Change this UUID

const preferredProxyIPs = [
  '1.1.1.1',
  '1.0.0.1',
  // Add more preferred IPs here if you want
];

const DNS_SERVER = '1.1.1.1';
// ==================================================

const WS_READY_STATE_OPEN = 1;

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');

      if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        return await handleWebSocket(request);
      }

      return new Response(
        `VLESS Worker is running\n\nUUID: ${userID}\nPreferred IPs: ${preferredProxyIPs.join(', ') || 'none'}\nDNS: ${DNS_SERVER}`,
        { status: 200, headers: { 'Content-Type': 'text/plain' } }
      );
    } catch (err) {
      return new Response('Error: ' + err.message, { status: 500 });
    }
  },
};

async function handleWebSocket(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let remoteSocket = null;
  let isClosed = false;

  server.addEventListener('message', async (event) => {
    try {
      if (isClosed) return;

      const data = event.data instanceof ArrayBuffer ? event.data : await event.data.arrayBuffer();

      if (!remoteSocket) {
        const parsed = parseVLESSHeader(new Uint8Array(data), userID);
        if (!parsed) {
          server.close(1000, 'Invalid UUID');
          return;
        }

        const { address, port, rawData } = parsed;

        let targetHost = address;
        if (!isIP(address)) {
          targetHost = (await resolveDNS(address, DNS_SERVER)) || address;
        }

        remoteSocket = await connectWithPreferred(targetHost, port, preferredProxyIPs);

        if (rawData && rawData.byteLength > 0) {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(rawData);
          writer.releaseLock();
        }

        // Client → Remote
        server.addEventListener('message', async (e) => {
          if (remoteSocket && !isClosed) {
            try {
              const writer = remoteSocket.writable.getWriter();
              await writer.write(e.data);
              writer.releaseLock();
            } catch {
              safeClose();
            }
          }
        });

        // Remote → Client
        remoteSocket.readable.pipeTo(new WritableStream({
          write(chunk) {
            if (server.readyState === WS_READY_STATE_OPEN) {
              server.send(chunk);
            }
          },
          close() { safeClose(); },
          abort() { safeClose(); }
        })).catch(() => safeClose());
      }
    } catch (err) {
      safeClose();
    }
  });

  server.addEventListener('close', () => safeClose());
  server.addEventListener('error', () => safeClose());

  function safeClose() {
    if (isClosed) return;
    isClosed = true;
    try { server.close(); } catch (e) {}
    try { if (remoteSocket) remoteSocket.close(); } catch (e) {}
  }

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function connectWithPreferred(hostname, port, preferredList) {
  for (const proxy of preferredList) {
    try {
      const socket = connect({ hostname: proxy, port: port });
      await socket.opened;
      return socket;
    } catch (e) {}
  }

  const socket = connect({ hostname, port });
  await socket.opened;
  return socket;
}

async function resolveDNS(domain, dnsServer) {
  try {
    const response = await fetch(`https://\( {dnsServer}/dns-query?name= \){domain}&type=A`, {
      headers: { 'Accept': 'application/dns-json' }
    });
    const data = await response.json();
    if (data.Answer && data.Answer.length > 0) {
      return data.Answer[0].data;
    }
  } catch (e) {}
  return null;
}

function isIP(str) {
  return /^(\d{1,3}\.){3}\d{1,3}$/.test(str) || str.includes(':');
}

function parseVLESSHeader(buffer, expectedUUID) {
  if (buffer.byteLength < 24) return null;

  const uuidBytes = buffer.slice(1, 17);
  const uuid = stringifyUUID(uuidBytes);
  if (uuid !== expectedUUID) return null;

  let offset = 17;
  const addonLen = buffer[offset++];
  offset += addonLen;

  const command = buffer[offset++];
  if (command !== 1) return null;

  const port = (buffer[offset] << 8) | buffer[offset + 1];
  offset += 2;

  const addressType = buffer[offset++];
  let address = '';
  let addressLen = 0;

  if (addressType === 1) {
    addressLen = 4;
    address = Array.from(buffer.slice(offset, offset + 4)).join('.');
  } else if (addressType === 2) {
    addressLen = buffer[offset++];
    address = new TextDecoder().decode(buffer.slice(offset, offset + addressLen));
  } else {
    return null;
  }

  offset += addressLen;
  const rawData = buffer.slice(offset);

  return { address, port, rawData };
}

function stringifyUUID(arr) {
  const hex = [...arr].map(b => b.toString(16).padStart(2, '0')).join('');
  return `\( {hex.slice(0, 8)}- \){hex.slice(8, 12)}-\( {hex.slice(12, 16)}- \){hex.slice(16, 20)}-${hex.slice(20)}`;
}
