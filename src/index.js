import { connect } from 'cloudflare:sockets';

// ====== CONFIG ======
const userID = 'bb001d2c-24f1-468c-bdb9-3834f4d2f7bc'; // Change this UUID later if you want

const preferredProxyIPs = [
  '1.1.1.1',
  '1.0.0.1'
];

const DNS_SERVER = '1.1.1.1';
// ====================

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');

      if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        return await handleWebSocket(request);
      }

      return new Response(
        `VLESS Worker is running\n\nUUID: ${userID}\nDNS: ${DNS_SERVER}`,
        {
          status: 200,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        }
      );
    } catch (err) {
      return new Response('Worker Error: ' + err.message, { status: 500 });
    }
  },
};

async function handleWebSocket(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let remoteSocket = null;
  let closed = false;

  const close = () => {
    if (closed) return;
    closed = true;
    try { server.close(); } catch (e) {}
    try { if (remoteSocket) remoteSocket.close(); } catch (e) {}
  };

  server.addEventListener('message', async (event) => {
    try {
      if (closed) return;

      const data = event.data instanceof ArrayBuffer
        ? event.data
        : await event.data.arrayBuffer();

      if (!remoteSocket) {
        const parsed = parseVLESSHeader(new Uint8Array(data), userID);
        if (!parsed) {
          server.close(1000, 'Invalid UUID');
          return;
        }

        const { address, port, rawData } = parsed;

        let targetHost = address;
        if (!isIP(address)) {
          targetHost = (await resolveDNS(address)) || address;
        }

        // Try preferred IPs first
        remoteSocket = await connectPreferred(targetHost, port);

        // Send remaining data
        if (rawData.byteLength > 0) {
          const writer = remoteSocket.writable.getWriter();
          await writer.write(rawData);
          writer.releaseLock();
        }

        // Client → Remote
        server.addEventListener('message', async (e) => {
          if (remoteSocket && !closed) {
            try {
              const writer = remoteSocket.writable.getWriter();
              await writer.write(e.data);
              writer.releaseLock();
            } catch (err) {
              close();
            }
          }
        });

        // Remote → Client
        remoteSocket.readable.pipeTo(
          new WritableStream({
            write(chunk) {
              if (server.readyState === 1) {
                server.send(chunk);
              }
            },
            close() { close(); },
            abort() { close(); },
          })
        ).catch(() => close());
      }
    } catch (err) {
      close();
    }
  });

  server.addEventListener('close', close);
  server.addEventListener('error', close);

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function connectPreferred(hostname, port) {
  for (const ip of preferredProxyIPs) {
    try {
      const socket = connect({ hostname: ip, port });
      await socket.opened;
      return socket;
    } catch (e) {}
  }

  // Fallback to direct
  const socket = connect({ hostname, port });
  await socket.opened;
  return socket;
}

async function resolveDNS(domain) {
  try {
    const res = await fetch(`https://\( {DNS_SERVER}/dns-query?name= \){domain}&type=A`, {
      headers: { Accept: 'application/dns-json' },
    });
    const data = await res.json();
    if (data.Answer && data.Answer.length > 0) {
      return data.Answer[0].data;
    }
  } catch (e) {}
  return null;
}

function isIP(str) {
  return /^(\d{1,3}\.){3}\d{1,3}$/.test(str);
}

function parseVLESSHeader(buffer, expectedUUID) {
  if (buffer.byteLength < 24) return null;

  const uuid = stringifyUUID(buffer.slice(1, 17));
  if (uuid !== expectedUUID) return null;

  let offset = 17;
  const addonLen = buffer[offset++];
  offset += addonLen;

  const command = buffer[offset++];
  if (command !== 1) return null; // TCP only

  const port = (buffer[offset] << 8) | buffer[offset + 1];
  offset += 2;

  const atype = buffer[offset++];
  let address = '';
  let len = 0;

  if (atype === 1) {
    // IPv4
    len = 4;
    address = Array.from(buffer.slice(offset, offset + 4)).join('.');
  } else if (atype === 2) {
    // Domain
    len = buffer[offset++];
    address = new TextDecoder().decode(buffer.slice(offset, offset + len));
  } else {
    return null;
  }

  offset += len;
  const rawData = buffer.slice(offset);

  return { address, port, rawData };
}

function stringifyUUID(arr) {
  const hex = [...arr].map((b) => b.toString(16).padStart(2, '0')).join('');
  return `\( {hex.slice(0, 8)}- \){hex.slice(8, 12)}-\( {hex.slice(12, 16)}- \){hex.slice(16, 20)}-${hex.slice(20)}`;
}
