const configuredApiBase = (process.env.NEXT_PUBLIC_API_URL || '').trim();
const configuredWsBase = (process.env.NEXT_PUBLIC_WS_URL || '').trim();

function withoutTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function withLeadingSlash(path: string): string {
  return path.startsWith('/') ? path : `/${path}`;
}

// An empty API base deliberately means browser same-origin.
export const API = withoutTrailingSlash(configuredApiBase);

export function apiUrl(path: string): string {
  return `${API}${withLeadingSlash(path)}`;
}

export function getWebSocketUrl(path = '/ws'): string {
  const socketPath = withLeadingSlash(path);

  if (configuredWsBase) {
    const base = withoutTrailingSlash(configuredWsBase);
    return base.endsWith(socketPath) ? base : `${base}${socketPath}`;
  }

  if (typeof window === 'undefined') {
    throw new Error('The same-origin WebSocket URL is only available in the browser');
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}${socketPath}`;
}

export async function getJSON(path: string) {
  const response = await fetch(apiUrl(path), {cache: 'no-store'});
  if (!response.ok) throw Error(await response.text());
  return response.json();
}

export async function postJSON(path: string, body: unknown) {
  const response = await fetch(apiUrl(path), {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  });
  if (!response.ok) throw Error(await response.text());
  return response.json();
}

export async function deleteJSON(path: string, body: unknown) {
  const response = await fetch(apiUrl(path), {
    method: 'DELETE',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  });
  if (!response.ok) throw Error(await response.text());
  return response.json();
}
