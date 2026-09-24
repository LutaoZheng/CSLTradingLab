export function normalizeTrustedOrigin(value?: string): string | null {
  const candidate = value?.trim();
  if (!candidate) return null;
  const url = new URL(candidate);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new Error('PUBLIC_ORIGIN must be an HTTP(S) origin without credentials, path, query, or fragment');
  }
  return url.origin;
}

export function loginRedirectUrl(requestUrl: string, trustedOrigin?: string): URL {
  const configured = normalizeTrustedOrigin(trustedOrigin);
  const base = configured ?? new URL(requestUrl).origin;
  return new URL('/login', base);
}
