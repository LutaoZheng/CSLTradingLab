import type { NextConfig } from 'next';
import {existsSync, readFileSync} from 'node:fs';
import {resolve} from 'node:path';

const rootEnvPath = resolve(process.cwd(), '..', '.env');
const rootEnv: Record<string, string> = {};
if (existsSync(rootEnvPath)) {
  for (const rawLine of readFileSync(rootEnvPath, 'utf8').split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf('=');
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    if (['NEXT_PUBLIC_API_URL', 'NEXT_PUBLIC_WS_URL', 'PUBLIC_ORIGIN', 'APP_ENV'].includes(key)) {
      rootEnv[key] = line.slice(separator + 1).trim().replace(/^['"]|['"]$/g, '');
    }
  }
}

const publicEnv: Record<string, string> = {};
for (const key of ['NEXT_PUBLIC_API_URL', 'NEXT_PUBLIC_WS_URL'] as const) {
  // An explicitly empty build environment variable disables a root .env value,
  // allowing a same-origin production build without changing source code.
  const processValue = process.env[key];
  const value = (processValue !== undefined ? processValue : rootEnv[key])?.trim();
  if (value) publicEnv[key] = value;
}

function normalizeOrigin(value: string | undefined): string | undefined {
  const candidate = value?.trim();
  if (!candidate) return undefined;
  const url = new URL(candidate);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new Error('PUBLIC_ORIGIN must be an HTTP(S) origin without credentials, path, query, or fragment');
  }
  return url.origin;
}

const appEnv = (process.env.APP_ENV ?? rootEnv.APP_ENV ?? 'development').trim().toLowerCase();
const trustedOrigin = normalizeOrigin(process.env.PUBLIC_ORIGIN ?? rootEnv.PUBLIC_ORIGIN);
if (appEnv === 'production' && !trustedOrigin) {
  throw new Error('PUBLIC_ORIGIN is required for a production frontend build');
}
if (appEnv === 'production' && !trustedOrigin?.startsWith('https://')) {
  throw new Error('PUBLIC_ORIGIN must use HTTPS for a production frontend build');
}

const config: NextConfig = {
  output: 'standalone',
  env: {...publicEnv, ...(trustedOrigin ? {CSL_TRUSTED_ORIGIN: trustedOrigin} : {})},
};
export default config;
