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
    if (key === 'NEXT_PUBLIC_API_URL' || key === 'NEXT_PUBLIC_WS_URL') {
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

const config: NextConfig = {
  output: 'standalone',
  env: publicEnv,
};
export default config;
