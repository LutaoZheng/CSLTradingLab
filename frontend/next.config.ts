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

const config: NextConfig = {
  output: 'standalone',
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || rootEnv.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
    NEXT_PUBLIC_WS_URL: process.env.NEXT_PUBLIC_WS_URL || rootEnv.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000',
  },
};
export default config;
