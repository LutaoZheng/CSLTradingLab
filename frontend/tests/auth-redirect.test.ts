import assert from 'node:assert/strict';
import test from 'node:test';
import {loginRedirectUrl, normalizeTrustedOrigin} from '../lib/auth-redirect.ts';

const productionOrigin = 'https://csltradinglab.duckdns.org';

test('standalone internal root redirects to the configured public login page', () => {
  assert.equal(loginRedirectUrl('https://localhost:3000/', productionOrigin).href, `${productionOrigin}/login`);
});

test('standalone internal operator page redirects to the configured public login page', () => {
  assert.equal(loginRedirectUrl('https://localhost:3000/live/chongqing', productionOrigin).href, `${productionOrigin}/login`);
});

test('malicious request hosts cannot override the configured redirect origin', () => {
  assert.equal(loginRedirectUrl('https://evil.example/live/chongqing', productionOrigin).href, `${productionOrigin}/login`);
  assert.equal(loginRedirectUrl('https://forwarded-host-attacker.example/', productionOrigin).href, `${productionOrigin}/login`);
});

test('local development falls back to the request origin when no trusted origin is configured', () => {
  assert.equal(loginRedirectUrl('http://localhost:3000/', undefined).href, 'http://localhost:3000/login');
});

test('trusted origin rejects credentials and non-origin URL components', () => {
  assert.throws(() => normalizeTrustedOrigin('https://user@example.com'));
  assert.throws(() => normalizeTrustedOrigin('https://example.com/path'));
  assert.throws(() => normalizeTrustedOrigin('javascript:alert(1)'));
});

test('the login route remains explicitly excluded from authentication redirects', async () => {
  const source = await import('node:fs/promises').then(fs => fs.readFile(new URL('../middleware.ts', import.meta.url), 'utf8'));
  assert.match(source, /path==='\/login'/);
});

test('post-login and administrator destinations remain same-origin relative paths', async () => {
  const loginSource = await import('node:fs/promises').then(fs => fs.readFile(new URL('../app/login/page.tsx', import.meta.url), 'utf8'));
  const adminSource = await import('node:fs/promises').then(fs => fs.readFile(new URL('../app/admin-verify/page.tsx', import.meta.url), 'utf8'));
  assert.doesNotMatch(loginSource, /localhost/);
  assert.doesNotMatch(adminSource, /localhost/);
  assert.match(adminSource, /router\.replace\('\/'\)/);
});
