import {defineConfig} from '@playwright/test';

const chrome='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

export default defineConfig({
  testDir:'./tests/e2e',
  fullyParallel:false,
  workers:1,
  timeout:45_000,
  use:{baseURL:'http://localhost:3000',headless:true,trace:'retain-on-failure'},
  projects:[{name:'chromium',use:{launchOptions:process.platform==='darwin'?{executablePath:chrome}:{}}}],
  webServer:[
    {command:'PYTHONPATH=. .venv/bin/python tests/e2e_server.py',cwd:'../backend',url:'http://localhost:8000/api/auth/me',reuseExistingServer:false,timeout:30_000},
    {command:'NEXT_PUBLIC_API_URL=http://localhost:8000 NEXT_PUBLIC_WS_URL=ws://localhost:8000 pnpm dev --hostname localhost',cwd:'.',url:'http://localhost:3000/login',reuseExistingServer:false,timeout:60_000},
  ],
});
