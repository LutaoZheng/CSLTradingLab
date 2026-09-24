import {expect,test,type Page} from '@playwright/test';

async function signIn(page:Page,username:string,password:string){
  await page.goto('/login');
  await page.getByLabel('Username').fill(username);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button',{name:'SIGN IN'}).click();
}

test('operator and admin remain isolated in independent browser contexts',async({browser})=>{
  const operatorContext=await browser.newContext();
  const adminContext=await browser.newContext();
  const operator=await operatorContext.newPage();
  const admin=await adminContext.newPage();

  await signIn(operator,'operator','browser-operator-pass');
  await expect(operator).toHaveURL(/\/live\/chongqing$/);
  await expect(operator.getByText('NO ACTIVE MATCH')).toBeVisible();

  await signIn(admin,'michael','browser-admin-pass');
  await expect(admin).toHaveURL(/\/dashboard$/);
  await expect(admin.getByText('NO ACTIVE MATCH')).toBeVisible();
  await expect(admin.getByRole('button',{name:'LOGOUT'})).toBeVisible();

  const operatorRole=await operator.evaluate(async()=>await (await fetch('http://localhost:8000/api/auth/me',{credentials:'include'})).json());
  const adminRole=await admin.evaluate(async()=>await (await fetch('http://localhost:8000/api/auth/me',{credentials:'include'})).json());
  expect(operatorRole.role).toBe('OPERATOR');
  expect(adminRole.role).toBe('ADMIN');
  expect((await operatorContext.request.get('http://localhost:8000/api/sessions')).status()).toBe(403);
  expect((await adminContext.request.get('http://localhost:8000/api/sessions')).status()).toBe(200);

  await operator.goto('/dashboard');
  await expect(operator).toHaveURL(/\/forbidden$/);
  await admin.goto('/live/chongqing');
  await expect(admin.getByRole('button',{name:'LOGOUT'})).toBeVisible();

  await admin.getByRole('button',{name:'LOGOUT'}).click();
  await expect(admin).toHaveURL(/\/login$/);
  expect((await adminContext.request.get('http://localhost:8000/api/sessions')).status()).toBe(401);
  expect((await operator.evaluate(async()=>await (await fetch('http://localhost:8000/api/auth/me',{credentials:'include'})).json())).role).toBe('OPERATOR');

  await operatorContext.close();
  await adminContext.close();
});

test('admin authorizes an isolated network test and operator receives an ACK',async({browser})=>{
  const operatorContext=await browser.newContext();
  const adminContext=await browser.newContext();
  const operator=await operatorContext.newPage();
  const admin=await adminContext.newPage();
  await signIn(operator,'operator','browser-operator-pass');
  await signIn(admin,'michael','browser-admin-pass');
  await expect(admin.getByText('NETWORK TEST · TEST ONLY')).toBeVisible();
  await admin.getByRole('button',{name:'CREATE NETWORK TEST'}).click();
  await expect(admin.getByRole('button',{name:'END TEST'})).toBeVisible();
  await operator.getByRole('link',{name:'NETWORK TEST · TEST ONLY'}).click();
  await expect(operator).toHaveURL(/\/live\/chongqing\/network-test$/);
  await expect(operator.getByText('TEST AUTHORIZED')).toBeVisible();
  await operator.getByRole('button',{name:'VPN CONFIRMED'}).click();
  await operator.getByRole('button',{name:'CALIBRATE CLOCK'}).click();
  await expect(operator.getByRole('button',{name:'RECALIBRATE CLOCK'})).toBeVisible();
  await operator.getByRole('button',{name:'SEND TEST PULSE'}).click();
  await expect(operator.getByText('ACK',{exact:true})).toBeVisible();
  await operator.getByRole('button',{name:'DUPLICATE LAST'}).click();
  await expect(operator.getByText('DEDUPLICATED')).toBeVisible();
  await operatorContext.setOffline(true);
  await operator.getByRole('button',{name:'SEND TEST PULSE'}).click();
  await expect(operator.getByText('1 TEST EVENT IN OUTBOX')).toBeVisible();
  await operatorContext.setOffline(false);
  await expect(operator.getByText('1 TEST EVENT IN OUTBOX')).not.toBeVisible({timeout:10_000});
  expect((await operatorContext.request.get('http://localhost:8000/api/network-tests/admin/runs')).status()).toBe(403);
  await admin.getByRole('button',{name:'END TEST'}).click();
  await expect(admin.getByRole('button',{name:'CREATE NETWORK TEST'})).toBeVisible();
  await operator.reload();
  await expect(operator.getByText('NO ACTIVE NETWORK TEST')).toBeVisible();
  await operatorContext.close();await adminContext.close();
});
