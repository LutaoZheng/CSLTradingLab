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
