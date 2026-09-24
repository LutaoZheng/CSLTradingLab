'use client';
import {FormEvent,useEffect,useState} from 'react';
import {useRouter} from 'next/navigation';
import {ApiError,getJSON,postJSON} from '../../lib/api';

export default function Login(){
  const [username,setUsername]=useState(''),[password,setPassword]=useState(''),[remember,setRemember]=useState(false),[error,setError]=useState(''),[busy,setBusy]=useState(false),router=useRouter();
  useEffect(()=>{getJSON('/api/auth/me').then(x=>{if(x.authenticated)router.replace(x.destination)}).catch(()=>{})},[router]);
  async function submit(e:FormEvent){e.preventDefault();setBusy(true);setError('');try{const x=await postJSON('/api/auth/login',{username,password,remember});router.replace(x.destination)}catch(err){setError(err instanceof ApiError&&err.status===429?'登录尝试过多，请稍后再试。':'用户名或密码错误。')}finally{setBusy(false)}}
  return <main className="auth-page"><section className="auth-card"><div className="brand-mark">CSL</div><h1>CSLTradingLab</h1><p className="muted center">重庆现场信号终端</p><form onSubmit={submit}><label>Username<input autoCapitalize="none" autoComplete="username" value={username} onChange={e=>setUsername(e.target.value)} required/></label><label>Password<input type="password" autoComplete="current-password" value={password} onChange={e=>setPassword(e.target.value)} required/></label><label className="remember"><input type="checkbox" checked={remember} onChange={e=>setRemember(e.target.checked)}/> Remember this device for 30 days</label>{error&&<p className="auth-error" role="alert">{error}</p>}<button className="button" disabled={busy}>{busy?'SIGNING IN…':'SIGN IN'}</button></form></section></main>
}
