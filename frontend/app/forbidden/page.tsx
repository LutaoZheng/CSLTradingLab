'use client';
import Link from 'next/link';

export default function Forbidden(){
  return <main className="auth-page"><section className="auth-card"><div className="brand-mark">403</div><h1>无管理员权限</h1><p className="muted center">当前操作员账号不能访问 Dashboard、比赛管理或研究数据。</p><Link className="button" href="/live/chongqing">返回现场页面</Link></section></main>;
}
