'use client';
import {useEffect} from 'react';
import {useRouter} from 'next/navigation';
import {getJSON} from '../lib/api';

export default function Home(){
  const router=useRouter();
  useEffect(()=>{getJSON('/api/auth/me').then(x=>router.replace(x.destination||'/login')).catch(()=>router.replace('/login'))},[router]);
  return <main className="page">Checking account authorization…</main>;
}
