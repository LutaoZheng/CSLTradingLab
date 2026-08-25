'use client';
import {useEffect,useState} from 'react';
import {teamName} from '../lib/display';

type CachedMatch={event_ticker:string;home_team:string;away_team:string;status:string;markets:unknown[]};

export default function Loading(){
  const [matches,setMatches]=useState<CachedMatch[]>([]);
  useEffect(()=>{try{setMatches(JSON.parse(sessionStorage.getItem('csl-last-known-matches')||'[]'))}catch{}},[]);
  return <main className="page"><h1>CSL Trading Lab</h1><p className="muted">Refreshing matches…</p>{matches.map(m=><section className="card compact" key={m.event_ticker}><div className="match">{teamName(m.home_team)} vs {teamName(m.away_team)}</div><p className="muted">{m.markets.length} Markets · {m.status}</p></section>)}</main>
}
