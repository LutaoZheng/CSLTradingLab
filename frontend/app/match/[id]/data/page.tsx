'use client';
import {use,useCallback,useEffect,useState} from 'react';
import Link from 'next/link';
import {API,getJSON} from '../../../../lib/api';
import {cents,clockTime,teamName} from '../../../../lib/display';

const milliseconds=(value:unknown)=>value==null?'—':`${Number(value).toFixed(3)} ms`;
const nanosecondTime=(value:unknown)=>value==null?'—':clockTime(Number(value));

export default function DataPage({params}:{params:Promise<{id:string}>}){
  const {id}=use(params),[d,setD]=useState<any>(),[error,setError]=useState(false),[retryKey,setRetryKey]=useState(0);
  const load=useCallback(async()=>{try{setD(await getJSON(`/api/sessions/${id}/data`));setError(false)}catch{setError(true)}},[id]);
  useEffect(()=>{load();const timer=setInterval(load,2000);return()=>clearInterval(timer)},[load,retryKey]);
  if(error&&!d)return <main className="page"><Link className="back" href="/">← 返回首页</Link><section className="card"><h1>DATA TEMPORARILY UNAVAILABLE</h1><p className="muted">Recorder is still running.</p><button className="button" onClick={()=>setRetryKey(value=>value+1)}>Retry</button></section></main>;
  if(!d)return <main className="page"><Link className="back" href="/">← 返回首页</Link><p>Loading…</p></main>;
  const c=d.counts,r=d.recorder,cal=d.latest_calibration,returnHref=d.status==='ACTIVE'?`/match/${id}`:'/';
  return <main className="page">
    <Link className="back" href={returnHref}>← 返回比赛</Link>
    {error&&<section className="card"><b>DATA TEMPORARILY UNAVAILABLE</b><p className="muted">Showing the latest saved view. Recorder is still running.</p><button className="button" onClick={()=>setRetryKey(value=>value+1)}>Retry</button></section>}
    <h1>SESSION DATA</h1>
    <section className="card"><div className="kv"><span>Session</span><b className="mono">{d.session.id}</b><span>Match</span><b>{teamName(d.session.home_team)} vs {teamName(d.session.away_team)}</b><span>Mode</span><b>{d.session.session_type}</b><span>Status</span><b className={d.status==='ACTIVE'?'ok':''}>{d.status}</b><span>Duration</span><b>{Math.round(d.duration_seconds)}s</b></div><div className="toolbar"><Link className="button" href={`/replay/${id}`}>回放记录</Link><a className="button" href={`${API}/api/sessions/${id}/export`} download>DOWNLOAD ZIP</a></div></section>
    <section className="card"><div className="kv"><span>Kalshi WS Messages</span><b>{c.kalshi_ws_messages}</b><span>Orderbook Events</span><b>{c.orderbook_events}</b><span>Quote Rows</span><b>{c.quote_rows}</b><span>Trade Rows</span><b>{c.trade_rows}</b><span>Human Events</span><b>{c.human_events}</b><span>Current Run Raw Written</span><b>{r.raw_messages_written}</b><span>Current Run DB Envelopes</span><b>{r.db_rows_written}</b><span>Queue Drops</span><b className={r.queue_drop_count===0?'ok':'bad'}>{r.queue_drop_count}</b><span>DB Failures</span><b className={r.db_write_failures===0?'ok':'bad'}>{r.db_write_failures}</b></div></section>
    <h2>LATENCY / MEASUREMENT</h2>
    <section className="card">{cal?<div className="kv"><span>Calibration</span><b className="mono">{cal.id}</b><span>Raw samples</span><b>{cal.samples.length}</b><span>RTT p50</span><b>{milliseconds(cal.rtt_p50_ms)}</b><span>RTT p95</span><b>{milliseconds(cal.rtt_p95_ms)}</b><span>Clock offset estimate</span><b>{milliseconds(cal.offset_ms)}</b></div>:<p className="muted">No clock calibration recorded for this Session.</p>}</section>
    {d.measurements.map((measurement:any)=><section className="card" key={measurement.event_id}><div className="kv"><span>Human Event</span><b>{measurement.event_type}</b><span>Calibration</span><b className="mono">{measurement.calibration_id||'—'}</b><span>Client click</span><b>{new Date(measurement.client_click_epoch_ms).toLocaleTimeString()}</b><span>Pointer → click</span><b>{measurement.pointerdown_perf_ts_ms==null?'—':milliseconds(measurement.client_click_perf_ms-measurement.pointerdown_perf_ts_ms)}</b><span>Server request entry</span><b>{nanosecondTime(measurement.server_request_entry_ts_ns)}</b><span>Server receive</span><b>{nanosecondTime(measurement.server_receive_ts_ns)}</b><span>DB commit complete</span><b>{nanosecondTime(measurement.db_commit_complete_ts_ns)}</b><span>NDJSON fsync complete</span><b>{nanosecondTime(measurement.human_raw_fsync_complete_ts_ns)}</b><span>Phone → AWS calibrated</span><b>{milliseconds(measurement.client_to_server_calibrated_ms)}</b><span>Entry → receive</span><b>{milliseconds(measurement.request_entry_to_receive_ms)}</b><span>Receive → DB</span><b>{milliseconds(measurement.server_receive_to_db_commit_ms)}</b><span>DB → NDJSON</span><b>{milliseconds(measurement.db_commit_to_raw_fsync_ms)}</b><span>AWS entry → persisted</span><b>{milliseconds(measurement.request_entry_to_raw_fsync_ms)}</b></div></section>)}
    <h2>EVENT TIMELINE</h2>
    <section className="card timeline">{d.timeline.map((x:any,i:number)=><div className="timeline-row" key={`${x.kind}-${x.timestamp_ns}-${i}`}><time>{clockTime(x.timestamp_ns)}</time><b className={`kind ${x.kind.toLowerCase()}`}>{x.kind}</b><span>{x.kind==='HUMAN'?(x.team?`${x.team==='HOME'?teamName(d.session.home_team):teamName(d.session.away_team)} `:'')+x.label:x.label}{x.yes_bid!=null||x.yes_ask!=null?` · YES ${cents(x.yes_bid)} / ${cents(x.yes_ask)} · NO ${cents(x.no_bid)} / ${cents(x.no_ask)}`:''}{x.kind==='TRADE'?` · ${cents(x.price)} × ${x.size??'—'}`:''}</span></div>)}</section>
  </main>
}
