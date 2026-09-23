'use client';
import {useCallback,useEffect,useRef,useState} from 'react';
import {ApiError,getJSON,postJSON} from '../../../lib/api';
import {createHumanEventDraft} from '../../../lib/human-events';
import {getBrowserOutboxStorage,HumanEventOutbox,type OutboxSnapshot} from '../../../lib/human-event-outbox';

type Button={event_type:string;label:string;teams:string[]};
type Match={session_id:string;event_ticker:string;home_label:string;away_label:string;start_time:string;timezone:string;allowed_event_buttons:Button[]};
type Boot={match:Match|null;connection:{server:boolean;kalshi_connected:boolean;kalshi_message_age_ms:number|null};events:any[]};
const labels:Record<string,string>={BALL_IN_NET:'GOAL',PENALTY_EVENT:'PENALTY',RED_CARD_EVENT:'RED CARD',VAR_CHECK:'VAR',EVENT_VOIDED:'CORRECTION'};

export default function Chongqing(){
  const [data,setData]=useState<Boot|null>(null),[online,setOnline]=useState(true),[delivery,setDelivery]=useState<OutboxSnapshot>({items:[],durabilityDegraded:false}),[error,setError]=useState(''),[lastAck,setLastAck]=useState<any>(null);
  const outbox=useRef<HumanEventOutbox|null>(null),pointer=useRef<Record<string,number>>({}),lastEvent=useRef<{id:string;group:string}|null>(null);
  const load=useCallback(()=>getJSON('/api/operator/bootstrap').then(setData).catch(e=>{if(e instanceof ApiError&&e.status===401)location.assign('/login');else setError('服务器状态暂时不可用。')}),[]);
  useEffect(()=>{setOnline(navigator.onLine);const up=()=>{setOnline(true);outbox.current?.retryAll()},down=()=>setOnline(false);addEventListener('online',up);addEventListener('offline',down);void load();const poll=setInterval(load,3000);return()=>{removeEventListener('online',up);removeEventListener('offline',down);clearInterval(poll)}},[load]);
  useEffect(()=>{const manager=new HumanEventOutbox({storage:getBrowserOutboxStorage(),send:async(_sid,event)=>{const fetchStart=performance.now();const response=await postJSON('/api/operator/events',{...event,client_fetch_start_perf_ts_ms:fetchStart});const ackPerf=performance.now(),ackWall=Date.now();setLastAck({...response,client_enqueue_perf_ts_ms:event.client_enqueue_perf_ts_ms,client_fetch_start_perf_ts_ms:fetchStart,client_ack_perf_ts_ms:ackPerf,ack_ms:ackPerf-fetchStart});void postJSON(`/api/operator/events/${event.event_id}/ack`,{client_ack_perf_ts_ms:ackPerf,client_ack_wall_ts_ms:ackWall});return response},onChange:setDelivery,onDelivered:()=>void load()});outbox.current=manager;manager.start();return()=>{manager.stop();outbox.current=null}},[load]);
  function down(key:string){pointer.current[key]=performance.now()}
  function send(type:string,team?:string,key?:string,detail:Record<string,unknown>={}){if(!data?.match){setError('暂无比赛，信号未发送。');return}const enqueue=performance.now(),group=type==='EVENT_VOIDED'&&lastEvent.current?lastEvent.current.group:undefined;const draft=createHumanEventDraft({eventType:type,team,group,detail,pointerdownPerfTsMs:key?pointer.current[key]:undefined});draft.payload.client_enqueue_perf_ts_ms=enqueue;draft.payload.authorized_event_ticker=data.match.event_ticker;if(key)delete pointer.current[key];if(type!=='EVENT_VOIDED')lastEvent.current={id:draft.eventId,group:draft.eventGroupId};const queued=outbox.current?.enqueue(data.match.session_id,draft.payload);if(!queued)setError('可靠发送队列尚未就绪，请重试。');else void outbox.current?.retry(draft.eventId)}
  function correction(){if(!lastEvent.current){setError('没有可更正的上一条事件。');return}send('EVENT_VOIDED',undefined,undefined,{target_event_id:lastEvent.current.id,reason:'OPERATOR_CORRECTION'})}
  async function logout(){await postJSON('/api/auth/logout',{});location.assign('/login')}
  const match=data?.match,server=!!data,kalshi=!!match&&!!data?.connection.kalshi_connected&&((data.connection.kalshi_message_age_ms??Infinity)<30000),buttons=match?.allowed_event_buttons||[];
  const teamButtons=buttons.filter(x=>x.teams.length),globalButtons=buttons.filter(x=>!x.teams.length&&x.event_type!=='EVENT_VOIDED'),hasCorrection=buttons.some(x=>x.event_type==='EVENT_VOIDED');
  return <main className="signal-page">
    <header className="signal-head"><div><small>CSLTradingLab · LIVE</small><h1>{match?<>{match.home_label} <span>vs</span> {match.away_label}</>:'暂无比赛'}</h1></div><button className="text-button" onClick={logout}>退出</button></header>
    <div className="connection-strip"><span className={online?'ok':'bad'}>● NETWORK {online?'ONLINE':'OFFLINE'}</span><span className={server?'ok':'bad'}>● SERVER {server?'CONNECTED':'DOWN'}</span><span className={kalshi?'ok':'bad'}>● FEED {kalshi?'LIVE':match?'STALE':'NO MATCH'}</span></div>
    {!match&&<section className="ack-card no-match"><b>暂无比赛 / NO ACTIVE MATCH</b><p className="muted">等待管理员配置并激活精确比赛合约。当前无法提交真实赛事信号。</p></section>}
    {delivery.items.length>0&&<div className="queue-warning">{delivery.items.length} EVENT{delivery.items.length>1?'S':''} WAITING TO SEND</div>}{delivery.durabilityDegraded&&<div className="queue-warning bad">LOCAL QUEUE NOT DURABLE — KEEP PAGE OPEN</div>}{error&&<p className="auth-error">{error}</p>}
    <section className="signal-grid">{teamButtons.flatMap(button=>button.teams.map(team=>{const key=`${button.event_type}-${team}`,style=button.event_type==='BALL_IN_NET'?'goal':button.event_type==='PENALTY_EVENT'?'penalty':'red';return <button disabled={!match} key={key} className={`signal-button ${style}`} onPointerDown={()=>down(key)} onClick={()=>send(button.event_type,team,key)}><small>{team}</small>{button.label}</button>}))}</section>
    {globalButtons.map(button=>{const key=button.event_type;return <button disabled={!match} key={key} className="signal-button var" onPointerDown={()=>down(key)} onClick={()=>send(button.event_type,undefined,key)}>{button.label}</button>})}
    {hasCorrection&&<button disabled={!match} className="signal-button correction" onClick={correction}>CORRECTION</button>}
    <section className="ack-card"><h2>LAST ACK</h2>{lastAck?<><b>{lastAck.duplicate?'DUPLICATE CONFIRMED':'RECEIVED'}</b><div className="ack-grid"><span>Enqueue → fetch</span><strong>{(lastAck.client_fetch_start_perf_ts_ms-lastAck.client_enqueue_perf_ts_ms).toFixed(1)} ms</strong><span>Fetch → ACK</span><strong>{lastAck.ack_ms.toFixed(1)} ms</strong><span>Server entry</span><strong>{lastAck.server_request_entry_ts_ns}</strong><span>Server receive</span><strong>{lastAck.server_receive_ts_ns}</strong><span>Realtime eligible</span><strong className={lastAck.realtime_eligible?'ok':'bad'}>{lastAck.realtime_eligible?'YES':'NO'}</strong></div></>:<p className="muted">等待第一个事件确认</p>}</section>
    <section className="ack-card"><h2>RECENT CONFIRMED EVENTS</h2>{data?.events.map(e=><div className="operator-event" key={e.event_id}><span>{new Date(e.device_wall_ts_ms).toLocaleTimeString()}</span><b>{e.team?`${e.team} `:''}{labels[e.event_type]||e.event_type}</b><i className={e.realtime_eligible?'ok':'bad'}>{e.realtime_eligible?'LIVE':'HISTORICAL'}</i></div>)}</section>
  </main>
}
