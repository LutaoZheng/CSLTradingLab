import {createUuid} from './uuid.ts';

export type ClockSample={sequence:number;client_send_wall_ms:number;client_receive_wall_ms:number;client_send_perf_ms:number;client_receive_perf_ms:number;server_receive_ts_ns:string;server_send_ts_ns:string;rtt_ms:number;offset_ms:number};
export type Calibration={calibration_id:string;client_created_wall_ms:number;samples:ClockSample[];offset_ms:number;uncertainty_ms:number;rtt_p50_ms:number;rtt_p95_ms:number;rtt_p99_ms:number;rtt_max_ms:number;jitter_ms:number};
export type NetworkTestEvent={event_id:string;calibration_id:string|null;scenario:'MANUAL'|'BURST_20'|'FIXED_1M'|'SOAK_5M'|'RECOVERY'|'DUPLICATE'|'BACKGROUND';vpn_confirmed:boolean|null;pointerdown_perf_ms:number|null;pointerdown_wall_ms:number|null;created_perf_ms:number;created_wall_ms:number;enqueue_perf_ms:number;enqueue_wall_ms:number};

function percentile(values:number[],p:number){if(!values.length)return 0;const ordered=[...values].sort((a,b)=>a-b),rank=(ordered.length-1)*p,lo=Math.floor(rank),hi=Math.ceil(rank);return ordered[lo]+(ordered[hi]-ordered[lo])*(rank-lo)}
function median(values:number[]){return percentile(values,.5)}

export function computeCalibration(samples:ClockSample[]):Calibration{
  if(samples.length<5)throw new Error('At least five clock samples are required');
  const ordered=[...samples].sort((a,b)=>a.rtt_ms-b.rtt_ms),best=ordered.slice(0,Math.max(5,Math.ceil(ordered.length/2))),offset=median(best.map(x=>x.offset_ms));
  const spread=median(best.map(x=>Math.abs(x.offset_ms-offset))),rtts=samples.map(x=>x.rtt_ms);
  return {calibration_id:createUuid(),client_created_wall_ms:Date.now(),samples,offset_ms:offset,uncertainty_ms:Math.max(...best.map(x=>x.rtt_ms))/2+spread,rtt_p50_ms:percentile(rtts,.5),rtt_p95_ms:percentile(rtts,.95),rtt_p99_ms:percentile(rtts,.99),rtt_max_ms:Math.max(...rtts),jitter_ms:spread};
}

export function createNetworkTestEvent(scenario:NetworkTestEvent['scenario'],vpnConfirmed:boolean|null,pointer?:{perf:number;wall:number}):NetworkTestEvent{
  const createdPerf=performance.now(),createdWall=Date.now(),enqueuePerf=performance.now(),enqueueWall=Date.now();
  return {event_id:createUuid(),calibration_id:null,scenario,vpn_confirmed:vpnConfirmed,pointerdown_perf_ms:pointer?.perf??null,pointerdown_wall_ms:pointer?.wall??null,created_perf_ms:createdPerf,created_wall_ms:createdWall,enqueue_perf_ms:enqueuePerf,enqueue_wall_ms:enqueueWall};
}

export async function sampleClock(runId:string,post:(path:string,body:unknown)=>Promise<any>,count=10):Promise<Calibration>{
  const samples:ClockSample[]=[];
  for(let sequence=0;sequence<count;sequence++){
    const clientSendWall=Date.now(),clientSendPerf=performance.now();
    const response=await post(`/api/network-tests/operator/runs/${runId}/ping`,{sequence,client_send_wall_ms:clientSendWall});
    const clientReceivePerf=performance.now(),clientReceiveWall=Date.now();
    const serverReceive=Number(BigInt(response.server_receive_ts_ns))/1e6,serverSend=Number(BigInt(response.server_send_ts_ns))/1e6;
    const rtt=Math.max(0,(clientReceivePerf-clientSendPerf)-(serverSend-serverReceive));
    const offset=((serverReceive+serverSend)/2)-((clientSendWall+clientReceiveWall)/2);
    samples.push({sequence,client_send_wall_ms:clientSendWall,client_receive_wall_ms:clientReceiveWall,client_send_perf_ms:clientSendPerf,client_receive_perf_ms:clientReceivePerf,server_receive_ts_ns:response.server_receive_ts_ns,server_send_ts_ns:response.server_send_ts_ns,rtt_ms:rtt,offset_ms:offset});
  }
  return computeCalibration(samples);
}
