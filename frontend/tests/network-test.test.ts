import test from 'node:test';
import assert from 'node:assert/strict';
import {computeCalibration,type ClockSample,type NetworkTestEvent} from '../lib/network-test.ts';
import {NETWORK_TEST_OUTBOX_KEY,NetworkTestOutbox} from '../lib/network-test-outbox.ts';

class MemoryStorage{data=new Map<string,string>();getItem(key:string){return this.data.get(key)??null}setItem(key:string,value:string){this.data.set(key,value)}}
const event=(id='00000000-0000-4000-8000-000000000001'):NetworkTestEvent=>({event_id:id,calibration_id:null,scenario:'RECOVERY',vpn_confirmed:true,pointerdown_perf_ms:1,pointerdown_wall_ms:1001,created_perf_ms:2,created_wall_ms:1002,enqueue_perf_ms:3,enqueue_wall_ms:1003});

test('clock calibration uses low-RTT midpoint samples and carries uncertainty',()=>{
  const samples:ClockSample[]=Array.from({length:10},(_,sequence)=>({sequence,client_send_wall_ms:1000,client_receive_wall_ms:1020+sequence,client_send_perf_ms:10,client_receive_perf_ms:30+sequence,server_receive_ts_ns:'0',server_send_ts_ns:'0',rtt_ms:20+sequence,offset_ms:sequence===9?500:5+sequence*.1}));
  const result=computeCalibration(samples);
  assert.ok(result.offset_ms>5&&result.offset_ms<6);
  assert.ok(result.uncertainty_ms>=12);
  assert.equal(result.samples.length,10);
});

test('isolated outbox survives refresh and preserves event identity and creation timestamps',async()=>{
  const storage=new MemoryStorage();let online=false;
  const first=new NetworkTestOutbox({storage,send:async()=>{throw new Error('offline')}});
  const original=event();first.enqueue('run-1',original);await first.retry(original.event_id);
  assert.equal(JSON.parse(storage.getItem(NETWORK_TEST_OUTBOX_KEY)!).length,1);
  let delivered:any;
  const restored=new NetworkTestOutbox({storage,send:async(runId,payload)=>{assert.equal(runId,'run-1');assert.equal(payload.event_id,original.event_id);assert.equal(payload.created_wall_ms,original.created_wall_ms);assert.equal(payload.client_attempt_count,2);if(!online)throw new Error('offline');return {ok:true}},onDelivered:(item,response)=>{delivered={item,response}}});
  online=true;await restored.retry(original.event_id);
  assert.equal(restored.snapshot().length,0);
  assert.equal(delivered.item.event.scenario,'RECOVERY');
});

test('outbox suppresses duplicate local enqueue and keeps test storage separate',()=>{
  const storage=new MemoryStorage();const box=new NetworkTestOutbox({storage,send:async()=>({ok:true})});
  box.enqueue('run-1',event());box.enqueue('run-1',event());
  assert.equal(box.snapshot().length,1);
  assert.equal(NETWORK_TEST_OUTBOX_KEY,'csl-network-test-outbox-v1');
  assert.notEqual(NETWORK_TEST_OUTBOX_KEY,'csl-human-event-outbox-v1');
});
