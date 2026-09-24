import type {NetworkTestEvent} from './network-test';

export const NETWORK_TEST_OUTBOX_KEY='csl-network-test-outbox-v1';
type Item={runId:string;event:NetworkTestEvent;attempts:number;lastError?:string};
type Storage={getItem(key:string):string|null;setItem(key:string,value:string):void};
type Options={storage:Storage;send:(runId:string,event:NetworkTestEvent&{fetch_start_perf_ms:number;fetch_start_wall_ms:number;client_attempt_count:number})=>Promise<any>;onChange?:(items:Item[])=>void;onDelivered?:(item:Item,response:any)=>void};

export class NetworkTestOutbox{
  private items:Item[]=[];private busy=new Set<string>();private timer?:ReturnType<typeof setInterval>;private options:Options;
  constructor(options:Options){this.options=options;this.load()}
  snapshot(){return this.items.map(item=>({...item,event:{...item.event}}))}
  enqueue(runId:string,event:NetworkTestEvent){if(this.items.some(x=>x.event.event_id===event.event_id))return;this.items.push({runId,event:{...event},attempts:0});this.save()}
  async retry(eventId:string){const item=this.items.find(x=>x.event.event_id===eventId);if(!item||this.busy.has(eventId))return;this.busy.add(eventId);item.attempts+=1;try{const response=await this.options.send(item.runId,{...item.event,fetch_start_perf_ms:performance.now(),fetch_start_wall_ms:Date.now(),client_attempt_count:item.attempts});this.items=this.items.filter(x=>x.event.event_id!==eventId);this.save();this.options.onDelivered?.(item,response)}catch(error){item.lastError=error instanceof Error?error.message:'send failed';this.save()}finally{this.busy.delete(eventId)}}
  retryAll(){for(const item of [...this.items])void this.retry(item.event.event_id)}
  start(){this.retryAll();this.timer=setInterval(()=>this.retryAll(),3000)}
  stop(){if(this.timer)clearInterval(this.timer)}
  private load(){try{const value=JSON.parse(this.options.storage.getItem(NETWORK_TEST_OUTBOX_KEY)||'[]');if(Array.isArray(value))this.items=value}catch{this.items=[]}this.notify()}
  private save(){try{this.options.storage.setItem(NETWORK_TEST_OUTBOX_KEY,JSON.stringify(this.items))}finally{this.notify()}}
  private notify(){this.options.onChange?.(this.snapshot())}
}

export function browserNetworkTestStorage():Storage{return localStorage}
