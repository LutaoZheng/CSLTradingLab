export const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export async function getJSON(path:string){const r=await fetch(API+path,{cache:'no-store'});if(!r.ok)throw Error(await r.text());return r.json()}
export async function postJSON(path:string,body:unknown){const r=await fetch(API+path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});if(!r.ok)throw Error(await r.text());return r.json()}
export async function deleteJSON(path:string,body:unknown){const r=await fetch(API+path,{method:'DELETE',headers:{'content-type':'application/json'},body:JSON.stringify(body)});if(!r.ok)throw Error(await r.text());return r.json()}
