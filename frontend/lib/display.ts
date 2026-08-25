export const teamName=(name:string)=>name;
export const cents=(value:unknown)=>value===null||value===undefined?'—':`${Math.round(Number(value)*100)}¢`;
export const clockTime=(ns:number)=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit',fractionalSecondDigits:3,hour12:false}).format(new Date(ns/1e6));
