'use client';
import {useState} from 'react';
import {postJSON} from '../../lib/api';

export function LogoutButton({className='text-button'}:{className?:string}){
  const [busy,setBusy]=useState(false),[failed,setFailed]=useState(false);
  async function logout(){
    setBusy(true);setFailed(false);
    try{await postJSON('/api/auth/logout',{});location.replace('/login')}
    catch{setFailed(true);setBusy(false)}
  }
  return <button className={className} onClick={logout} disabled={busy} title={failed?'Logout failed; retry when connected.':undefined}>{failed?'RETRY LOGOUT':busy?'LOGGING OUT…':'LOGOUT'}</button>
}
