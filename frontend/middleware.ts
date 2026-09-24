import {NextRequest,NextResponse} from 'next/server';
import {loginRedirectUrl} from './lib/auth-redirect';

export function middleware(request:NextRequest){
  const path=request.nextUrl.pathname;
  if(path.startsWith('/_next')||path==='/login'||path==='/'||path==='/dashboard'||path==='/admin-verify'||path==='/forbidden')return NextResponse.next();
  if(!request.cookies.get('csl_session'))return NextResponse.redirect(loginRedirectUrl(request.url,process.env.CSL_TRUSTED_ORIGIN));
  return NextResponse.next();
}

export const config={matcher:['/((?!favicon.ico).*)']};
