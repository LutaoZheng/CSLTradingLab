import {NextRequest,NextResponse} from 'next/server';

export function middleware(request:NextRequest){
  const path=request.nextUrl.pathname;
  if(path.startsWith('/_next')||path==='/login')return NextResponse.next();
  if(!request.cookies.get('csl_session'))return NextResponse.redirect(new URL('/login',request.url));
  return NextResponse.next();
}

export const config={matcher:['/((?!favicon.ico).*)']};
