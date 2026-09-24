import hashlib, hmac, secrets, time
from dataclasses import dataclass
from datetime import datetime, timezone
from fastapi import HTTPException, Request, WebSocket
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from .models import AuthSession

SESSION_COOKIE="csl_session"
CSRF_COOKIE="csl_csrf"
OPERATOR_ROLE="OPERATOR"
ADMIN_ROLE="ADMIN"
SESSION_ROLES={OPERATOR_ROLE,ADMIN_ROLE}

def hash_password(password:str, *, salt:bytes|None=None)->str:
    """stdlib scrypt password hash; encoded value is safe to store in the environment."""
    salt=salt or secrets.token_bytes(16)
    digest=hashlib.scrypt(password.encode(),salt=salt,n=2**14,r=8,p=1,dklen=32)
    return f"scrypt$16384$8$1${salt.hex()}${digest.hex()}"

def verify_password(password:str, encoded:str)->bool:
    try:
        algorithm,n,r,p,salt_hex,digest_hex=encoded.split("$")
        if algorithm!="scrypt": return False
        actual=hashlib.scrypt(password.encode(),salt=bytes.fromhex(salt_hex),n=int(n),r=int(r),p=int(p),dklen=len(bytes.fromhex(digest_hex)))
        return hmac.compare_digest(actual,bytes.fromhex(digest_hex))
    except (ValueError,TypeError): return False

def token_hash(value:str)->str: return hashlib.sha256(value.encode()).hexdigest()
def now_ns()->int: return time.time_ns()

@dataclass(slots=True)
class Principal:
    session: AuthSession
    role: str
    high_risk_verified: bool

    @property
    def is_admin(self)->bool: return self.role==ADMIN_ROLE

class LoginLimiter:
    def __init__(self, limit=5, window_seconds=900): self.limit=limit; self.window=window_seconds; self.failures={}
    def check(self,key:str):
        cutoff=time.monotonic()-self.window; attempts=[x for x in self.failures.get(key,[]) if x>=cutoff]; self.failures[key]=attempts
        if len(attempts)>=self.limit: raise HTTPException(429,"Too many login attempts; try again later")
    def fail(self,key:str): self.failures.setdefault(key,[]).append(time.monotonic())
    def success(self,key:str): self.failures.pop(key,None)

login_limiter=LoginLimiter()

class AuthManager:
    def __init__(self,maker:async_sessionmaker,cfg): self.maker=maker; self.cfg=cfg
    @property
    def admin_username(self): return self.cfg.admin_username or self.cfg.auth_username
    def configured(self): return bool(self.cfg.operator_username and self.admin_username and self.cfg.auth_password_hash and self.cfg.admin_password_hash)
    async def create(self,role:str,remember:bool,user_agent:str|None):
        if role not in SESSION_ROLES: raise ValueError("Unknown session role")
        raw=secrets.token_urlsafe(32); csrf=secrets.token_urlsafe(24); created=now_ns()
        lifetime=(self.cfg.auth_remember_days*86400 if remember else self.cfg.auth_session_hours*3600)*1_000_000_000
        row=AuthSession(id=secrets.token_hex(16),role=role,token_hash=token_hash(raw),csrf_hash=token_hash(csrf),created_at_ns=created,expires_at_ns=created+lifetime,last_seen_at_ns=created,remembered=remember,revoked_at_ns=None,admin_verified_until_ns=None,user_agent=(user_agent or "")[:300])
        async with self.maker() as db: db.add(row); await db.commit()
        return row,raw,csrf,int(lifetime/1_000_000_000)
    async def principal_from_token(self,raw:str|None,required_role:str|None=None,high_risk=False)->Principal:
        if not raw: raise HTTPException(401,"Authentication required")
        digest=token_hash(raw); current=now_ns()
        async with self.maker() as db:
            row=(await db.execute(select(AuthSession).where(AuthSession.token_hash==digest))).scalar_one_or_none()
            if not row or row.revoked_at_ns is not None or row.expires_at_ns<=current or row.role not in SESSION_ROLES: raise HTTPException(401,"Session expired or revoked")
            if required_role and row.role!=required_role: raise HTTPException(403,"Insufficient role")
            if high_risk and (row.role!=ADMIN_ROLE or not row.admin_verified_until_ns or row.admin_verified_until_ns<=current): raise HTTPException(403,"Recent administrator verification required")
            row.last_seen_at_ns=current; await db.commit()
        return Principal(row,row.role,bool(row.admin_verified_until_ns and row.admin_verified_until_ns>current))
    async def principal(self,request:Request,required_role:str|None=None,high_risk=False): return await self.principal_from_token(request.cookies.get(SESSION_COOKIE),required_role,high_risk)
    async def websocket_principal(self,ws:WebSocket,required_role:str=ADMIN_ROLE): return await self.principal_from_token(ws.cookies.get(SESSION_COOKIE),required_role)
    def validate_origin(self,request:Request):
        origin=request.headers.get("origin")
        if origin!=self.cfg.public_origin: raise HTTPException(403,"Origin rejected")
    def validate_csrf(self,request:Request,principal:Principal):
        cookie=request.cookies.get(CSRF_COOKIE); header=request.headers.get("x-csrf-token")
        if not cookie or not header or not hmac.compare_digest(cookie,header) or not hmac.compare_digest(token_hash(cookie),principal.session.csrf_hash): raise HTTPException(403,"CSRF validation failed")
    async def require(self,request:Request,required_role:str|None=None,csrf=False,high_risk=False):
        principal=await self.principal(request,required_role,high_risk)
        if csrf: self.validate_origin(request); self.validate_csrf(request,principal)
        return principal
    async def elevate(self,principal:Principal):
        until=now_ns()+self.cfg.admin_reauth_minutes*60*1_000_000_000
        async with self.maker() as db:
            row=await db.get(AuthSession,principal.session.id); row.admin_verified_until_ns=until; await db.commit()
        return until
    async def revoke(self,session_id:str):
        async with self.maker() as db:
            row=await db.get(AuthSession,session_id)
            if row and row.revoked_at_ns is None: row.revoked_at_ns=now_ns(); await db.commit(); return True
        return False
    async def revoke_all_except(self,current_id:str):
        async with self.maker() as db:
            rows=(await db.execute(select(AuthSession).where(AuthSession.id!=current_id,AuthSession.revoked_at_ns.is_(None)))).scalars().all()
            current=now_ns()
            for row in rows: row.revoked_at_ns=current
            await db.commit(); return len(rows)

def set_auth_cookies(response,raw,csrf,max_age):
    response.set_cookie(SESSION_COOKIE,raw,max_age=max_age,secure=True,httponly=True,samesite="strict",path="/")
    response.set_cookie(CSRF_COOKIE,csrf,max_age=max_age,secure=True,httponly=False,samesite="strict",path="/")

def clear_auth_cookies(response):
    response.delete_cookie(SESSION_COOKIE,path="/",secure=True,httponly=True,samesite="strict")
    response.delete_cookie(CSRF_COOKIE,path="/",secure=True,httponly=False,samesite="strict")
