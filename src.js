import { buildPushPayload } from '@block65/webcrypto-web-push';
const json=(data,status=200)=>Response.json(data,{status,headers:{'Cache-Control':'no-store'}});
async function hash(token){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(token)))).map(x=>x.toString(16).padStart(2,'0')).join('');}
function endpointOK(s){try{const u=new URL(s.endpoint);return u.protocol==='https:'&&u.port===''&&['fcm.googleapis.com','updates.push.services.mozilla.com','web.push.apple.com','wns.windows.com'].some(h=>u.hostname===h||u.hostname.endsWith('.'+h))&&typeof s.keys?.p256dh==='string'&&typeof s.keys?.auth==='string';}catch{return false;}}
export default {
 async fetch(request,env){const u=new URL(request.url);
  if(u.pathname==='/api/config')return json({publicKey:env.VAPID_PUBLIC_KEY||'',ready:!!(env.VAPID_PUBLIC_KEY&&env.VAPID_PRIVATE_KEY&&env.VAPID_SUBJECT)});
  if(!['/ws','/api/push'].includes(u.pathname))return env.ASSETS.fetch(request);
  const room=u.searchParams.get('room')||'';if(!/^[a-zA-Z0-9_-]{12,80}$/.test(room))return json({error:'올바르지 않은 방 주소'},400);
  if(request.headers.get('Origin')!==u.origin)return json({error:'Forbidden'},403);
  return env.ROOMS.get(env.ROOMS.idFromName(room)).fetch(request);
 }
};
export class VoiceRoom {
 constructor(ctx,env){this.ctx=ctx;this.env=env;}
 async fetch(request){const u=new URL(request.url);
  if(u.pathname==='/api/push'){
   if(request.method!=='POST')return json({error:'POST required'},405);
   if(Number(request.headers.get('Content-Length')||0)>12000)return json({error:'Too large'},413);
   let body;try{const raw=await request.text();if(raw.length>12000)return json({error:'Too large'},413);body=JSON.parse(raw);}catch{return json({error:'Invalid JSON'},400);}
   if(!/^[a-f0-9]{64}$/.test(body.token||''))return json({error:'Invalid device token'},400);
   const key='push:'+await hash(body.token);
   if(body.action==='remove'){await this.ctx.storage.delete(key);return json({ok:true});}
   if(!this.env.VAPID_PRIVATE_KEY||!this.env.VAPID_PUBLIC_KEY||!this.env.VAPID_SUBJECT)return json({error:'관리자가 푸시 인증키를 먼저 설정해야 합니다.'},503);
   if(!endpointOK(body.subscription))return json({error:'지원하지 않는 푸시 구독 주소'},400);
   if(!(await this.ctx.storage.get(key))&&(await this.ctx.storage.list({prefix:'push:',limit:101})).size>=100)return json({error:'방 알림 구독 정원 초과'},429);
   await this.ctx.storage.put(key,{subscription:body.subscription,updated:Date.now(),origin:u.origin,room:u.searchParams.get('room')});return json({ok:true});
  }
  if(request.headers.get('Upgrade')?.toLowerCase()!=='websocket')return new Response('WebSocket required',{status:426});
  if(this.ctx.getWebSockets().length>=6)return new Response('방 정원 6명을 초과했습니다.',{status:429});
  const history=await this.ctx.storage.get('messages')||[];
  const pair=new WebSocketPair(),id=crypto.randomUUID(),name=(u.searchParams.get('name')||'참여자').slice(0,24),token=u.searchParams.get('device')||'';
  const device=/^[a-f0-9]{64}$/.test(token)?await hash(token):'';
  const existing=this.ctx.getWebSockets().map(s=>s.deserializeAttachment());
  pair[1].serializeAttachment({id,name,device,muted:false,robot:false,visible:true,last:0,count:0,chatLast:0});this.ctx.acceptWebSocket(pair[1]);
  pair[1].send(JSON.stringify({type:'welcome',id,history,peers:existing.map(({id,name,muted,robot})=>({id,name,muted,robot}))}));
  this.broadcast({type:'join',peer:{id,name,muted:false,robot:false}},pair[1]);return new Response(null,{status:101,webSocket:pair[0]});
 }
 broadcast(m,except){for(const s of this.ctx.getWebSockets())if(s!==except)try{s.send(JSON.stringify(m));}catch{}}
 async webSocketMessage(socket,raw){if(typeof raw!=='string'||raw.length>64000)return;let m;try{m=JSON.parse(raw);}catch{return;}const a=socket.deserializeAttachment(),now=Date.now();if(now-a.last>1000){a.last=now;a.count=0;}a.count++;socket.serializeAttachment(a);if(a.count>80)return;
  if(m.type==='signal'&&typeof m.to==='string'){const d=this.ctx.getWebSockets().find(s=>s.deserializeAttachment().id===m.to);if(d&&(m.description||m.candidate))d.send(JSON.stringify({type:'signal',from:a.id,description:m.description,candidate:m.candidate}));}
  else if(m.type==='chat'&&typeof m.text==='string'&&m.text.trim()){
   if(now-a.chatLast<1000)return;a.chatLast=now;socket.serializeAttachment(a);
   const msg={type:'chat',messageId:crypto.randomUUID(),id:a.id,name:a.name,text:m.text.trim().slice(0,1000),time:now};
   await this.ctx.storage.transaction(async txn=>{const all=await txn.get('messages')||[];all.push(msg);await txn.put('messages',all.slice(-200));});
   this.broadcast(msg);this.ctx.waitUntil(this.push(msg,a.device));
  }else if(m.type==='state'){a.muted=!!m.muted;a.robot=!!m.robot;if(typeof m.visible==='boolean')a.visible=m.visible;socket.serializeAttachment(a);this.broadcast({type:'state',id:a.id,muted:a.muted,robot:a.robot});}
 }
 async push(msg,sender){if(!this.env.VAPID_PRIVATE_KEY)return;const foreground=new Set(this.ctx.getWebSockets().map(s=>s.deserializeAttachment()).filter(a=>a.visible).map(a=>a.device));const subs=await this.ctx.storage.list({prefix:'push:'});const results=await Promise.allSettled([...subs].map(async([key,v])=>{
  const device=key.slice(5);if(device===sender||foreground.has(device))return;
  // No message text in the notification: reduce lock-screen disclosure.
  const data=JSON.stringify({title:'로봇 라운지',body:'채팅방에 새 메시지가 도착했습니다.',url:v.origin+'/?room='+encodeURIComponent(v.room),tag:v.room});
  const payload=await buildPushPayload({data,options:{ttl:3600}},v.subscription,{subject:this.env.VAPID_SUBJECT,publicKey:this.env.VAPID_PUBLIC_KEY,privateKey:this.env.VAPID_PRIVATE_KEY});
  const r=await fetch(v.subscription.endpoint,{...payload,redirect:'error'});if([404,410].includes(r.status))await this.ctx.storage.delete(key);else if(!r.ok)throw new Error('push HTTP '+r.status);
 }));if(results.some(r=>r.status==='rejected'))console.warn('일부 푸시 전송 실패. 자동 재시도는 하지 않습니다.');}
 webSocketClose(s,code,reason){const a=s.deserializeAttachment();try{s.close(code,reason);}catch{}this.broadcast({type:'leave',id:a.id},s);}
 webSocketError(s){this.webSocketClose(s,1011,'Connection error');}
}
