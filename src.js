export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== '/ws') return env.ASSETS.fetch(request);
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket required', {status:426});
    if (request.headers.get('Origin') !== url.origin) return new Response('Forbidden', {status:403});
    const room = url.searchParams.get('room') || '';
    if (!/^[a-zA-Z0-9_-]{12,80}$/.test(room)) return new Response('Invalid room', {status:400});
    return env.ROOMS.get(env.ROOMS.idFromName(room)).fetch(request);
  }
};
export class VoiceRoom {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    if (this.ctx.getWebSockets().length >= 6) return new Response('방 정원 6명을 초과했습니다.', {status:429});
    const pair = new WebSocketPair();
    const id = crypto.randomUUID();
    const name = (new URL(request.url).searchParams.get('name') || '참여자').slice(0,24);
    const existing = this.ctx.getWebSockets().map(s => s.deserializeAttachment());
    pair[1].serializeAttachment({id,name,muted:false,robot:false,last:0,count:0});
    this.ctx.acceptWebSocket(pair[1]);
    pair[1].send(JSON.stringify({type:'welcome',id,peers:existing.map(({id,name,muted,robot})=>({id,name,muted,robot}))}));
    this.broadcast({type:'join',peer:{id,name,muted:false,robot:false}},pair[1]);
    return new Response(null,{status:101,webSocket:pair[0]});
  }
  broadcast(msg, except) { for (const s of this.ctx.getWebSockets()) if(s!==except) try{s.send(JSON.stringify(msg));}catch{} }
  webSocketMessage(socket, raw) {
    if (typeof raw !== 'string' || raw.length > 64000) return;
    let m; try {m=JSON.parse(raw);}catch{return;}
    const a=socket.deserializeAttachment();
    const now=Date.now(); if(now-a.last>1000){a.last=now;a.count=0;} a.count++; socket.serializeAttachment(a); if(a.count>80)return;
    if(m.type==='signal' && typeof m.to==='string') {
      const dest=this.ctx.getWebSockets().find(s=>s.deserializeAttachment().id===m.to);
      if(dest && (m.description || m.candidate)) dest.send(JSON.stringify({type:'signal',from:a.id,description:m.description,candidate:m.candidate}));
    } else if(m.type==='chat' && typeof m.text==='string') {
      this.broadcast({type:'chat',id:a.id,name:a.name,text:m.text.slice(0,1000),time:now});
    } else if(m.type==='state') {
      a.muted=!!m.muted;a.robot=!!m.robot;socket.serializeAttachment(a);
      this.broadcast({type:'state',id:a.id,muted:a.muted,robot:a.robot});
    }
  }
  webSocketClose(socket,code,reason) { const a=socket.deserializeAttachment();try{socket.close(code,reason);}catch{}this.broadcast({type:'leave',id:a.id},socket); }
  webSocketError(socket) {this.webSocketClose(socket,1011,'Connection error');}
}
