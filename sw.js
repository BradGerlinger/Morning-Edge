const CACHE='edge-value-v10-11';
self.addEventListener('install',e=>e.waitUntil((async()=>{await self.skipWaiting();})()));
self.addEventListener('activate',e=>e.waitUntil((async()=>{for(const k of await caches.keys())await caches.delete(k);await self.clients.claim();})()));
self.addEventListener('fetch',e=>{
 const u=new URL(e.request.url);
 if(u.pathname.startsWith('/api/')||u.pathname==='/sw.js')return;
 if(e.request.mode==='navigate'||e.request.destination==='document'){
   e.respondWith(fetch(new Request(e.request,{cache:'no-store'})));return;
 }
 e.respondWith(fetch(e.request,{cache:'no-store'}).catch(()=>caches.match(e.request)));
});
self.addEventListener('push',e=>{let d={title:'15 Minute Edge',body:'New V10.11 value trade',url:'/?app=v10.11'};try{if(e.data)d={...d,...e.data.json()}}catch(_){if(e.data)d.body=e.data.text()}e.waitUntil(self.registration.showNotification(d.title,{body:d.body,icon:'/assets/icon-192.png?v=10.9',badge:'/assets/icon-192.png?v=10.9',data:{url:d.url||'/?app=v10.11'},tag:d.ticker||'edge-v10',renotify:true}))});
self.addEventListener('notificationclick',e=>{e.notification.close();const u=e.notification.data?.url||'/?app=v10.11';e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>{for(const c of cs){if('focus'in c){c.navigate(u);return c.focus()}}return clients.openWindow(u)}))});
