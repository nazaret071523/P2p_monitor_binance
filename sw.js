/* Venbot PWA shell cache. API responses and authenticated data are NEVER cached. */
const CACHE_NAME = 'venbot-shell-31-73-52';
const SHELL = ['/', '/offline.html', '/manifest.webmanifest', '/icons/venbot-192.png', '/icons/venbot-512.png', '/icons/venbot-512-maskable.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((key) => key.startsWith('venbot-shell-') && key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/webhook') || url.pathname.startsWith('/billing')) return;

  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request).then((response) => {
        if (response && response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put('/', copy));
        }
        return response;
      }).catch(async () => (await caches.match('/')) || (await caches.match('/offline.html')))
    );
    return;
  }

  const isStaticShell = ['/manifest.webmanifest', '/offline.html', '/icons/venbot-192.png', '/icons/venbot-512.png', '/icons/venbot-512-maskable.png', '/venbot-ads-config.js', '/venbot-ads-loader.js'].includes(url.pathname);
  if (isStaticShell) {
    event.respondWith(caches.match(request).then((cached) => cached || fetch(request).then((response) => {
      if (response && response.ok) caches.open(CACHE_NAME).then((cache) => cache.put(request, response.clone()));
      return response;
    })));
  }
});
