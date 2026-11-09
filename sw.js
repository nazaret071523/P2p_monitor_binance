/* Venbot PWA shell cache 31.73.52.
 * Cache only public app-shell files/pages. Never cache API, auth, webhook or billing data.
 */
const CACHE_NAME = 'venbot-shell-31-73-52';
const SHELL = [
  '/',
  '/index.html',
  '/offline.html',
  '/privacy.html',
  '/terms.html',
  '/help.html',
  '/risk.html',
  '/advertising.html',
  '/contact.html',
  '/delete-account.html',
  '/manifest.webmanifest',
  '/icons/venbot-192.png',
  '/icons/venbot-512.png',
  '/icons/venbot-512-maskable.png',
  '/venbot-ads-config.js',
  '/venbot-ads-loader.js'
];
const PUBLIC_PAGES = new Set([
  '/', '/index.html', '/offline.html', '/privacy.html', '/terms.html',
  '/help.html', '/risk.html', '/advertising.html', '/contact.html',
  '/delete-account.html', '/delete-account', '/privacy', '/terms',
  '/advertising', '/contact', '/help', '/risk'
]);
const STATIC_SHELL = new Set([
  '/manifest.webmanifest', '/icons/venbot-192.png', '/icons/venbot-512.png',
  '/icons/venbot-512-maskable.png', '/venbot-ads-config.js',
  '/venbot-ads-loader.js'
]);

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((key) => key.startsWith('venbot-shell-') && key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/webhook') || url.pathname.startsWith('/billing') || url.pathname.startsWith('/auth/')) return;

  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request).then((response) => {
        if (response && response.ok && PUBLIC_PAGES.has(url.pathname)) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(url.pathname, copy);
            if (url.pathname === '/') cache.put('/index.html', response.clone());
          });
        }
        return response;
      }).catch(async () => {
        const cached = await caches.match(url.pathname);
        if (cached) return cached;
        if (url.pathname === '/' || url.pathname === '/index.html') {
          return (await caches.match('/index.html')) || (await caches.match('/')) || (await caches.match('/offline.html'));
        }
        return (await caches.match('/offline.html')) || Response.error();
      })
    );
    return;
  }

  if (STATIC_SHELL.has(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => cached || fetch(request).then((response) => {
        if (response && response.ok) {
          caches.open(CACHE_NAME).then((cache) => cache.put(request, response.clone()));
        }
        return response;
      }))
    );
  }
});
