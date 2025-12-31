
/* Simple PWA cache for your app shell + runtime fallback */
const CACHE_VERSION = 'attendance-v1';
const STATIC_CACHE = `static-${CACHE_VERSION}`;
const RUNTIME_CACHE = `runtime-${CACHE_VERSION}`;

const APP_SHELL = [
  '/',                      // HTML principal
  '/static/manifest.json'   // manifest
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => ![STATIC_CACHE, RUNTIME_CACHE].includes(k))
            .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // No interceptar POST/DELETE (justificaciones, overrides)
  if (request.method !== 'GET') return;

  // Página principal: cache-first (offline friendly)
  if (url.pathname === '/') {
    event.respondWith(
      caches.match(request).then(cached =>
        cached ||
        fetch(request).then(res => {
          caches.open(STATIC_CACHE).then(cache => cache.put(request, res.clone()));
          return res;
        }).catch(() => cached)
      )
    );
    return;
  }

  // APIs dinámicas: network-first con fallback si existe caché
  const isAPI = url.pathname.startsWith('/attendance') ||
                url.pathname.startsWith('/export.xlsx') ||
                url.pathname.startsWith('/justifications_report.xlsx');

  if (isAPI) {
    event.respondWith(
      fetch(request)
        .then(res => {
          if (res.ok) {
            caches.open(RUNTIME_CACHE).then(cache => cache.put(request, res.clone()));
          }
          return res;
        })
        .catch(() => caches.match(request))
    );
    return;
  }

  // Estáticos: cache-first
  const isStatic = url.pathname.startsWith('/static/');
  if (isStatic) {
    event.respondWith(
      caches.match(request).then(cached =>
        cached ||
        fetch(request).then(res => {
          caches.open(STATIC_CACHE).then(cache => cache.put(request, res.clone()));
          return res;
        })
      )
    );
    return;
  }
});
