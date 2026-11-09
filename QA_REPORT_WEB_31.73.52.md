# Venbot Web 31.73.52 — QA local

## Base y alcance
- Base de código: paquete Contabo `31.73.51`.
- Backend compatible requerido: `31.73.51`.
- Tipo: actualización frontend estática.
- Sin cambios a `bot.py`, SQL, migraciones, variables de entorno, PostgreSQL/Supabase, motores predictivos, modelos, fórmulas o históricos.

## Cambios de aplicación
- `index.html`
  - Enlaces locales del menú abren en la misma pestaña.
  - Al cambiar de símbolo Spot se limpian tarjetas/detalles anteriores antes de iniciar el fetch.
  - Se valida que el precio observado y las cuatro proyecciones sean utilizables y correspondan al símbolo solicitado.
  - Si hay error, se limpia la lectura anterior y se muestra estado de error.
  - Solicitudes del Centro Predictivo reciben secuencia; las respuestas de bancos/símbolos/períodos anteriores no pueden sobrescribir la selección más reciente.
  - Respuestas de estado adaptativo también se descartan cuando ya no corresponden a la selección actual.
  - Los filtros 7D y 30D se ocultan en P2P porque el endpoint del backend limita `history_hours` a 24. Spot conserva 24H/7D/30D.
  - Se conserva la carga de validación histórica Spot en segundo plano; no bloquea la proyección principal.
  - Tablas técnicas y de calibración adaptan su ancho en móvil y usan el desplazamiento horizontal existente, evitando comprimir las columnas hasta hacerlas ilegibles.
- `sw.js`
  - Caché shell `venbot-shell-31-73-52`.
  - Cachea cada página pública con su propia ruta en lugar de sobrescribir siempre `/`.
  - Nunca cachea endpoints API, auth, webhook o billing.
- `vercel.json`
  - `framework: null` (Other/static).
  - Lista explícita de recursos estáticos `@vercel/static`: `*.html`, `*.js`, `*.webmanifest`, `ads.txt`, `icons/**`.
  - No declara constructor Python ni regla catch-all que envíe todas las rutas a `index.html`.
  - Rewrites solo para raíz y páginas estáticas específicas.
- `delete-account.html`: v1.3.

## Pruebas ejecutadas localmente
- `python -m unittest discover -s tests -v`: **128/128 OK**.
- `python -m py_compile bot.py spot_learning.py venbot_microstructure_shadow.py`: OK (solo comprobación; backend no modificado).
- `node --check sw.js`: OK.
- `node --check venbot-ads-config.js`: OK.
- `node --check venbot-ads-loader.js`: OK.
- `node tests/test_ads_loader.js`: 3/3 OK.
- `node --check` sobre los 6 bloques JavaScript inline de `index.html`: OK.
- JSON `vercel.json` y `manifest.webmanifest`: válidos.
- Referencias a recursos locales en páginas HTML: sin recursos faltantes detectados.
- Pruebas de regresión añadidas para Vercel estático, recursos PWA, menú Android, limpieza Spot, selección de períodos y respuestas atrasadas del Centro Predictivo.

## No probado desde este entorno
- Build remoto real de Vercel / estado Production Ready.
- Navegación real en Android/PWA tras actualización del service worker.
- APIs vivas de Contabo, sesión VIP, Telegram, Binance y datos de producción.
- Comportamiento visual real en dispositivo tras el despliegue.

## Riesgo y reversión
- Este paquete modifica solo archivos estáticos públicos en la raíz del repo.
- No instalar ni extraer este ZIP en Contabo.
- Para revertir la web, usar GitHub → commit → Revert del commit web 31.73.52. No tocar la versión del backend 31.73.51.
