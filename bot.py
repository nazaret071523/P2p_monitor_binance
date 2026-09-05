import os
import io
import asyncio
import logging
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

from pydantic import BaseModel, Field

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None

import pytz
import psycopg2
import requests
import numpy as np
import xgboost as xgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import uvicorn

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("venbot")

VET = pytz.timezone("America/Caracas")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
TELEGRAM_ALERT_CHAT_ID = os.getenv("TELEGRAM_ALERT_CHAT_ID", "").strip()
COLLECT_INTERVAL_SECONDS = max(8, int(os.getenv("COLLECT_INTERVAL_SECONDS", "10")))
P2P_SCAN_ADS = min(100, max(20, int(os.getenv("P2P_SCAN_ADS", "100"))))
P2P_BANK_REFRESH_SECONDS = max(20, int(os.getenv("P2P_BANK_REFRESH_SECONDS", "30")))
MARKET_MAX_AGE_SECONDS = max(8, int(os.getenv("MARKET_MAX_AGE_SECONDS", "20")))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/free").strip()

# Orígenes separados por coma. Si no se configura, se permite cualquier origen
# sin credenciales, suficiente para un monitor público.
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "*").strip()
ALLOWED_ORIGINS = [x.strip() for x in ALLOWED_ORIGINS_RAW.split(",") if x.strip()] or ["*"]

# Solo se usan si todavía no existe ninguna lectura real.
ULTIMO_REGISTRO_VALIDO = {
    "compra": 0.0,
    "venta": 0.0,
    "timestamp": None,
}
ULTIMO_BCV_VALIDO = {
    "usd": 0.0,
    "eur": 0.0,
    "timestamp": None,
    "source": "sin_datos",
}
ULTIMO_ESTADO_TENDENCIA = None
TENDENCIA_CANDIDATA = None
TENDENCIA_CANDIDATA_CONTEO = 0
ULTIMA_ALERTA_TENDENCIA_TS = 0.0
TELEGRAM_TREND_CONFIRMATIONS = max(2, int(os.getenv("TELEGRAM_TREND_CONFIRMATIONS", "3")))
TELEGRAM_TREND_COOLDOWN_SECONDS = max(60, int(os.getenv("TELEGRAM_TREND_COOLDOWN_SECONDS", "900")))
CONFIGURACION_BANCOS = {}
telegram_app = None
_P2P_BANK_METHOD_CACHE = {"methods": {}, "expires": 0.0}
_P2P_BANK_AD_CACHE = {}
collector_task = None

# Sesiones activas: solo se conserva un identificador efímero enviado por el cliente
# y su último heartbeat. No se guardan IP, correo ni otros datos personales.
ONLINE_SESSIONS = {}
ONLINE_LOCK = threading.Lock()
ONLINE_TTL_SECONDS = max(45, int(os.getenv("ONLINE_TTL_SECONDS", "90")))

# Cachés cortas para que el monitor y la IA no repitan consultas pesadas a Supabase
# mientras el usuario está navegando. Los datos siguen siendo reales; solo se
# reutiliza durante unos segundos la misma lectura.
_ANALYSIS_CACHE = {}
_HISTORY_CACHE = {}
_AI_CONTEXT_CACHE = {"value": None, "expires": 0.0}
_CACHE_TTL_ANALYSIS = 8.0
_CACHE_TTL_HISTORY = 10.0
_CACHE_TTL_AI = 5.0
LIVE_CACHE = {"value": None, "expires": 0.0}
LIVE_LOCK = threading.Lock()


# ==========================================
# BASE DE DATOS POSTGRESQL / SUPABASE
# ==========================================
def validar_configuracion():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL no está configurada. La persistencia no funcionará.")
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN no está configurado. El bot de Telegram no iniciará.")
    if not RENDER_EXTERNAL_URL:
        logger.warning("RENDER_EXTERNAL_URL no está configurado. No se registrará webhook automáticamente.")


def obtener_conexion():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def inicializar_db():
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS muestras_p2p (
                        id BIGSERIAL PRIMARY KEY,
                        compra DOUBLE PRECISION NOT NULL,
                        venta DOUBLE PRECISION NOT NULL,
                        liquidez_score INTEGER DEFAULT 0,
                        banco TEXT DEFAULT 'GENERAL',
                        fecha TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_muestras_p2p_banco_fecha
                    ON muestras_p2p (banco, fecha DESC);
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS mercado_actual (
                        id INTEGER PRIMARY KEY,
                        compra DOUBLE PRECISION NOT NULL,
                        venta DOUBLE PRECISION NOT NULL,
                        liquidez_score INTEGER DEFAULT 0,
                        bcv_usd DOUBLE PRECISION,
                        bcv_eur DOUBLE PRECISION,
                        fuente_bcv TEXT,
                        fecha TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS usuarios_p2p (
                        telegram_id BIGINT PRIMARY KEY,
                        username TEXT,
                        estado_suscripcion TEXT DEFAULT 'no_registrado',
                        referencia_pago TEXT,
                        fecha_expiracion TIMESTAMPTZ,
                        creado_en TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        actualizado_en TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
        logger.info("Base de datos inicializada correctamente.")
    except Exception as e:
        logger.exception("Error inicializando DB: %s", e)


def guardar_muestra_db(compra, venta, liquidez_score=0, banco="GENERAL", fecha=None):
    try:
        fecha = fecha or datetime.now(VET)
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO muestras_p2p
                    (compra, venta, liquidez_score, banco, fecha)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (float(compra), float(venta), int(liquidez_score), banco, fecha),
                )
        return True
    except Exception as e:
        logger.exception("Error guardando muestra: %s", e)
        return False


def guardar_mercado_actual(compra, venta, liquidez, bcv_usd, bcv_eur, fuente_bcv):
    try:
        now = datetime.now(VET)
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mercado_actual
                    (id, compra, venta, liquidez_score, bcv_usd, bcv_eur, fuente_bcv, fecha)
                    VALUES (1, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        compra = EXCLUDED.compra,
                        venta = EXCLUDED.venta,
                        liquidez_score = EXCLUDED.liquidez_score,
                        bcv_usd = EXCLUDED.bcv_usd,
                        bcv_eur = EXCLUDED.bcv_eur,
                        fuente_bcv = EXCLUDED.fuente_bcv,
                        fecha = EXCLUDED.fecha
                    """,
                    (
                        float(compra),
                        float(venta),
                        int(liquidez),
                        float(bcv_usd) if bcv_usd else None,
                        float(bcv_eur) if bcv_eur else None,
                        fuente_bcv,
                        now,
                    ),
                )
        return True
    except Exception as e:
        logger.exception("Error guardando mercado actual: %s", e)
        return False


def obtener_mercado_actual_db():
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT compra, venta, liquidez_score, bcv_usd, bcv_eur, fuente_bcv, fecha
                    FROM mercado_actual
                    WHERE id = 1
                """)
                row = cur.fetchone()
        if not row:
            return None
        return {
            "compra": float(row[0]),
            "venta": float(row[1]),
            "liquidez": int(row[2] or 0),
            "bcv": float(row[3]) if row[3] is not None else 0.0,
            "eur": float(row[4]) if row[4] is not None else 0.0,
            "fuente_bcv": row[5] or "sin_datos",
            "fecha": row[6],
        }
    except Exception as e:
        logger.exception("Error leyendo mercado actual: %s", e)
        return None


def obtener_estadisticas_db(limit=2000, banco="GENERAL", desde: Optional[datetime] = None):
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                conditions = []
                params = []
                if banco != "GENERAL":
                    conditions.append("banco = %s")
                    params.append(banco)
                else:
                    # GENERAL debe leer solo GENERAL, no mezclar BBVA/MERCANTIL.
                    conditions.append("banco = 'GENERAL'")
                if desde is not None:
                    conditions.append("fecha >= %s")
                    params.append(desde)

                where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
                params.append(int(limit))
                cur.execute(
                    f"""
                    SELECT compra, venta, liquidez_score, fecha
                    FROM muestras_p2p
                    {where_sql}
                    ORDER BY fecha DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return list(reversed(rows))
    except Exception as e:
        logger.exception("Error obteniendo estadísticas: %s", e)
        return []


# ==========================================
# HTTP AUXILIAR
# ==========================================
HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; Venbot/2.0; +https://render.com)",
    "Accept": "application/json,text/plain,*/*",
})


def _float_positivo(value):
    try:
        x = float(value)
        return x if x > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


# ==========================================
# TASAS BCV
# ==========================================
def obtener_tasas_bcv_oficiales():
    """
    DolarAPI publica las cotizaciones oficiales BCV en:
      /v1/dolares/oficial
      /v1/euros/oficial
    Conservamos la última lectura real si la fuente falla.
    """
    global ULTIMO_BCV_VALIDO

    usd = 0.0
    eur = 0.0
    source = "DolarAPI/BCV"
    errores = []

    try:
        r = HTTP.get("https://ve.dolarapi.com/v1/dolares/oficial", timeout=8)
        r.raise_for_status()
        data = r.json()
        usd = _float_positivo(data.get("promedio")) or _float_positivo(data.get("venta")) or _float_positivo(data.get("compra"))
    except Exception as e:
        errores.append(f"USD: {e}")

    try:
        r = HTTP.get("https://ve.dolarapi.com/v1/euros/oficial", timeout=8)
        r.raise_for_status()
        data = r.json()
        eur = _float_positivo(data.get("promedio")) or _float_positivo(data.get("venta")) or _float_positivo(data.get("compra"))
    except Exception as e:
        errores.append(f"EUR: {e}")

    if usd > 0 and eur > 0:
        ULTIMO_BCV_VALIDO = {
            "usd": round(usd, 2),
            "eur": round(eur, 2),
            "timestamp": datetime.now(VET),
            "source": source,
        }
        return dict(ULTIMO_BCV_VALIDO)

    if errores:
        logger.warning("Fallo parcial/total consultando BCV: %s", " | ".join(errores))

    # Nunca inventamos una tasa. Si existe una lectura real anterior, se conserva.
    if ULTIMO_BCV_VALIDO["usd"] > 0 or ULTIMO_BCV_VALIDO["eur"] > 0:
        return dict(ULTIMO_BCV_VALIDO)

    # Intentar recuperar la última tasa persistida.
    db = obtener_mercado_actual_db()
    if db and (db["bcv"] > 0 or db["eur"] > 0):
        return {
            "usd": round(db["bcv"], 2),
            "eur": round(db["eur"], 2),
            "timestamp": db["fecha"],
            "source": db["fuente_bcv"] or "DB",
        }

    return {"usd": 0.0, "eur": 0.0, "timestamp": None, "source": "sin_datos"}


# ==========================================
# BINANCE P2P
# ==========================================
def filtrar_outliers_iqr(precios):
    if not precios or len(precios) < 4:
        return list(precios)
    arr = np.array(precios, dtype=float)
    q25, q75 = np.percentile(arr, 25), np.percentile(arr, 75)
    iqr = q75 - q25
    if iqr == 0:
        return list(precios)
    low = q25 - 1.5 * iqr
    high = q75 + 1.5 * iqr
    return [p for p in precios if low <= p <= high] or list(precios)


def calcular_vwap_con_filtro(items):
    pares = []
    for item in items:
        try:
            adv = item.get("adv") or {}
            price = _float_positivo(adv.get("price"))
            if not price:
                continue
            volume = _float_positivo(adv.get("surplusAmount"))
            if not volume:
                volume = 1.0
            pares.append((price, volume))
        except Exception:
            continue

    if not pares:
        return 0.0

    precios = [p for p, _ in pares]
    permitidos = filtrar_outliers_iqr(precios)

    # Contador para preservar anuncios con precios duplicados.
    remaining = {}
    for p in permitidos:
        remaining[p] = remaining.get(p, 0) + 1

    pv = 0.0
    vol = 0.0
    for price, volume in pares:
        if remaining.get(price, 0) <= 0:
            continue
        remaining[price] -= 1
        pv += price * volume
        vol += volume

    return pv / vol if vol > 0 else 0.0


def _ad_contiene_banco(item, banco):
    """Filtra por banco usando todos los campos visibles del anuncio."""
    if banco == "GENERAL":
        return True
    aliases = {
        "MERCANTIL": ["mercantil"],
        "PROVINCIAL": ["provincial", "bbva provincial", "bbva"],
        "BNC": ["bnc", "banco nacional de credito", "banco nacional de crédito"],
    }.get(banco, [banco.lower()])
    try:
        import json
        blob = json.dumps(item, ensure_ascii=False).lower()
    except Exception:
        blob = str(item).lower()
    return any(a in blob for a in aliases)


def _anuncio_no_verificado(item):
    """Excluye comerciantes Pro/Verificados de forma defensiva."""
    try:
        adv = item.get("adv") or {}
        advertiser = item.get("advertiser") or {}
        for obj in (item, adv, advertiser):
            if not isinstance(obj, dict):
                continue
            if obj.get("proMerchant") is True or obj.get("proMerchantAds") is True:
                return False
            if str(obj.get("userType", "")).lower() in {"merchant", "pro", "verified", "verifiedmerchant"}:
                return False
            if obj.get("merchant") is True or obj.get("verifiedMerchant") is True:
                return False
        return True
    except Exception:
        return False


def _normalizar_anuncio(item):
    """Normaliza anuncios de la API Agent tanto si vienen con adv anidado como planos."""
    if not isinstance(item, dict):
        return None
    if isinstance(item.get("adv"), dict):
        return item
    # Algunas respuestas pueden entregar los campos del anuncio en el nivel raíz.
    if any(k in item for k in ("price", "advNo", "minSingleTransAmount", "maxSingleTransAmount", "surplusAmount")):
        return {"adv": item, "advertiser": item.get("advertiser") or item.get("merchant") or {}}
    return item


def _extraer_ads_binance(body):
    """Normaliza respuestas de los endpoints públicos C2C actuales."""
    candidates = []
    if isinstance(body, dict):
        data = body.get("data")
        candidates.append(data)
        if isinstance(data, dict):
            for key in ("ads", "data", "rows", "list", "items"):
                candidates.append(data.get(key))
        for key in ("ads", "rows", "list", "items"):
            candidates.append(body.get(key))
    else:
        candidates.append(body)

    for value in candidates:
        if isinstance(value, list):
            normalized = [_normalizar_anuncio(x) for x in value]
            return [x for x in normalized if x]
    return []


def _binance_fetch_raw(trade_type, rows=P2P_SCAN_ADS):
    """Obtiene un universo amplio de anuncios P2P sin inventar paginación.

    El Agent API público limita `limit` a 20. Para alcanzar 100 anuncios usamos
    el endpoint público C2C legacy con páginas de 20 y las combinamos. Si ese
    endpoint falla, volvemos al Agent API de 20 anuncios.
    """
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.binance.com",
        "Referer": "https://www.binance.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139 Safari/537.36",
    }
    rows = min(max(int(rows), 20), 100)
    fallback_agent = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
    legacy_url = "https://www.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

    # 1) Fuente amplia: 5 páginas x 20 = hasta 100 anuncios por dirección.
    page_count = (rows + 19) // 20
    def fetch_page(page):
        payload = {
            "asset": "USDT", "fiat": "VES", "page": page, "rows": 20,
            "tradeType": trade_type, "payTypes": ["BANK"],
            "publisherType": None, "merchantCheck": False,
            "proMerchantAds": False, "shieldMerchantAds": False,
            "countries": [],
        }
        r = HTTP.post(legacy_url, json=payload, headers=headers, timeout=8)
        r.raise_for_status()
        data = _extraer_ads_binance(r.json())
        logger.info("Binance C2C página %s %s: %s anuncios", page, trade_type, len(data))
        return data

    try:
        with ThreadPoolExecutor(max_workers=min(page_count, 5)) as ex:
            pages = list(ex.map(fetch_page, range(1, page_count + 1)))
        combined = []
        seen = set()
        for page_data in pages:
            for item in page_data:
                adv = item.get("adv") or {}
                key = str(adv.get("advNo") or item.get("advNo") or id(item))
                if key not in seen:
                    seen.add(key)
                    combined.append(item)
                if len(combined) >= rows:
                    break
            if len(combined) >= rows:
                break
        if combined:
            logger.info("Binance P2P %s: %s anuncios combinados (objetivo %s)", trade_type, len(combined), rows)
            return combined
    except Exception as e:
        logger.warning("Binance C2C paginado %s falló: %s", trade_type, e)

    # 2) Respaldo Agent API actual, limitado a 20 por contrato.
    try:
        params = {
            "fiat": "VES", "asset": "USDT", "tradeType": trade_type,
            "limit": 20, "order": "price", "tradeMethodIdentifiers": "BANK",
        }
        r = HTTP.get(fallback_agent, params=params, headers=headers, timeout=8)
        r.raise_for_status()
        data = _extraer_ads_binance(r.json())
        if data:
            logger.info("Binance Agent %s: %s anuncios recibidos", trade_type, len(data))
            return data
    except Exception as e:
        logger.warning("Binance Agent %s falló: %s", trade_type, e)

    raise RuntimeError(f"No se pudieron obtener anuncios P2P para {trade_type}")

def _normalizar_texto(texto):
    import unicodedata
    valor = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(ch for ch in valor if not unicodedata.combining(ch)).lower().strip()


def _obtener_metodos_pago_ves():
    """Obtiene dinámicamente los identifiers de métodos P2P para VES.

    Binance documenta que los identifiers deben salir de trade-methods y no
    deben asumirse/hardcodearse. Se cachean brevemente para evitar consultas
    innecesarias al endpoint público.
    """
    now = time.monotonic()
    cached = _P2P_BANK_METHOD_CACHE
    if cached.get("methods") and now < cached.get("expires", 0):
        return cached["methods"]

    url = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/trade-methods"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.binance.com",
        "Referer": "https://www.binance.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139 Safari/537.36",
    }
    r = HTTP.get(url, params={"fiat": "VES"}, headers=headers, timeout=8)
    r.raise_for_status()
    body = r.json()
    data = body.get("data") if isinstance(body, dict) else body
    if isinstance(data, dict):
        for key in ("tradeMethods", "methods", "list", "items", "data"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        data = []

    methods = {}
    for method in data:
        if not isinstance(method, dict):
            continue
        identifier = str(method.get("identifier") or method.get("tradeMethodIdentifier") or "").strip()
        name = str(method.get("tradeMethodName") or method.get("name") or "").strip()
        if identifier:
            methods[identifier] = name
    if not methods:
        raise RuntimeError("Binance trade-methods VES no devolvió identifiers")

    cached["methods"] = methods
    cached["expires"] = now + P2P_BANK_REFRESH_SECONDS
    logger.info("Binance métodos P2P VES: %s", ", ".join(f"{k}={v}" for k, v in methods.items()))
    return methods


def _resolver_metodo_banco(banco):
    """Relaciona el banco con el identifier REAL devuelto por Binance."""
    aliases = {
        "MERCANTIL": ("mercantil",),
        "PROVINCIAL": ("provincial", "bbva provincial", "bbva"),
        "BNC": ("bnc", "banco nacional de credito"),
    }.get(banco, ())
    if not aliases:
        return None
    methods = _obtener_metodos_pago_ves()
    normalized_aliases = tuple(_normalizar_texto(x) for x in aliases)

    # Primero por nombre visible.
    for identifier, name in methods.items():
        haystack = _normalizar_texto(f"{name} {identifier}")
        if any(alias in haystack for alias in normalized_aliases):
            logger.info("Binance método %s -> identifier %s (%s)", banco, identifier, name or "sin nombre")
            return identifier
    return None


def _binance_fetch_bank_specific(trade_type, banco):
    """Consulta directamente los anuncios del banco mediante su identifier."""
    cache_key = (trade_type, banco)
    now = time.monotonic()
    cached = _P2P_BANK_AD_CACHE.get(cache_key)
    if cached and now < cached[0]:
        return cached[1]

    try:
        identifier = _resolver_metodo_banco(banco)
    except Exception as e:
        logger.warning("No se pudieron resolver métodos VES para %s: %s", banco, e)
        return []
    if not identifier:
        logger.warning("Binance no devolvió identifier para %s", banco)
        return []

    url = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.binance.com",
        "Referer": "https://www.binance.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139 Safari/537.36",
    }
    try:
        r = HTTP.get(url, params={
            "fiat": "VES", "asset": "USDT", "tradeType": trade_type,
            "limit": 20, "order": "price", "tradeMethodIdentifiers": identifier,
        }, headers=headers, timeout=8)
        r.raise_for_status()
        data = _extraer_ads_binance(r.json())
        _P2P_BANK_AD_CACHE[cache_key] = (now + P2P_BANK_REFRESH_SECONDS, data)
        logger.info("Binance P2P %s %s (%s): %s anuncios", banco, trade_type, identifier, len(data))
        return data
    except Exception as e:
        logger.warning("Binance P2P %s %s falló: %s", banco, trade_type, e)
        return []


def _ad_contiene_banco(item, banco_filtro):
    if banco_filtro == "GENERAL":
        return True
    try:
        blob = _normalizar_texto(json.dumps(item, ensure_ascii=False))
    except Exception:
        blob = _normalizar_texto(str(item))
    aliases = {
        "MERCANTIL": ("mercantil",),
        "PROVINCIAL": ("provincial", "bbva provincial", "bbva"),
        "BNC": ("bnc", "banco nacional de credito"),
    }
    return any(_normalizar_texto(alias) in blob for alias in aliases.get(banco_filtro, (banco_filtro.lower(),)))

def _anuncio_no_verificado(item):
    """Excluye comerciantes Pro/Verificados de forma defensiva."""
    try:
        adv = item.get("adv") or {}
        advertiser = item.get("advertiser") or item.get("merchant") or {}
        for obj in (item, adv, advertiser):
            if not isinstance(obj, dict):
                continue
            if obj.get("proMerchant") is True or obj.get("proMerchantAds") is True:
                return False
            if obj.get("merchant") is True or obj.get("verifiedMerchant") is True:
                return False
            if obj.get("isVerified") is True or obj.get("verified") is True:
                return False
            if str(obj.get("userType", "")).lower() in {"merchant", "pro", "verified", "verifiedmerchant"}:
                return False
        return True
    except Exception:
        return False


def _anuncio_cumple_monto(item, trade_type):
    """Comprueba que el anuncio pueda atender el monto de referencia."""
    target = 300000.0 if trade_type == "SELL" else 10000.0
    adv = item.get("adv") or {}
    try:
        minimo = float(adv.get("minSingleTransAmount") or adv.get("minAmount") or 0)
        maximo = float(adv.get("maxSingleTransAmount") or adv.get("maxAmount") or 0)
        if minimo and target < minimo:
            return False
        if maximo and target > maximo:
            return False
    except Exception:
        pass
    return True


def _binance_search(trade_type, banco_filtro="GENERAL"):
    raw = _binance_fetch_raw(trade_type, rows=20)
    bancos = ["MERCANTIL", "PROVINCIAL", "BNC"] if banco_filtro == "GENERAL" else [banco_filtro]
    result = []
    for banco in bancos:
        elegibles = [
            x for x in raw
            if _anuncio_no_verificado(x)
            and _anuncio_cumple_monto(x, trade_type)
            and _ad_contiene_banco(x, banco)
        ]
        result.extend(elegibles[:10])
    seen = set()
    unique = []
    for item in result:
        adv = item.get("adv") or {}
        key = str(adv.get("advNo") or item.get("advNo") or id(item))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique[:30] if banco_filtro == "GENERAL" else unique[:10]

def _filtrar_anuncios_elegibles(raw, trade_type):
    return [
        x for x in (raw or [])
        if _anuncio_no_verificado(x) and _anuncio_cumple_monto(x, trade_type)
    ]


def _seleccionar_anuncios_por_banco(raw, trade_type, banco_filtro, bank_raw=None):
    """Selecciona anuncios por banco usando identifiers reales de Binance.

    Para bancos individuales, bank_raw ya viene filtrado por el endpoint
    ad-list con el identifier oficial del método de pago. GENERAL se construye
    exclusivamente como la unión de los tres bancos, 10 anuncios elegibles
    por banco y dirección.
    """
    if banco_filtro == "GENERAL":
        result = []
        bank_raw = bank_raw or {}
        for banco in ("MERCANTIL", "PROVINCIAL", "BNC"):
            source = bank_raw.get(banco) or []
            elegibles = _filtrar_anuncios_elegibles(source, trade_type)
            result.extend(elegibles[:10])

        seen = set()
        unique = []
        for item in result:
            adv = item.get("adv") or {}
            key = str(adv.get("advNo") or item.get("advNo") or id(item))
            if key not in seen:
                seen.add(key)
                unique.append(item)
        logger.info("P2P selección GENERAL %s: %s anuncios elegibles", trade_type, len(unique[:30]))
        return unique[:30]

    source = bank_raw.get(banco_filtro) if isinstance(bank_raw, dict) else None
    if source is None:
        source = raw or []
    elegibles = _filtrar_anuncios_elegibles(source, trade_type)
    logger.info("P2P selección %s %s: %s anuncios elegibles", banco_filtro, trade_type, len(elegibles[:10]))
    return elegibles[:10]

def obtener_precios_binance_p2p(banco_filtro="GENERAL"):
    """Lee Binance P2P en vivo. Comprar=SELL y Vender=BUY."""
    global ULTIMO_REGISTRO_VALIDO
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_sell = ex.submit(_binance_fetch_raw, "SELL", P2P_SCAN_ADS)
            f_buy = ex.submit(_binance_fetch_raw, "BUY", P2P_SCAN_ADS)
            raw_sell = f_sell.result(timeout=18)
            raw_buy = f_buy.result(timeout=18)

        anuncios_compra_usuario = _seleccionar_anuncios_por_banco(raw_sell, "SELL", banco_filtro)
        anuncios_venta_usuario = _seleccionar_anuncios_por_banco(raw_buy, "BUY", banco_filtro)
        compra = calcular_vwap_con_filtro(anuncios_compra_usuario)
        venta = calcular_vwap_con_filtro(anuncios_venta_usuario)
        if compra > 0 and venta > 0:
            liquidez = len(anuncios_compra_usuario) + len(anuncios_venta_usuario)
            ULTIMO_REGISTRO_VALIDO = {"compra": compra, "venta": venta, "timestamp": datetime.now(VET)}
            logger.info("P2P %s listo: %s anuncios SELL + %s BUY", banco_filtro, len(anuncios_compra_usuario), len(anuncios_venta_usuario))
            return round(compra, 2), round(venta, 2), liquidez
    except Exception as e:
        logger.warning("Binance P2P no disponible para %s: %s", banco_filtro, e)

    if banco_filtro == "GENERAL":
        db = obtener_mercado_actual_db()
    else:
        filas = obtener_estadisticas_db(limit=1, banco=banco_filtro)
        db = None
        if filas:
            c, v, l, f = filas[0]
            db = {"compra": c, "venta": v, "liquidez": l}
    if db and db["compra"] > 0 and db["venta"] > 0:
        return round(float(db["compra"]), 2), round(float(db["venta"]), 2), int(db.get("liquidez", 0) or 0)
    # Never leak GENERAL into a selected bank. A bank failure must remain bank-specific.
    if banco_filtro == "GENERAL" and ULTIMO_REGISTRO_VALIDO["compra"] > 0 and ULTIMO_REGISTRO_VALIDO["venta"] > 0:
        return round(ULTIMO_REGISTRO_VALIDO["compra"], 2), round(ULTIMO_REGISTRO_VALIDO["venta"], 2), 0
    return 0.0, 0.0, 0

def recolectar_mercado_general():
    compra, venta, liquidez = obtener_precios_binance_p2p("GENERAL")
    tasas = obtener_tasas_bcv_oficiales()

    if compra > 0 and venta > 0:
        now = datetime.now(VET)
        guardar_muestra_db(compra, venta, liquidez, "GENERAL", now)
        guardar_mercado_actual(
            compra,
            venta,
            liquidez,
            tasas["usd"],
            tasas["eur"],
            tasas["source"],
        )

    return {
        "compra": compra,
        "venta": venta,
        "liquidez": liquidez,
        "bcv": tasas["usd"],
        "eur": tasas["eur"],
        "fuente_bcv": tasas["source"],
        "timestamp": datetime.now(VET),
    }


# ==========================================
# MOTOR QUANT
# ==========================================
def _porcentaje_cambio(base, actual):
    try:
        return ((float(actual) - float(base)) / float(base) * 100.0) if float(base) else 0.0
    except Exception:
        return 0.0


def _clasificar_spread(spread, precio):
    pct = (spread / precio * 100.0) if precio > 0 else 0.0
    if pct >= 1.50:
        return "🔴 *Spread Amplio:* ejecución más costosa."
    if pct >= 0.80:
        return "🟡 *Spread Normal:* mercado con margen intermedio."
    return "🟢 *Spread Estrecho:* mercado relativamente comprimido."


def _obtener_punto_cercano_por_horas(fechas, valores, horas):
    """Referencia temporal real para momentum, sin convertir huecos en datos ficticios."""
    if len(fechas) == 0 or len(valores) == 0:
        return None
    try:
        span = fechas[-1] - fechas[0]
        # Para etiquetar 1h/3h necesitamos haber observado al menos ~70% de esa ventana.
        if span < timedelta(hours=horas * 0.70):
            return None
        objetivo = fechas[-1] - timedelta(hours=horas)
        i = min(range(len(fechas)), key=lambda j: abs(fechas[j] - objetivo))
        distancia = abs(fechas[i] - objetivo)
        tolerancia = timedelta(minutes=max(8.0, min(45.0, horas * 60.0 * 0.35)))
        if distancia > tolerancia:
            return None
        return float(valores[i])
    except Exception:
        return None


def detectar_manipulacion_mercado(mids, spreads, actual_mid, actual_spread):
    """Detecta patrones anómalos, no afirma manipulación intencional.
    Requiere señales combinadas para reducir falsos positivos.
    """
    try:
        arr = np.asarray(mids, dtype=float)
        spr = np.asarray(spreads, dtype=float)
        if len(arr) < 8:
            return {"activa": False, "nivel": "normal", "motivos": []}
        recent = arr[-12:]
        returns = np.diff(recent) / recent[:-1] * 100.0
        typical = float(np.median(np.abs(returns))) if len(returns) else 0.0
        last_move = float(returns[-1]) if len(returns) else 0.0
        median = float(np.median(arr[-60:]))
        distance = abs(actual_mid - median) / median * 100.0 if median else 0.0
        avg_spread = float(np.median(spr[-60:])) if len(spr) else actual_spread
        spread_jump = abs(actual_spread - avg_spread) / avg_spread * 100.0 if avg_spread > 0 else 0.0
        shock_threshold = max(0.18, typical * 4.0)
        reasons = []
        score = 0
        if abs(last_move) >= shock_threshold:
            score += 1; reasons.append(f"movimiento puntual {last_move:+.3f}%")
        if distance >= max(0.30, typical * 6.0):
            score += 1; reasons.append(f"desviación {distance:.3f}% de la mediana")
        if spread_jump >= 25.0:
            score += 1; reasons.append(f"spread cambia {spread_jump:.1f}%")
        # Reversión rápida: salto y corrección en las dos últimas muestras.
        if len(returns) >= 3 and abs(returns[-2]) >= shock_threshold * 0.75 and (returns[-2] * returns[-1] < 0):
            score += 1; reasons.append("salto con reversión rápida")
        if score >= 3:
            level = "alto"
        elif score == 2:
            level = "vigilancia"
        else:
            level = "normal"
        return {"activa": score >= 2, "nivel": level, "score": score, "motivos": reasons}
    except Exception:
        return {"activa": False, "nivel": "normal", "score": 0, "motivos": []}


def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    """Motor cuantitativo único usado por Telegram, monitor y contexto de Venbot AI.

    Trabaja con timestamps reales. La salida de 7H es un escenario estadístico central,
    no una garantía: combina momentum multi-ventana, una regresión temporal reciente,
    rango/volatilidad y una corrección moderada hacia la mediana del mercado.
    """
    filas = obtener_estadisticas_db(limit=2500, banco=banco_filtro)
    total_muestras = len(filas)

    if actual_compra <= 0 or actual_venta <= 0:
        return {
            "pred_compra": 0.0, "pred_venta": 0.0,
            "pred_compra_str": "Sin datos", "pred_venta_str": "Sin datos",
            "tendencia": "⚠️ SIN DATOS DE MERCADO", "detalle_tendencia": "Sin lectura P2P válida.",
            "piso_str": "Sin datos", "techo_str": "Sin datos",
            "muestras": int(total_muestras), "liquidez_actual": int(liquidez_actual),
            "estado_comunidad": "⚠️ Sin lectura", "confianza": 0,
            "cambios": {}, "volatilidad_pct": 0.0, "spread_pct": 0.0,
            "spread_promedio": 0.0, "rango_pct": 0.0,
            "forecast_low_mid": None, "forecast_high_mid": None, "cobertura_horas": 0.0,
        }

    now = datetime.now(VET)
    compra = float(actual_compra)
    venta = float(actual_venta)
    mid_actual = (compra + venta) / 2.0

    series = []
    for c, v, _, fecha in filas:
        try:
            c, v = float(c), float(v)
            if c > 0 and v > 0 and fecha:
                dt = fecha.astimezone(VET) if getattr(fecha, "tzinfo", None) else VET.localize(fecha)
                series.append((dt, (c + v) / 2.0, c, v))
        except Exception:
            continue

    if not series or now >= max(x[0] for x in series):
        series.append((now, mid_actual, compra, venta))
    series.sort(key=lambda x: x[0])

    fechas = [x[0] for x in series]
    mids = np.asarray([x[1] for x in series], dtype=float)
    compras = np.asarray([x[2] for x in series], dtype=float)
    ventas = np.asarray([x[3] for x in series], dtype=float)
    cobertura_horas = max(0.0, (fechas[-1] - fechas[0]).total_seconds() / 3600.0) if len(fechas) > 1 else 0.0

    cambios = {}
    for horas, etiqueta in [(5/60, "5m"), (0.25, "15m"), (0.5, "30m"), (1.0, "1h"), (3.0, "3h"), (7.0, "7h")]:
        previo = _obtener_punto_cercano_por_horas(fechas, mids, horas)
        cambios[etiqueta] = round(_porcentaje_cambio(previo, mid_actual), 3) if previo is not None else None

    cutoff_7h = now - timedelta(hours=7)
    recent_idx = [i for i, dt in enumerate(fechas) if dt >= cutoff_7h]
    if len(recent_idx) < 3:
        recent_idx = list(range(max(0, len(mids) - min(len(mids), 180)), len(mids)))
    recent = mids[recent_idx] if recent_idx else mids
    recent_buy = compras[recent_idx] if recent_idx else compras
    recent_sell = ventas[recent_idx] if recent_idx else ventas

    returns = np.diff(recent) / recent[:-1] * 100.0 if len(recent) > 2 else np.array([])
    volatilidad = float(np.std(returns)) if len(returns) else 0.0
    abs_typical = float(np.median(np.abs(returns))) if len(returns) else 0.0

    # Niveles con la MISMA serie de midpoint que usa el monitor.
    support_7h = float(np.min(recent)) if len(recent) else mid_actual
    resistance_7h = float(np.max(recent)) if len(recent) else mid_actual
    median_7h = float(np.median(recent)) if len(recent) else mid_actual
    range_size_7h = max(0.0, resistance_7h - support_7h)
    rango_pct = (range_size_7h / mid_actual * 100.0) if mid_actual else 0.0
    range_position_7h = ((mid_actual - support_7h) / range_size_7h * 100.0) if range_size_7h > 0 else 50.0

    # Drift por momentum, normalizado por hora y ponderado por estabilidad temporal.
    rates = []
    for key, hours, weight in (("15m", .25, .10), ("30m", .5, .20), ("1h", 1.0, .35), ("3h", 3.0, .35)):
        val = cambios.get(key)
        if val is not None:
            rates.append((val / hours, weight))
    if rates:
        denom = sum(w for _, w in rates)
        momentum_rate_h = sum(r*w for r,w in rates) / denom
    else:
        momentum_rate_h = 0.0

    # Regresión sobre timestamps reales de hasta 7h. Aporta dirección sin asumir muestreo uniforme.
    regression_rate_h = 0.0
    regression_r2 = 0.0
    if len(recent_idx) >= 6:
        r_dates = [fechas[i] for i in recent_idx]
        x = np.asarray([(dt - r_dates[0]).total_seconds()/3600.0 for dt in r_dates], dtype=float)
        y = np.asarray([mids[i] for i in recent_idx], dtype=float)
        if len(np.unique(x)) >= 3 and float(np.ptp(x)) >= 0.25:
            try:
                slope, intercept = np.polyfit(x, y, 1)
                yhat = slope*x + intercept
                ss_res = float(np.sum((y-yhat)**2))
                ss_tot = float(np.sum((y-np.mean(y))**2))
                regression_r2 = max(0.0, min(1.0, 1.0 - ss_res/ss_tot)) if ss_tot > 0 else 0.0
                regression_rate_h = float(slope / mid_actual * 100.0) if mid_actual else 0.0
            except Exception:
                pass

    reg_weight = 0.15 + 0.30 * regression_r2
    drift_h = (1.0 - reg_weight) * momentum_rate_h + reg_weight * regression_rate_h

    # La mediana aporta una fuerza pequeña de reversión para no extrapolar ruido indefinidamente.
    mean_reversion_pct = ((median_7h - mid_actual) / mid_actual * 100.0) * 0.18 if mid_actual else 0.0
    raw_delta_pct = drift_h * 7.0 + mean_reversion_pct
    max_delta_pct = max(0.30, min(2.50, max(0.45, rango_pct * 1.10, volatilidad * 12.0)))
    delta_pct = float(np.clip(raw_delta_pct, -max_delta_pct, max_delta_pct))
    pred_mid = mid_actual * (1.0 + delta_pct / 100.0)

    spread_actual = venta - compra
    spreads = ventas - compras
    recent_spreads = spreads[recent_idx] if recent_idx else spreads
    avg_spread = float(np.median(recent_spreads)) if len(recent_spreads) else spread_actual
    spread_change_pct = ((spread_actual - avg_spread) / avg_spread * 100.0) if avg_spread > 0 else 0.0
    pred_spread = max(0.01, 0.75 * spread_actual + 0.25 * avg_spread)
    pred_compra = pred_mid - pred_spread / 2.0
    pred_venta = pred_mid + pred_spread / 2.0
    spread_pct = (spread_actual / compra * 100.0) if compra else 0.0

    # Banda orientativa del escenario central: basada en rango y ruido recientes.
    uncertainty_pct = max(0.12, min(1.50, max(rango_pct * 0.30, volatilidad * 7.5, abs_typical * 6.0)))
    forecast_low_mid = pred_mid * (1.0 - uncertainty_pct/100.0)
    forecast_high_mid = pred_mid * (1.0 + uncertainty_pct/100.0)

    c15, c1h, c3h = cambios.get("15m"), cambios.get("1h"), cambios.get("3h")
    señales = [x for x in (c15, c1h, c3h) if x is not None]
    score = drift_h
    umbral_h = max(0.008, volatilidad * 0.8, abs_typical * 1.2)
    if drift_h > umbral_h and sum(1 for x in señales if x > 0) >= max(1, len(señales)//2):
        tendencia = "🟢 TENDENCIA ALCISTA"
        detalle = "Momentum y pendiente temporal favorecen un escenario central alcista a 7H."
    elif drift_h < -umbral_h and sum(1 for x in señales if x < 0) >= max(1, len(señales)//2):
        tendencia = "🔴 TENDENCIA BAJISTA"
        detalle = "Momentum y pendiente temporal favorecen un escenario central bajista a 7H."
    elif abs(drift_h) <= umbral_h:
        tendencia = "⚪ RANGO / LATERAL"
        detalle = "La pendiente estimada permanece dentro del ruido estadístico reciente."
    else:
        tendencia = "🟡 SEÑAL MIXTA"
        detalle = "Momentum y regresión temporal todavía no confirman la misma dirección."

    # Calidad de señal: cobertura temporal + densidad + acuerdo + ajuste de regresión.
    coverage_score = min(1.0, cobertura_horas / 7.0)
    density_score = min(1.0, len(recent) / 120.0)
    if señales:
        direction = 1 if drift_h > 0 else (-1 if drift_h < 0 else 0)
        if direction:
            agreement = sum(1 for x in señales if (x > 0) == (direction > 0)) / len(señales)
        else:
            agreement = sum(1 for x in señales if abs(x) <= max(0.03, abs_typical*2)) / len(señales)
    else:
        agreement = 0.0
    confianza = int(round(100 * (0.32*coverage_score + 0.23*density_score + 0.25*agreement + 0.20*regression_r2)))
    confianza = max(15, min(92, confianza)) if len(series) >= 3 else 0

    manipulacion = detectar_manipulacion_mercado(mids, spreads, mid_actual, spread_actual)

    liquidez = int(liquidez_actual)
    if liquidez >= 40:
        estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos"
    elif liquidez >= 20:
        estado_comunidad = "🟡 Liquidez Moderada"
    else:
        estado_comunidad = "🔴 Liquidez Baja / Poca Cobertura"

    return {
        "pred_compra": round(pred_compra, 2), "pred_venta": round(pred_venta, 2),
        "pred_compra_str": f"{pred_compra:.2f} Bs", "pred_venta_str": f"{pred_venta:.2f} Bs",
        "pred_mid": round(pred_mid, 2), "forecast_low_mid": round(forecast_low_mid, 2), "forecast_high_mid": round(forecast_high_mid, 2),
        "tendencia": tendencia, "detalle_tendencia": detalle,
        "piso_str": f"{support_7h:.2f} Bs", "techo_str": f"{resistance_7h:.2f} Bs",
        "muestras": int(total_muestras), "liquidez_actual": liquidez, "estado_comunidad": estado_comunidad,
        "confianza": confianza, "cambios": cambios, "volatilidad_pct": round(volatilidad, 4),
        "spread_pct": round(spread_pct, 3), "spread_promedio": round(avg_spread, 2),
        "spread_cambio_pct": round(spread_change_pct, 2), "soporte_7h": round(support_7h, 2),
        "resistencia_7h": round(resistance_7h, 2), "mediana_7h": round(median_7h, 2),
        "posicion_rango_7h": round(range_position_7h, 1), "rango_pct": round(rango_pct, 3),
        "max_delta_pct": round(max_delta_pct, 3), "delta_7h_pct": round(delta_pct, 3),
        "regression_r2": round(regression_r2, 3), "cobertura_horas": round(cobertura_horas, 2),
    }


def generar_imagen_grafica_cuantica(filas, banco):
    if not filas or len(filas) < 5:
        return None

    compras = np.array([f[0] for f in filas], dtype=float)
    ventas = np.array([f[1] for f in filas], dtype=float)
    fechas = [f[3] for f in filas]

    window_size = min(len(compras) - 1, 5)
    X, y_c, y_v = [], [], []
    for i in range(window_size, len(compras)):
        dt_muestra = fechas[i]
        hora_feat = dt_muestra.hour if dt_muestra else 12
        X.append(list(compras[i-window_size:i]) + [float(hora_feat)])
        y_c.append(compras[i])
        y_v.append(ventas[i])

    X = np.array(X, dtype=float)
    y_c = np.array(y_c, dtype=float)
    y_v = np.array(y_v, dtype=float)

    pasos = [0, 2, 4, 6, 8]
    base = datetime.now(VET)
    tiempos = [base + timedelta(hours=h) for h in pasos]
    c_fut, v_fut = [float(compras[-1])], [float(ventas[-1])]

    if len(X):
        mc = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0, random_state=42)
        mv = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0, random_state=42)
        mc.fit(X, y_c)
        mv.fit(X, y_v)
        sim_c = list(compras[-window_size:])
        sim_v = list(ventas[-window_size:])
        for h in pasos[1:]:
            hora = float((base.hour + h) % 24)
            pc = float(mc.predict(np.array([sim_c[-window_size:] + [hora]], dtype=float))[0])
            pv = float(mv.predict(np.array([sim_v[-window_size:] + [hora]], dtype=float))[0])
            c_fut.append(round(pc, 2))
            v_fut.append(round(pv, 2))
            sim_c.append(pc)
            sim_v.append(pv)

    while len(c_fut) < len(pasos):
        c_fut.append(float(compras[-1]))
        v_fut.append(float(ventas[-1]))

    std_c = float(np.std(compras)) if len(compras) > 1 else 0.5
    std_v = float(np.std(ventas)) if len(ventas) > 1 else 0.5
    upper = [round(v + std_v * 0.8, 2) for v in v_fut]
    lower = [round(c - std_c * 0.8, 2) for c in c_fut]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    fig.patch.set_facecolor("#0b0f19")
    ax.set_facecolor("#0f172a")
    ax.grid(True, linestyle=":", alpha=0.25, color="#38bdf8")
    for spine in ax.spines.values():
        spine.set_edgecolor("#334155")

    ax.plot(fechas, ventas, color="#f59e0b", linewidth=2.2, label="Venta Real")
    ax.plot(fechas, compras, color="#10b981", linewidth=2.2, label="Compra Real")
    ax.plot(tiempos, v_fut, color="#f59e0b", linestyle="--", linewidth=2, marker="^", label="Proyección Venta")
    ax.plot(tiempos, c_fut, color="#10b981", linestyle="--", linewidth=2, marker="v", label="Proyección Compra")
    ax.fill_between(tiempos, lower, upper, color="#38bdf8", alpha=0.15, label="Canal de Volatilidad")

    ax.set_title(f"VENBOT PREDICCIONES // [{banco}]", color="#38bdf8", fontsize=10, fontweight="bold", loc="left")
    ax.set_ylabel("Tasa VES / USDT", color="#94a3b8", fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=VET))
    ax.tick_params(colors="#94a3b8", labelsize=8)
    ax.legend(loc="upper left", facecolor="#0f172a", edgecolor="#334155", labelcolor="#cbd5e1", fontsize=7)
    plt.xticks(rotation=15)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160)
    buf.seek(0)
    plt.close(fig)
    return buf


def obtener_ultimo_mercado_banco(banco):
    if banco == "GENERAL":
        return obtener_mercado_actual_db() or {}
    filas = obtener_estadisticas_db(limit=1, banco=banco)
    if not filas:
        return {}
    c, v, l, f = filas[0]
    return {"compra": float(c or 0), "venta": float(v or 0), "liquidez": int(l or 0), "fecha": f}


def calcular_analisis_monitor(banco_filtro="GENERAL"):
    """Diagnóstico único y compartido por monitor web, IA y Telegram."""
    cache_key = banco_filtro or "GENERAL"
    cached = _ANALYSIS_CACHE.get(cache_key)
    if cached and time.monotonic() < cached[0]:
        return cached[1]

    mercado = obtener_ultimo_mercado_banco(banco_filtro)
    compra = float(mercado.get("compra", 0) or 0)
    venta = float(mercado.get("venta", 0) or 0)
    liquidez = int(mercado.get("liquidez", 0) or 0)

    if compra <= 0 or venta <= 0:
        return {
            "ok": False,
            "banco": banco_filtro,
            "tactica": {"estado": "⚪ ESPERA / SIN DATOS", "detalle": "Aún no hay una lectura P2P válida."},
            "flujo": {"estado": "⚪ SIN DATOS", "detalle": "Sin lectura suficiente de liquidez y spread."},
            "niveles": {"estado": "⚪ SIN DATOS", "detalle": "Sin rango histórico suficiente."},
            "metricas": {
                "momentum_pct": 0, "volatilidad_pct": 0, "spread_pct": 0,
                "spread_promedio": 0, "spread_cambio_pct": 0, "liquidez": liquidez,
                "soporte": 0, "resistencia": 0, "mediana": 0, "posicion_rango": 0,
                "muestras": 0, "calidad_datos": 0,
            },
            "ventanas": {},
        }

    q = motor_quant_inteligente(compra, venta, liquidez, banco_filtro)
    cambios = q.get("cambios", {}) or {}
    c5 = cambios.get("5m")
    c15 = cambios.get("15m")
    c30 = cambios.get("30m")
    c1h = cambios.get("1h")
    c3h = cambios.get("3h")

    filas = obtener_estadisticas_db(limit=2000, banco=banco_filtro)
    mids = []
    spreads = []
    fechas = []
    for c, v, l, f in filas:
        try:
            c, v = float(c or 0), float(v or 0)
            if c > 0 and v > 0 and f:
                mids.append((c + v) / 2.0)
                spreads.append(v - c)
                fechas.append(f)
        except Exception:
            continue

    now = datetime.now(VET)
    current_mid = (compra + venta) / 2.0
    if not fechas or now >= max((f.astimezone(VET) if getattr(f, 'tzinfo', None) else VET.localize(f)) for f in fechas):
        mids.append(current_mid)
        spreads.append(venta - compra)
        fechas.append(now)

    # Igual que en el motor de Telegram: trabajar siempre en orden temporal.
    ordered = sorted(zip(fechas, mids, spreads), key=lambda x: x[0])
    if ordered:
        fechas = [x[0] for x in ordered]
        mids = [x[1] for x in ordered]
        spreads = [x[2] for x in ordered]

    arr = np.asarray(mids, dtype=float)
    recent = arr[-min(len(arr), 420):]
    support = float(q.get("soporte_7h", current_mid) or current_mid)
    resistance = float(q.get("resistencia_7h", current_mid) or current_mid)
    median = float(q.get("mediana_7h", current_mid) or current_mid)
    range_size = max(0.0, resistance - support)
    range_pos = float(q.get("posicion_rango_7h", 50.0) or 50.0)

    spread_actual = venta - compra
    avg_spread = float(np.mean(spreads[-min(len(spreads), 420):])) if spreads else spread_actual
    spread_change_pct = ((spread_actual - avg_spread) / avg_spread * 100.0) if avg_spread > 0 else 0.0
    spread_pct = (spread_actual / compra * 100.0) if compra > 0 else 0.0

    # Umbral adaptativo: se basa en el ruido observado, no en ±0.35% fijo.
    returns = np.diff(recent) / recent[:-1] * 100.0 if len(recent) > 2 else np.array([])
    vol = float(np.std(returns)) if len(returns) else float(q.get("volatilidad_pct", 0) or 0)
    abs_typical = float(np.median(np.abs(returns))) if len(returns) else 0.0
    threshold = max(0.025, vol * 2.2, abs_typical * 2.5)

    aligned_up = sum(1 for x in (c15, c30, c1h) if x is not None and x > threshold)
    aligned_down = sum(1 for x in (c15, c30, c1h) if x is not None and x < -threshold)
    score_values = [x for x in (c15, c30, c1h) if x is not None]
    score = (0.45 * (c15 or 0) + 0.30 * (c30 or 0) + 0.25 * (c1h or 0)) if score_values else 0.0

    manipulacion = q.get("manipulacion") or {"activa": False, "nivel": "normal", "motivos": []}
    if manipulacion.get("nivel") == "alto":
        tactica_estado = "🚨 VIGILANCIA · MOVIMIENTO ANÓMALO"
        tactica_detail = "Posible anomalía de mercado: " + "; ".join(manipulacion.get("motivos", [])[:3]) + ". No implica manipulación intencional."
    elif spread_pct >= 1.50:
        tactica_estado = "🟠 EJECUCIÓN COSTOSA"
        tactica_detail = f"El spread está en {spread_pct:.2f}%; el coste de ejecución es elevado."
    elif aligned_up >= 2 and score > threshold:
        tactica_estado = "🟢 MOMENTO FAVORABLE"
        tactica_detail = f"Impulso alcista confirmado en varias ventanas; lectura corta {score:+.2f}%."
    elif aligned_down >= 2 and score < -threshold:
        tactica_estado = "🟡 DEFENSIVA"
        tactica_detail = f"Presión bajista confirmada en varias ventanas; lectura corta {score:+.2f}%."
    elif aligned_up and aligned_down:
        tactica_estado = "🟡 SEÑAL MIXTA"
        tactica_detail = "Las ventanas recientes están divididas; conviene esperar confirmación."
    else:
        tactica_estado = "⚪ ESPERA / RANGO"
        tactica_detail = f"El movimiento reciente está dentro del ruido observado ({threshold:.3f}% de umbral adaptativo)."

    if liquidez >= 40 and spread_change_pct > 8:
        flujo_estado = "⚡ FLUJO EXPANSIVO"
        flujo_detail = f"Liquidez alta y spread {spread_change_pct:+.1f}% sobre su media reciente."
    elif liquidez <= 15 or spread_change_pct < -8:
        flujo_estado = "🟡 FLUJO CONTRAÍDO"
        flujo_detail = f"La cobertura o el spread reciente sugieren menor actividad relativa ({spread_change_pct:+.1f}%)."
    else:
        flujo_estado = "🟢 FLUJO NORMAL"
        flujo_detail = f"Actividad operativa estable; spread {spread_change_pct:+.1f}% vs. media reciente."

    if range_size <= 0:
        niveles_estado = "⚖️ ZONA MEDIA"
        niveles_detail = "No hay rango suficiente para separar soporte y resistencia."
    elif range_pos >= 80:
        niveles_estado = "🎯 CERCA DE RESISTENCIA"
        niveles_detail = f"Precio en {range_pos:.0f}% del rango reciente; resistencia {resistance:.2f} Bs."
    elif range_pos <= 20:
        niveles_estado = "🛡️ CERCA DE SOPORTE"
        niveles_detail = f"Precio en {range_pos:.0f}% del rango reciente; soporte {support:.2f} Bs."
    else:
        niveles_estado = "⚖️ ZONA MEDIA"
        niveles_detail = f"Precio en {range_pos:.0f}% del rango reciente, entre soporte y resistencia."

    # Calidad: continuidad + cantidad + acuerdo entre ventanas.
    available = [x for x in (c5, c15, c30, c1h) if x is not None]
    agreement = max(aligned_up, aligned_down, len(available) - aligned_up - aligned_down) / max(1, len(available))
    continuity = min(1.0, len(arr) / 420.0)
    quality = int(round(100 * (0.55 * continuity + 0.45 * agreement))) if available else 0

    result = {
        "ok": True,
        "banco": banco_filtro,
        "tactica": {"estado": tactica_estado, "detalle": tactica_detail},
        "flujo": {"estado": flujo_estado, "detalle": flujo_detail},
        "niveles": {"estado": niveles_estado, "detalle": niveles_detail},
        "tendencia": q.get("tendencia"),
        "detalle_tendencia": q.get("detalle_tendencia"),
        "manipulacion": manipulacion,
        "proyeccion_7h": {
            "compra": q.get("pred_compra"),
            "venta": q.get("pred_venta"),
            "compra_str": q.get("pred_compra_str"),
            "venta_str": q.get("pred_venta_str"),
            "mid": q.get("pred_mid"),
            "rango_mid_min": q.get("forecast_low_mid"),
            "rango_mid_max": q.get("forecast_high_mid"),
            "delta_pct": q.get("delta_7h_pct"),
            "calidad": q.get("confianza"),
            "cobertura_horas": q.get("cobertura_horas", 0.0),
            "soporte_7h": q.get("soporte_7h"),
            "resistencia_7h": q.get("resistencia_7h"),
            "mediana_7h": q.get("mediana_7h"),
            "posicion_rango_7h": q.get("posicion_rango_7h"),
            "muestras": q.get("muestras", 0),
            "regression_r2": q.get("regression_r2", 0.0),
        },
        "ventanas": {k: cambios.get(k) for k in ("5m", "15m", "30m", "1h", "3h", "7h")},
        "metricas": {
            "momentum_pct": round(score, 3),
            "momentum_5m_pct": round(c5, 3) if c5 is not None else None,
            "momentum_15m_pct": round(c15, 3) if c15 is not None else None,
            "momentum_30m_pct": round(c30, 3) if c30 is not None else None,
            "momentum_1h_pct": round(c1h, 3) if c1h is not None else None,
            "momentum_3h_pct": round(c3h, 3) if c3h is not None else None,
            "volatilidad_pct": round(vol, 4),
            "umbral_adaptativo_pct": round(threshold, 4),
            "spread_pct": round(spread_pct, 3),
            "spread_promedio": round(avg_spread, 2),
            "spread_cambio_pct": round(spread_change_pct, 2),
            "liquidez": liquidez,
            "soporte": round(support, 2),
            "resistencia": round(resistance, 2),
            "mediana": round(median, 2),
            "posicion_rango": round(range_pos, 1),
            "rango_pct": round((range_size / current_mid * 100.0) if current_mid else 0.0, 3),
            "muestras": len(arr),
            "calidad_datos": int(q.get("confianza", quality) or quality),
        },
    }
    _ANALYSIS_CACHE[cache_key] = (time.monotonic() + _CACHE_TTL_ANALYSIS, result)
    return result


# ==========================================
# TELEGRAM
# ==========================================
def obtener_teclado_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Análisis P2P y Proyección 7H", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("💎 Muestra los plans VIP y PREMIUM", callback_data="cmd_suscribir")],
        [InlineKeyboardButton("📊 Gráfica de Protección Temporal", callback_data="cmd_grafica")],
        [InlineKeyboardButton("🏦 Configurar Filtro de Bancos", callback_data="cmd_bancos")],
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = "🦜 *VENBOT PREDICCIONES - SISTEMA DE PROTECCIÓN*\nSelecciona una opción del menú táctico:"
    if update.callback_query:
        await update.callback_query.answer()
        if update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())


async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if update.callback_query:
        await update.callback_query.answer()

    banco = CONFIGURACION_BANCOS.get(chat_id, "GENERAL")
    # Telegram debe usar la misma captura persistida que alimenta el monitor.
    # Así no genera otra consulta Binance ni queda desincronizado del frontend.
    mercado = await asyncio.to_thread(obtener_ultimo_mercado_banco, banco)
    c_real = float(mercado.get("compra", 0) or 0)
    v_real = float(mercado.get("venta", 0) or 0)
    liquidez = int(mercado.get("liquidez", 0) or 0)
    if c_real <= 0 or v_real <= 0:
        c_real, v_real, liquidez = await asyncio.to_thread(obtener_precios_binance_p2p, banco)
    datos = await asyncio.to_thread(motor_quant_inteligente, c_real, v_real, liquidez, banco)

    ahora = datetime.now(VET)
    objetivo = ahora + timedelta(hours=7)
    spread_actual = v_real - c_real if c_real and v_real else 0.0
    spread_pct = (spread_actual / c_real * 100.0) if c_real else 0.0
    analisis = _clasificar_spread(spread_actual, c_real)

    cambios = datos.get("cambios", {})
    def fmt_change(key):
        val = cambios.get(key)
        if val is None:
            return "n/d"
        return f"{val:+.3f}%"

    texto = (
        f"🦜 *VENBOT PREDICCIONES // TENDENCIA P2P*\n"
        f"🏦 *Filtro Banco:* `{banco}`\n"
        f"──────────────────────────────\n"
        f"⏱ *Ventana de proyección:* `{ahora.strftime('%I:%M %p')}` ➔ `{objetivo.strftime('%I:%M %p')}`\n\n"
        f"📊 *PRECIOS P2P ACTUALES (VWAP)*\n"
        f"• Comprar USDT: `{c_real:.2f} Bs`\n"
        f"• Vender USDT: `{v_real:.2f} Bs`\n"
        f"• Spread: `{spread_actual:.2f} Bs` · `{spread_pct:.2f}%`\n\n"
        f"🔍 *DIAGNÓSTICO DE MERCADO*\n"
        f"• {analisis}\n"
        f"• Liquidez: `{datos['estado_comunidad']}`\n"
        f"• Muestras: `{datos['muestras']}`\n"
        f"• Canal 7H: `{datos.get('soporte_7h', datos['piso_str'])}` / `{datos.get('resistencia_7h', datos['techo_str'])}`\n"
        f"• Volatilidad reciente: `{datos['volatilidad_pct']:.3f}%`\n\n"
        f"📈 *MOMENTUM MULTI-TEMPORAL*\n"
        f"• 5 min: `{fmt_change('5m')}`\n"
        f"• 15 min: `{fmt_change('15m')}`\n"
        f"• 30 min: `{fmt_change('30m')}`\n"
        f"• 1 hora: `{fmt_change('1h')}`\n"
        f"• 3 horas: `{fmt_change('3h')}`\n\n"
        f"🧭 *NIVELES 7H*\n"
        f"• Soporte: `{datos.get('soporte_7h', datos['piso_str'])}`\n"
        f"• Resistencia: `{datos.get('resistencia_7h', datos['techo_str'])}`\n"
        f"• Posición del rango: `{datos.get('posicion_rango_7h', 50):.1f}%`\n\n"
        f"🔮 *PROYECCIÓN CUANTITATIVA (7H)*\n"
        f"• Compra estimada: `{datos['pred_compra_str']}`\n"
        f"• Venta estimada: `{datos['pred_venta_str']}`\n"
        f"• Tendencia: `{datos['tendencia']}`\n"
        f"• Calidad de señal: `{datos['confianza']}%`\n"
        f"• Rango estimado midpoint: `{datos.get('forecast_low_mid', 0):.2f} – {datos.get('forecast_high_mid', 0):.2f} Bs`\n"
        f"• Lectura: {datos.get('detalle_tendencia', '')}\n\n"
        f"⚠️ _La proyección es estadística y sirve como referencia; no garantiza el precio futuro._"
    )
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))


async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if update.callback_query:
        await update.callback_query.answer()
    banco = CONFIGURACION_BANCOS.get(chat_id, "GENERAL")
    filas = await asyncio.to_thread(obtener_estadisticas_db, 35, banco)
    buf = await asyncio.to_thread(generar_imagen_grafica_cuantica, filas, banco)
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    if buf:
        await context.bot.send_photo(
            chat_id=chat_id, photo=buf,
            caption=f"📊 *Venbot Predicciones [{banco}]*",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado)
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Datos insuficientes para [{banco}].",
            reply_markup=InlineKeyboardMarkup(teclado)
        )


async def cmd_bancos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    teclado = [
        [InlineKeyboardButton("Mercantil", callback_data="banco_MERCANTIL"), InlineKeyboardButton("Provincial", callback_data="banco_PROVINCIAL")],
        [InlineKeyboardButton("BNC", callback_data="banco_BNC"), InlineKeyboardButton("3 Bancos", callback_data="banco_GENERAL")],
        [InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")],
    ]
    chat_id = update.effective_chat.id
    banco_actual = CONFIGURACION_BANCOS.get(chat_id, "GENERAL")
    texto = f"🏦 *Selecciona el banco de arbitraje:*\nActualmente: `{banco_actual}`"
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))


async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    texto = "💎 *Planes VIP y Premium Disponibles*\nAcceso prioritario a funciones avanzadas."
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    chat_id = update.effective_chat.id
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))


async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data
    chat_id = update.effective_chat.id
    if data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_bancos":
        await cmd_bancos(update, context)
    elif data == "cmd_suscribir":
        await cmd_suscribir(update, context)
    elif data == "cmd_menu":
        await start(update, context)
    elif data.startswith("banco_"):
        banco = data.replace("banco_", "", 1)
        CONFIGURACION_BANCOS[chat_id] = banco
        await query.answer(f"Filtro cambiado a {banco}")
        await cmd_prediccion(update, context)


# ==========================================
# RECOLECCIÓN
# ==========================================
async def tarea_recoleccion_automatica():
    global ULTIMO_ESTADO_TENDENCIA
    while True:
        try:
            # Una sola captura amplia por dirección. De ella salen GENERAL y los
            # tres bancos, evitando 4-8 consultas redundantes por ciclo.
            with ThreadPoolExecutor(max_workers=4) as ex:
                f_sell = ex.submit(_binance_fetch_raw, "SELL", P2P_SCAN_ADS)
                f_buy = ex.submit(_binance_fetch_raw, "BUY", P2P_SCAN_ADS)
                f_bank_sell = ex.submit(lambda: {b: _binance_fetch_bank_specific("SELL", b) for b in ("MERCANTIL", "PROVINCIAL", "BNC")})
                f_bank_buy = ex.submit(lambda: {b: _binance_fetch_bank_specific("BUY", b) for b in ("MERCANTIL", "PROVINCIAL", "BNC")})
                raw_sell = await asyncio.to_thread(f_sell.result, 20)
                raw_buy = await asyncio.to_thread(f_buy.result, 20)
                bank_sell = await asyncio.to_thread(f_bank_sell.result, 20)
                bank_buy = await asyncio.to_thread(f_bank_buy.result, 20)

            def calcular_desde_raw(banco):
                if banco == "GENERAL":
                    sell = _seleccionar_anuncios_por_banco(raw_sell, "SELL", banco, bank_sell)
                    buy = _seleccionar_anuncios_por_banco(raw_buy, "BUY", banco, bank_buy)
                else:
                    sell = _seleccionar_anuncios_por_banco(raw_sell, "SELL", banco, bank_sell)
                    buy = _seleccionar_anuncios_por_banco(raw_buy, "BUY", banco, bank_buy)
                c = calcular_vwap_con_filtro(sell)
                v = calcular_vwap_con_filtro(buy)
                return c, v, len(sell) + len(buy)

            mercado = None
            resultados = {}
            for banco in ("GENERAL", "MERCANTIL", "PROVINCIAL", "BNC"):
                c, v, l = calcular_desde_raw(banco)
                resultados[banco] = (c, v, l)
                logger.info("P2P %s listo: %.2f compra / %.2f venta / %s anuncios", banco, c, v, l)
                if c > 0 and v > 0:
                    await asyncio.to_thread(guardar_muestra_db, c, v, l, banco)
                if banco == "GENERAL":
                    tasas = await asyncio.to_thread(obtener_tasas_bcv_oficiales)
                    now = datetime.now(VET)
                    await asyncio.to_thread(guardar_mercado_actual, c, v, l, tasas["usd"], tasas["eur"], tasas["source"])
                    mercado = {"compra": c, "venta": v, "liquidez": l, "bcv": tasas["usd"], "eur": tasas["eur"], "fuente_bcv": tasas["source"], "timestamp": now}

            if mercado and mercado["compra"] > 0 and mercado["venta"] > 0:
                datos = await asyncio.to_thread(motor_quant_inteligente, mercado["compra"], mercado["venta"], mercado["liquidez"], "GENERAL")
                tendencia = datos["tendencia"]
                manip = datos.get("manipulacion") or {}

                # Histeresis/confirmación: un cambio de tendencia debe repetirse en
                # varias capturas antes de avisar. Así se evita RANGO↔ALCISTA cada ciclo.
                global TENDENCIA_CANDIDATA, TENDENCIA_CANDIDATA_CONTEO, ULTIMA_ALERTA_TENDENCIA_TS, ULTIMO_ESTADO_TENDENCIA
                if tendencia != TENDENCIA_CANDIDATA:
                    TENDENCIA_CANDIDATA = tendencia
                    TENDENCIA_CANDIDATA_CONTEO = 1
                else:
                    TENDENCIA_CANDIDATA_CONTEO += 1

                ahora_mono = time.monotonic()
                tendencia_confirmada = (
                    TENDENCIA_CANDIDATA_CONTEO >= TELEGRAM_TREND_CONFIRMATIONS
                    and tendencia != ULTIMO_ESTADO_TENDENCIA
                )
                cooldown_ok = (ahora_mono - ULTIMA_ALERTA_TENDENCIA_TS) >= TELEGRAM_TREND_COOLDOWN_SECONDS
                debe_alertar_tendencia = tendencia_confirmada and cooldown_ok
                debe_alertar_anomalia = bool(manip.get("activa"))

                if TELEGRAM_ALERT_CHAT_ID and telegram_app and (debe_alertar_tendencia or debe_alertar_anomalia):
                    if debe_alertar_tendencia:
                        ULTIMO_ESTADO_TENDENCIA = tendencia
                        ULTIMA_ALERTA_TENDENCIA_TS = ahora_mono
                    tipo = "🚨 ALERTA DE MOVIMIENTO ANÓMALO" if debe_alertar_anomalia else "🚨 ALERTA PROACTIVA DE MERCADO P2P"
                    motivos = "; ".join(manip.get("motivos", [])[:3])
                    extra = f"\n• Anomalía: `{motivos}`" if motivos else ""
                    await telegram_app.bot.send_message(chat_id=TELEGRAM_ALERT_CHAT_ID, text=(f"{tipo}\n• Tendencia: `{tendencia}`\n• Comprar USDT: `{mercado['compra']:.2f} Bs`\n• Vender USDT: `{mercado['venta']:.2f} Bs`" + extra + "\n• El cambio de tendencia requiere confirmación y cooldown para evitar falsas oscilaciones." if not debe_alertar_anomalia else f"{tipo}\n• Tendencia: `{tendencia}`\n• Comprar USDT: `{mercado['compra']:.2f} Bs`\n• Vender USDT: `{mercado['venta']:.2f} Bs`" + extra + "\n• La anomalía es una señal estadística y no prueba manipulación intencional."), parse_mode="Markdown")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Error en tarea autónoma: %s", e)
        await asyncio.sleep(COLLECT_INTERVAL_SECONDS)


# ==========================================
# FASTAPI
# ==========================================
app = FastAPI(title="Venbot API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "status": "ok",
        "service": "Venbot",
        "api": "/api/precios",
        "history": "/api/history?period=1d",
    }


@app.get("/api/health")
def health():
    mercado = obtener_mercado_actual_db()
    return {
        "status": "ok",
        "database": bool(mercado) if DATABASE_URL else False,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
        "ai_configured": bool((GEMINI_API_KEY and genai) or OPENROUTER_API_KEY),
        "ai_model": GEMINI_MODEL if GEMINI_API_KEY else (OPENROUTER_MODEL if OPENROUTER_API_KEY else None),
        "timestamp": datetime.now(VET).isoformat(),
    }


def _limpiar_sesiones_online():
    ahora = time.monotonic()
    with ONLINE_LOCK:
        vencidas = [sid for sid, ts in ONLINE_SESSIONS.items() if ahora - ts > ONLINE_TTL_SECONDS]
        for sid in vencidas:
            ONLINE_SESSIONS.pop(sid, None)
        return len(ONLINE_SESSIONS)


class OnlineHeartbeat(BaseModel):
    session_id: str = Field(min_length=16, max_length=80, pattern=r"^[A-Za-z0-9_-]+$")


@app.post("/api/online/heartbeat")
def online_heartbeat(payload: OnlineHeartbeat):
    with ONLINE_LOCK:
        ONLINE_SESSIONS[payload.session_id] = time.monotonic()
    return {"ok": True, "online": _limpiar_sesiones_online()}


@app.get("/api/online")
def online_count():
    return {"ok": True, "online": _limpiar_sesiones_online(), "ttl_seconds": ONLINE_TTL_SECONDS}


LEGAL_PAGE_STYLE = """
body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;background:#070a11;color:#e2e8f0;margin:0;padding:32px;line-height:1.6}
main{max-width:850px;margin:auto;background:#0f172a;border:1px solid rgba(255,255,255,.08);border-radius:20px;padding:28px}
h1,h2{color:#6ee7b7} a{color:#38bdf8} .muted{color:#94a3b8;font-size:.9rem}
"""

def _legal_html(title, body):
    return HTMLResponse(f"<!doctype html><html lang='es'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{title} · Venbot</title><style>{LEGAL_PAGE_STYLE}</style></head><body><main><p class='muted'><a href='/'>← Venbot</a></p>{body}<hr><p class='muted'>Venbot · Mercado P2P USDT/VES · Última actualización: septiembre de 2026</p></main></body></html>")


@app.get("/legal/privacy", response_class=HTMLResponse)
def legal_privacy():
    return _legal_html("Política de privacidad", """<h1>Política de privacidad</h1><p>Venbot utiliza datos necesarios para mostrar información del mercado P2P, operar funciones solicitadas por el usuario y mejorar la seguridad y funcionamiento del servicio.</p><h2>Datos</h2><p>La aplicación puede procesar identificadores técnicos de sesión para mostrar una cantidad aproximada de usuarios conectados. Este contador utiliza identificadores efímeros y no necesita almacenar la dirección IP.</p><p>Si se habilitan cuentas, alertas, suscripciones o servicios de terceros, se informará al usuario sobre los datos y finalidades correspondientes.</p><h2>Servicios de terceros</h2><p>Venbot puede utilizar Binance P2P, fuentes oficiales del BCV, servicios de alojamiento, analítica y proveedores publicitarios. Estos servicios pueden tratar datos conforme a sus propias políticas.</p><h2>Publicidad</h2><p>La versión gratuita puede mostrar publicidad mediante proveedores como Google AdMob. Las preferencias de anuncios y las solicitudes de consentimiento se gestionarán según la normativa aplicable.</p><h2>Contacto</h2><p>Para solicitudes relacionadas con privacidad, utiliza el canal oficial de soporte publicado dentro de la aplicación.</p>""")

@app.get("/legal/terms", response_class=HTMLResponse)
def legal_terms():
    return _legal_html("Términos de uso", """<h1>Términos de uso</h1><p>Venbot es una herramienta informativa para consultar precios P2P, referencias oficiales y análisis estadístico.</p><h2>Sin asesoramiento financiero</h2><p>La información mostrada no constituye asesoramiento financiero, oferta de compra o venta ni garantía de resultados. El usuario toma sus propias decisiones y debe verificar las condiciones de cada anuncio antes de operar.</p><h2>Sin custodia</h2><p>Venbot no custodia fondos ni ejecuta operaciones P2P en nombre del usuario.</p><h2>Disponibilidad</h2><p>Los precios, anuncios y fuentes externas pueden cambiar, quedar temporalmente sin servicio o contener retrasos. El servicio puede usar la última lectura real disponible cuando una fuente externa no responde.</p>""")

@app.get("/blog", response_class=HTMLResponse)
def blog_page():
    return _legal_html("Blog", """<h1>Blog Venbot</h1><h2>Cómo leer el mercado P2P</h2><p>Aprende a interpretar compra, venta, spread, liquidez, tendencia y niveles del mercado USDT/VES.</p><h2>Guía de seguridad P2P</h2><p>Verifica siempre el nombre del comerciante, método de pago, límites del anuncio y condiciones antes de liberar fondos.</p><h2>Próximamente</h2><p>Publicaremos artículos sobre gestión de riesgo, lectura de gráficos y uso responsable de herramientas de análisis.</p>""")

@app.get("/tutorial", response_class=HTMLResponse)
def tutorial_page():
    return _legal_html("Tutorial Venbot", """<h1>Tutorial de Venbot</h1><h2>1. Mercado</h2><p>Consulta Comprar USDT y Vender USDT, el spread y las tasas oficiales.</p><h2>2. Gráfico</h2><p>Selecciona 5m, 15m, 30m, 1h o 1D para estudiar la evolución histórica disponible.</p><h2>3. Análisis</h2><p>Revisa tendencia, flujo, niveles y el escenario estadístico de 7 horas.</p><h2>4. Chat IA</h2><p>Puedes hacer preguntas generales o pedir una lectura del mercado. En preguntas P2P, Venbot proporciona a la IA el contexto real disponible.</p><h2>5. Alertas</h2><p>Configura una alerta local para recibir una notificación cuando el precio objetivo se alcance en este navegador.</p>""")


def _mercado_desactualizado(mercado):
    if not mercado or not mercado.get("fecha"):
        return True
    fecha = mercado["fecha"]
    if fecha.tzinfo is None:
        fecha = VET.localize(fecha)
    age = (datetime.now(VET) - fecha.astimezone(VET)).total_seconds()
    return age > MARKET_MAX_AGE_SECONDS


@app.get("/api/precios")
def obtener_precios_api(refresh: bool = Query(False)):
    """Entrega la última captura real persistida. El recolector mantiene el
    mercado actualizado; solo se fuerza una captura si no existe o está vieja.
    """
    with LIVE_LOCK:
        mercado = obtener_mercado_actual_db() or {}
        stale = _mercado_desactualizado(mercado)
        if (not mercado or stale) and refresh:
            try:
                recolectar_mercado_general()
                mercado = obtener_mercado_actual_db() or mercado
            except Exception as e:
                logger.warning("Refresh P2P manual falló: %s", e)
        if not mercado:
            return {"ok": False, "error": "Todavía no existe una lectura real."}
        fecha = mercado.get("fecha") or datetime.now(VET)
        if fecha.tzinfo is None:
            fecha = VET.localize(fecha)
        age = max(0.0, (datetime.now(VET) - fecha.astimezone(VET)).total_seconds())
        compra = float(mercado.get("compra", 0) or 0)
        venta = float(mercado.get("venta", 0) or 0)
        spread = round(venta - compra, 2)
        result = {
            "ok": compra > 0 and venta > 0, "compra": round(compra, 2), "venta": round(venta, 2),
            "buy": round(compra, 2), "sell": round(venta, 2), "spread": spread,
            "spread_pct": round((spread/compra)*100, 2) if compra else 0.0,
            "bcv": round(float(mercado.get("bcv", 0) or 0), 2), "eur": round(float(mercado.get("eur", 0) or 0), 2),
            "liquidez": int(mercado.get("liquidez", 0) or 0), "fuente_bcv": mercado.get("fuente_bcv") or "DB",
            "timestamp": fecha.astimezone(VET).isoformat(), "age_seconds": round(age, 1),
            "stale": age > MARKET_MAX_AGE_SECONDS,
            "source": "Binance P2P live" if age <= MARKET_MAX_AGE_SECONDS else "última lectura real",
        }
        LIVE_CACHE["value"] = result
        LIVE_CACHE["expires"] = time.monotonic() + 2.0
        return result


@app.get("/api/market")
def obtener_precios_market_alias():
    return obtener_precios_api(False)


@app.get("/api/analysis")
def obtener_analysis_api():
    return calcular_analisis_monitor("GENERAL")


@app.get("/api/history")
def obtener_history(period: str = Query("5m", pattern="^(5m|15m|30m|1h|1d)$")):
    """Histórico ligero: el servidor entrega OHLC ya agregado para no cargar el móvil."""
    cached = _HISTORY_CACHE.get(period)
    if cached and time.monotonic() < cached[0]:
        return cached[1]

    # ~90-170 velas visibles por marco: suficiente detalle sin miles de puntos.
    configuracion = {
        "5m": (12, 1800, 5 * 60),
        "15m": (36, 2400, 15 * 60),
        "30m": (72, 2500, 30 * 60),
        "1h": (7 * 24, 2500, 60 * 60),
        "1d": (90 * 24, 5000, 24 * 60 * 60),
    }
    horas, limite, bucket_seconds = configuracion[period]
    desde = datetime.now(VET) - timedelta(hours=horas)
    filas = obtener_estadisticas_db(limit=limite, banco="GENERAL", desde=desde)

    puntos = []
    for c, v, l, f in filas:
        if not f:
            continue
        try:
            dt = VET.localize(f) if f.tzinfo is None else f.astimezone(VET)
            compra, venta = float(c or 0), float(v or 0)
            if compra > 0 and venta > 0:
                puntos.append((dt, compra, venta, int(l or 0)))
        except Exception:
            continue

    mercado = obtener_mercado_actual_db() or {}
    mc, mv = float(mercado.get("compra", 0) or 0), float(mercado.get("venta", 0) or 0)
    mf = mercado.get("fecha")
    if mc > 0 and mv > 0:
        live_dt = (VET.localize(mf) if mf and mf.tzinfo is None else mf.astimezone(VET)) if mf else datetime.now(VET)
        puntos.append((live_dt, mc, mv, int(mercado.get("liquidez", 0) or 0)))

    puntos.sort(key=lambda x: x[0])
    buckets = {}
    for dt, compra, venta, liq in puntos:
        epoch = int(dt.timestamp())
        key = (epoch // bucket_seconds) * bucket_seconds
        mid = (compra + venta) / 2.0
        b = buckets.get(key)
        if b is None:
            buckets[key] = {"x": key * 1000, "o": mid, "h": mid, "l": mid, "c": mid, "liquidez": liq}
        else:
            b["h"] = max(b["h"], mid); b["l"] = min(b["l"], mid); b["c"] = mid; b["liquidez"] = liq

    candles = [buckets[k] for k in sorted(buckets)]
    # Tope visual para mantener velas anchas/detalladas en móvil.
    max_candles = 170 if period != "1d" else 100
    candles = candles[-max_candles:]
    for c in candles:
        for k in ("o", "h", "l", "c"):
            c[k] = round(float(c[k]), 3)

    # data queda solo como compatibilidad y se limita mucho; el frontend v9 usa candles.
    data = [{"compra": round(c,2), "venta": round(v,2), "liquidez": l, "timestamp": dt.isoformat()} for dt,c,v,l in puntos[-300:]]
    result = {"ok": True, "period": period, "count": len(candles), "candles": candles, "data": data}
    _HISTORY_CACHE[period] = (time.monotonic() + _CACHE_TTL_HISTORY, result)
    return result


VENBOT_AI_SYSTEM = """Eres Venbot AI, un asistente conversacional avanzado en español. Eres el copiloto del usuario: puedes conversar sobre temas generales, explicar conceptos, ayudar con cálculos, planificación y razonamiento, y también analizar el mercado P2P USDT/VES cuando el usuario lo pida.

REGLAS ESTRICTAS PARA MERCADO:
1) Usa exclusivamente el CONTEXTO REAL DE VENBOT recibido en cada consulta. Nunca inventes precios, tasas, liquidez, muestras, horarios, momentum, soporte, resistencia o proyecciones.
2) Distingue siempre: dato observado, cálculo estadístico, estimación y recomendación. Una proyección nunca es un precio garantizado.
3) Comprar USDT = anuncios SELL de Binance (el usuario compra USDT). Vender USDT = anuncios BUY (el usuario vende USDT). No inviertas jamás estas etiquetas.
4) Si una ventana temporal aparece como n/d, significa que no hay datos suficientes o continuidad suficiente para calcularla; no la rellenes con 0.00% ni inventes una lectura.
5) Si los datos son insuficientes por falta de histórico, dilo claramente y usa solo lo que sí está observado.
6) Para preguntas de mercado, responde primero con una lectura breve y después con los números relevantes del contexto. No contradigas el bloque analítico de Venbot.

Mantén continuidad real con el historial y responde de forma natural y fluida. Si la consulta NO es de mercado, eres un asistente general completo: responde cualquier tema permitido sin intentar llevar la conversación a P2P. Para consultas de mercado, usa exactamente el mismo motor cuantitativo que alimenta monitor y Telegram: conclusión primero, luego 3-5 métricas y una recomendación táctica. Si te preguntan por una predicción a 7H, usa proyeccion_7h y explica que es un escenario estadístico central con rango estimado, no certeza. Evita repetir todo el contexto y no cortes una frase a mitad. No prometas ganancias ni certeza financiera."""


def _obtener_contexto_bancos_ia():
    """Lee la última muestra REAL persistida de cada banco, sin lanzar consultas nuevas a Binance."""
    bancos = {}
    for banco in ("MERCANTIL", "PROVINCIAL", "BNC"):
        filas = obtener_estadisticas_db(limit=1, banco=banco)
        if not filas:
            bancos[banco] = {"disponible": False}
            continue
        c, v, l, f = filas[0]
        bancos[banco] = {
            "disponible": True,
            "comprar_usdt_sell": round(float(c or 0), 2),
            "vender_usdt_buy": round(float(v or 0), 2),
            "spread": round(float(v or 0) - float(c or 0), 2),
            "liquidez_anuncios": int(l or 0),
            "timestamp": f.isoformat() if f else None,
        }
    return bancos


def _serializar_contexto_mercado():
    cached = _AI_CONTEXT_CACHE.get("value")
    if cached and time.monotonic() < _AI_CONTEXT_CACHE.get("expires", 0):
        return cached

    mercado = obtener_mercado_actual_db() or {}
    analisis = calcular_analisis_monitor("GENERAL")
    filas = obtener_estadisticas_db(limit=120, banco="GENERAL")
    hist = []
    for c, v, l, f in filas:
        hist.append({"compra": round(float(c),2), "venta": round(float(v),2), "liquidez": int(l or 0), "fecha": f.isoformat() if f else None})
    compra = float(mercado.get("compra",0) or 0); venta = float(mercado.get("venta",0) or 0)
    spread = venta-compra if compra and venta else 0
    result = {
        "mercado_actual": {"comprar_usdt_sell": compra, "vender_usdt_buy": venta, "spread": round(spread,2), "spread_pct": round(spread/compra*100,2) if compra else 0, "liquidez": int(mercado.get("liquidez",0) or 0), "bcv_usd": mercado.get("bcv",0), "euro": mercado.get("eur",0), "timestamp": mercado.get("fecha").isoformat() if mercado.get("fecha") else None},
        "bancos": _obtener_contexto_bancos_ia(),
        "analisis_cuantitativo": analisis,
        "historial_general": hist[-120:],
        "regla_temporal": "Las variaciones 5m/15m/30m/1h/3h/7h se calculan contra datos con timestamp real; n/d significa insuficiencia de histórico o un hueco demasiado grande."
    }
    _AI_CONTEXT_CACHE["value"] = result
    _AI_CONTEXT_CACHE["expires"] = time.monotonic() + _CACHE_TTL_AI
    return result


def _respuesta_gemini_interactions_rest(prompt, model, temperature=0.35, system_instruction=None, max_output_tokens=900, timeout=7, tools=None):
    """Ruta REST recomendada por Google para Gemini Interactions API."""
    if not GEMINI_API_KEY:
        return None
    url = "https://generativelanguage.googleapis.com/v1beta/interactions"
    payload = {
        "model": model,
        "system_instruction": system_instruction or VENBOT_AI_SYSTEM,
        "input": prompt,
        "generation_config": {"max_output_tokens": max_output_tokens},
        "store": False,
        **({"tools": tools} if tools else {}),
    }
    try:
        r = requests.post(
            url,
            headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"},
            json=payload,
            timeout=timeout,
        )
        if not r.ok:
            logger.warning("Gemini Interactions REST %s HTTP %s: %s", model, r.status_code, r.text[:500])
            return None
        data = r.json()
        # Interactions API returns model_output steps containing text blocks.
        for step in reversed(data.get("steps") or []):
            if not isinstance(step, dict) or step.get("type") != "model_output":
                continue
            for block in step.get("content") or []:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = str(block.get("text", "")).strip()
                    if text:
                        return text
        text = str(data.get("output_text", "") or "").strip()
        return text or None
    except Exception as e:
        logger.warning("Gemini Interactions REST %s falló: %s", model, e)
        return None


def _respuesta_gemini_interactions_sdk(prompt, model, temperature=0.35):
    if not GEMINI_API_KEY or genai is None:
        return None
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        interaction = client.interactions.create(
            model=model,
            system_instruction=VENBOT_AI_SYSTEM,
            input=prompt,
            generation_config={"max_output_tokens": 900},
            store=False,
        )
        text = (getattr(interaction, "output_text", None) or "").strip()
        if text:
            return text
        for step in reversed(getattr(interaction, "steps", None) or []):
            if getattr(step, "type", None) != "model_output":
                continue
            for block in getattr(step, "content", None) or []:
                if getattr(block, "type", None) == "text":
                    text = (getattr(block, "text", None) or "").strip()
                    if text:
                        return text
        return None
    except Exception as e:
        logger.warning("Gemini Interactions SDK %s falló: %s", model, e)
        return None


def _respuesta_gemini_rest(prompt, model, temperature=0.35):
    """Compatibilidad legacy generateContent, útil como último fallback."""
    if not GEMINI_API_KEY:
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {
        "system_instruction": {"parts": [{"text": VENBOT_AI_SYSTEM}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": 900},
    }
    try:
        r = requests.post(url, params={"key": GEMINI_API_KEY}, json=payload, timeout=15)
        if not r.ok:
            logger.warning("Gemini legacy REST %s HTTP %s: %s", model, r.status_code, r.text[:500])
            return None
        data = r.json()
        parts = (((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
        text = "".join(str(x.get("text", "")) for x in parts if isinstance(x, dict)).strip()
        return text or None
    except Exception as e:
        logger.warning("Gemini legacy REST %s falló: %s", model, e)
        return None


def _respuesta_gemini_sdk(prompt, model, temperature=0.35):
    if not GEMINI_API_KEY or genai is None or types is None:
        return None
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=VENBOT_AI_SYSTEM,
                temperature=temperature,
                max_output_tokens=900,
            ),
        )
        return (getattr(response, "text", None) or "").strip() or None
    except Exception as e:
        logger.warning("Gemini legacy SDK %s falló: %s", model, e)
        return None

def _respuesta_openrouter(prompt, temperature=0.45):
    if not OPENROUTER_API_KEY:
        return None
    try:
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json", "HTTP-Referer": RENDER_EXTERNAL_URL or "https://p2p-monitor-binance.onrender.com", "X-Title": "Venbot"},
            json={"model": OPENROUTER_MODEL, "messages": [{"role": "system", "content": VENBOT_AI_SYSTEM}, {"role": "user", "content": prompt}], "temperature": temperature, "max_tokens": 900},
            timeout=15,
        )
        if not r.ok:
            logger.warning("OpenRouter HTTP %s: %s", r.status_code, r.text[:500])
            return None
        data = r.json()
        return (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip() or None
    except Exception as e:
        logger.warning("OpenRouter fallback falló: %s", e)
        return None

def _respuesta_local_mercado(contexto):
    """Fallback determinista: responde con datos reales de Venbot sin fingir que Gemini respondió."""
    m = contexto.get("mercado_actual") or {}
    bancos = contexto.get("bancos") or {}
    a = contexto.get("analisis_cuantitativo") or {}
    def money(x):
        try: return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception: return "n/d"
    compra = m.get("comprar_usdt_sell")
    venta = m.get("vender_usdt_buy")
    lines = [f"Lectura Venbot (respuesta local de respaldo): Compra USDT {money(compra)} Bs · Venta USDT {money(venta)} Bs."]
    if a:
        tendencia = a.get("tendencia") or a.get("estado_tendencia") or "n/d"
        calidad = a.get("proyeccion_7h", {}).get("calidad", a.get("calidad_datos", "n/d")) if isinstance(a.get("proyeccion_7h"), dict) else a.get("calidad_datos", "n/d")
        p7 = a.get("proyeccion_7h") if isinstance(a.get("proyeccion_7h"), dict) else {}
        lines.append(f"Tendencia: {tendencia}. Calidad de señal: {calidad}%.")
        if p7:
            cobertura = float(p7.get("cobertura_horas", 0) or 0)
            if cobertura >= 4.9:
                lines.append(f"Escenario 7H: compra {money(p7.get('compra'))} Bs · venta {money(p7.get('venta'))} Bs · rango midpoint {money(p7.get('rango_mid_min'))}–{money(p7.get('rango_mid_max'))} Bs · cobertura {cobertura:.1f} h.")
                lines.append(f"Soporte: {money(p7.get('soporte_7h'))} Bs · Resistencia: {money(p7.get('resistencia_7h'))} Bs.")
            else:
                lines.append(f"Escenario 7H: histórico insuficiente para una proyección confiable (cobertura {cobertura:.1f} h).")
    disponibles=[]
    for nombre in ("MERCANTIL","PROVINCIAL","BNC"):
        b=bancos.get(nombre) or {}
        if b.get("disponible"):
            disponibles.append(f"{nombre.title()}: comprar {money(b.get('comprar_usdt_sell'))} · vender {money(b.get('vender_usdt_buy'))} Bs")
    if disponibles: lines.append("Bancos: " + " | ".join(disponibles) + ".")
    lines.append("Nota: este respaldo usa la última lectura real persistida; no es una respuesta generada por Gemini.")
    return "\n".join(lines)


def _money_ia(x):
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "n/d"


def _pregunta_comparacion_bancos(low):
    bank_terms = sum(1 for x in ("mercantil", "provincial", "bnc") if x in low)
    compare_terms = any(x in low for x in (
        "mejor banco", "mejor", "compara", "comparar", "cuál banco", "cual banco",
        "qué banco", "que banco", "mejor opción", "mejor opcion", "más conveniente", "mas conveniente"
    ))
    # También debe detectar preguntas genéricas como:
    # “¿Cuál es el mejor banco para comprar USDT?” sin mencionar el nombre
    # de ningún banco. Las tasas se resuelven siempre desde el contexto P2P real.
    generic_bank_compare = (
        "banco" in low and compare_terms and
        any(x in low for x in ("comprar", "compra", "vender", "venta", "usdt", "precio"))
    )
    return bank_terms >= 2 or (bank_terms >= 1 and compare_terms) or generic_bank_compare


def _tipo_comparacion_bancos(low):
    """Determina si la pregunta pide compra, venta o ambas."""
    compra = any(x in low for x in ("comprar usdt", "comprar", "compra usdt", "compra"))
    venta = any(x in low for x in ("vender usdt", "vender", "venta usdt", "venta"))
    if compra and not venta:
        return "compra"
    if venta and not compra:
        return "venta"
    return "ambas"


def _respuesta_comparacion_bancos(contexto, tipo="ambas"):
    bancos = contexto.get("bancos") or {}
    rows = []
    for nombre in ("MERCANTIL", "PROVINCIAL", "BNC"):
        b = bancos.get(nombre) or {}
        try:
            compra = float(b.get("comprar_usdt_sell", 0) or 0)
            venta = float(b.get("vender_usdt_buy", 0) or 0)
        except Exception:
            continue
        if b.get("disponible") and compra > 0 and venta > 0:
            rows.append((nombre, compra, venta))
    if not rows:
        return "No hay lecturas bancarias reales disponibles en este momento."

    mejor_compra = min(rows, key=lambda x: x[1])
    mejor_venta = max(rows, key=lambda x: x[2])
    lines = ["Comparación actual de bancos (datos P2P reales de Venbot):"]

    if tipo in ("compra", "ambas"):
        lines.append(f"• Mejor para Comprar USDT: {mejor_compra[0].title()} — {_money_ia(mejor_compra[1])} Bs/USDT (menor precio).")
    if tipo in ("venta", "ambas"):
        lines.append(f"• Mejor para Vender USDT: {mejor_venta[0].title()} — {_money_ia(mejor_venta[2])} Bs/USDT (mayor precio).")

    # En una pregunta específica, mostramos las tres alternativas pero solo en la
    # dirección solicitada; así evitamos mezclar compra con venta.
    if tipo == "compra":
        for nombre, compra, _venta in rows:
            lines.append(f"• {nombre}: comprar {_money_ia(compra)} Bs/USDT.")
    elif tipo == "venta":
        for nombre, _compra, venta in rows:
            lines.append(f"• {nombre}: vender {_money_ia(venta)} Bs/USDT.")
    else:
        for nombre, compra, venta in rows:
            lines.append(f"• {nombre}: comprar {_money_ia(compra)} · vender {_money_ia(venta)} Bs.")
    return "\n".join(lines)


def _respuesta_banco_individual(contexto, low):
    bancos = contexto.get("bancos") or {}
    for nombre in ("MERCANTIL", "PROVINCIAL", "BNC"):
        if nombre.lower() in low:
            b = bancos.get(nombre) or {}
            if b.get("disponible"):
                return f"{nombre.title()} ahora: Comprar USDT {_money_ia(b.get('comprar_usdt_sell'))} Bs · Vender USDT {_money_ia(b.get('vender_usdt_buy'))} Bs. Datos P2P reales persistidos de Venbot."
    return None


def _respuesta_7h_local(contexto):
    a = contexto.get("analisis_cuantitativo") or {}
    p = a.get("proyeccion_7h") or {}
    cobertura = float(p.get("cobertura_horas", 0) or 0)
    tendencia = a.get("tendencia") or "⚪ SIN DATOS"
    calidad = p.get("calidad", 0)
    if cobertura < 4.9:
        return f"Para las próximas 7 horas, la señal actual es {tendencia}, pero Venbot solo tiene {cobertura:.1f} h de cobertura histórica; todavía no es suficiente para una proyección 7H confiable."
    return (f"Escenario estadístico 7H: {tendencia}. Calidad de señal {calidad}%. "
            f"Compra estimada {_money_ia(p.get('compra'))} Bs · venta estimada {_money_ia(p.get('venta'))} Bs. "
            f"Rango midpoint {_money_ia(p.get('rango_mid_min'))}–{_money_ia(p.get('rango_mid_max'))} Bs. "
            f"Cobertura histórica {cobertura:.1f} h. No es una garantía de precio futuro.")



def _pregunta_manipulacion(low):
    return any(x in low for x in (
        "manipulacion", "manipulación", "manipulado", "manipulada",
        "movimiento anomalo", "movimiento anómalo", "anomalía", "anomalia",
        "pump", "dump", "movimiento raro", "movimiento extraño", "movimiento extrano",
        "mercado manipulado", "mercado raro"
    ))


def _respuesta_manipulacion(contexto):
    a = contexto.get("analisis_cuantitativo") or {}
    m = a.get("manipulacion") or {"activa": False, "nivel": "normal", "score": 0, "motivos": []}
    if m.get("activa"):
        nivel = str(m.get("nivel", "vigilancia")).upper()
        motivos = "; ".join(m.get("motivos") or [])
        return (f"🚨 Venbot detecta una anomalía estadística de nivel {nivel}. "
                f"Señales observadas: {motivos or 'movimiento fuera de lo habitual'}. "
                "Esto puede indicar un movimiento anormal, pero no demuestra por sí solo manipulación intencional.")
    return ("🟢 No detecto una anomalía estadística activa en la lectura actual del P2P. "
            "El movimiento observado está dentro de los patrones recientes de Venbot. "
            "Esto no significa que sea imposible una manipulación; solo que el detector no encuentra señales suficientes ahora mismo.")


def _respuesta_momento_banco(contexto, low):
    bancos = contexto.get("bancos") or {}
    nombre = None
    for n in ("MERCANTIL", "PROVINCIAL", "BNC"):
        if n.lower() in low:
            nombre = n
            break
    if not nombre:
        return None
    b = bancos.get(nombre) or {}
    if not b.get("disponible"):
        return f"No tengo una lectura reciente disponible de {nombre.title()} para evaluar el momento."
    compra = float(b.get("comprar_usdt_sell", 0) or 0)
    venta = float(b.get("vender_usdt_buy", 0) or 0)
    p = (contexto.get("analisis_cuantitativo") or {}).get("proyeccion_7h") or {}
    tendencia = (contexto.get("analisis_cuantitativo") or {}).get("tendencia") or "sin datos"
    calidad = p.get("calidad", 0)
    direccion = "comprar" if any(x in low for x in ("comprar", "compra")) else "vender" if any(x in low for x in ("vender", "venta")) else None
    if direccion == "comprar":
        ranking = sorted(((n, float((bancos.get(n) or {}).get("comprar_usdt_sell", 0) or 0)) for n in bancos), key=lambda x: x[1] if x[1] > 0 else 1e99)
        mejor = ranking[0] if ranking and ranking[0][1] > 0 else (nombre, compra)
        return (f"Ahora mismo {nombre.title()} marca {_money_ia(compra)} Bs/USDT para comprar. "
                f"Es {'el mejor precio observado entre los bancos disponibles' if mejor[0] == nombre else 'una alternativa, pero no el precio más bajo observado'}. "
                f"La señal general de Venbot es {tendencia} con calidad {calidad}%. "
                "Si buscas entrar, el precio actual es favorable solo en relación con las cotizaciones observadas; no garantiza que el mercado no siga bajando.")
    if direccion == "vender":
        ranking = sorted(((n, float((bancos.get(n) or {}).get("vender_usdt_buy", 0) or 0)) for n in bancos), key=lambda x: x[1], reverse=True)
        mejor = ranking[0] if ranking and ranking[0][1] > 0 else (nombre, venta)
        return (f"Ahora mismo {nombre.title()} marca {_money_ia(venta)} Bs/USDT para vender. "
                f"Es {'el mejor precio observado entre los bancos disponibles' if mejor[0] == nombre else 'una alternativa, pero no el precio más alto observado'}. "
                f"La señal general de Venbot es {tendencia} con calidad {calidad}%. "
                "La lectura es informativa y no garantiza el precio siguiente.")
    return f"Ahora mismo {nombre.title()} está en {_money_ia(compra)} Bs para comprar y {_money_ia(venta)} Bs para vender."


def _ai_necesita_busqueda_web(low):
    return any(x in low for x in (
        "hoy", "ahora", "actualmente", "últimas noticias", "ultimas noticias", "noticias",
        "último", "ultimo", "última", "ultima", "reciente", "recientes", "2026",
        "esta semana", "este mes", "precio actual", "qué pasó", "que paso", "quién es", "quien es"
    ))


def _preparar_prompt_ia(mensaje, historial, contexto):
    prev = []
    for h in (historial or [])[-8:]:
        role = "user" if str(h.get("role", "")).lower() in {"user", "human"} else "assistant"
        content = str(h.get("content", h.get("text", "")))[:1800]
        if content:
            prev.append({"role": role, "content": content})
    return ("CONTEXTO REAL DE VENBOT:\n" + json.dumps(contexto, ensure_ascii=False, default=str) +
            "\n\nHISTORIAL RECIENTE:\n" + json.dumps(prev, ensure_ascii=False) +
            "\n\nPREGUNTA ACTUAL:\n" + mensaje)


def _stream_event(text=None, done=False):
    payload = {"done": bool(done)}
    if text is not None:
        payload["text"] = text
    return "data: " + json.dumps(payload, ensure_ascii=True) + "\n\n"


def _generador_ai_stream(mensaje, historial):
    """SSE: respuestas locales salen inmediatamente; Gemini llega por fragmentos."""
    t0 = time.monotonic()
    texto = (mensaje or "").strip()
    low = texto.lower()
    market_query = any(k in low for k in (
        "p2p", "usdt", "ves", "comprar", "vender", "precio", "mercado", "spread", "liquidez",
        "momentum", "soporte", "resistencia", "proyeccion", "proyección", "prediccion", "predicción",
        "tendencia", "bcv", "dolar", "dólar", "euro", "binance", "tasa", "arbitraje", "7h", "7 horas",
        "mercantil", "provincial", "bnc", "banco", "manipulacion", "manipulación", "anomalia", "anomalía"
    ))
    contexto = _serializar_contexto_mercado() if market_query else {"modo": "general"}
    if market_query:
        if _pregunta_manipulacion(low):
            yield _stream_event(_respuesta_manipulacion(contexto)); yield _stream_event(done=True); return
        if _pregunta_comparacion_bancos(low):
            yield _stream_event(_respuesta_comparacion_bancos(contexto, _tipo_comparacion_bancos(low))); yield _stream_event(done=True); return
        natural_bank = _respuesta_momento_banco(contexto, low)
        if natural_bank and any(x in low for x in ("momento", "conviene", "conviene comprar", "buen momento", "vale la pena", "recomiendas", "recomienda")):
            yield _stream_event(natural_bank); yield _stream_event(done=True); return
        if any(x in low for x in ("próximas 7 horas", "proximas 7 horas", "7 horas", "proyección 7h", "proyeccion 7h", "predicción 7h", "prediccion 7h")):
            yield _stream_event(_respuesta_7h_local(contexto)); yield _stream_event(done=True); return
        if any(x in low for x in ("precio actual", "precio de usdt", "cuánto está usdt", "cuanto esta usdt", "cotización actual", "cotizacion actual")):
            yield _stream_event(_respuesta_local_mercado(contexto)); yield _stream_event(done=True); return
    prompt = _preparar_prompt_ia(texto, historial, contexto)
    if GEMINI_API_KEY:
        system = VENBOT_AI_SYSTEM
        if market_query:
            system += "\n\nPara mercado, usa el contexto real y responde de forma natural, directa y humana. No recites todo el contexto."
        else:
            system += "\n\nHabla como un asistente humano y útil: natural, claro, contextual y sin frases robóticas. Responde directamente antes de ampliar."
        # Toda consulta GENERAL tiene acceso a Google Search. Gemini decide si
        # realmente necesita buscar; así Venbot puede responder tanto conocimiento
        # general como preguntas actuales sin depender de una lista rígida de palabras.
        tools = None if market_query else [{"type": "google_search"}]
        payload = {
            "model": GEMINI_MODEL, "system_instruction": system, "input": prompt,
            "generation_config": {
                "temperature": 0.15 if market_query else 0.35,
                "max_output_tokens": 600 if market_query else 450,
            },
            "store": False,
            "stream": True,
        }
        if tools: payload["tools"] = tools
        try:
            r = requests.post(
                "https://generativelanguage.googleapis.com/v1beta/interactions",
                headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json", "Accept": "text/event-stream"},
                json=payload, timeout=(3, 18), stream=True,
            )
            if r.ok:
                got = False
                for line in r.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if raw == "[DONE]":
                        continue
                    try: ev = json.loads(raw)
                    except Exception: continue
                    if ev.get("event_type") == "step.delta":
                        delta = ev.get("delta") or {}
                        if delta.get("type") == "text" and delta.get("text"):
                            got = True
                            yield _stream_event(str(delta["text"]))
                if got:
                    yield _stream_event(done=True); return
            else:
                logger.warning("Gemini stream HTTP %s: %s", r.status_code, r.text[:300])
        except Exception as e:
            logger.warning("Gemini stream falló: %s", e)

        # Si el stream no entrega texto (por ejemplo, una incidencia temporal del
        # stream o una respuesta con herramientas), reintentamos la misma pregunta
        # por Interactions normal. Para consultas generales mantenemos Google Search
        # habilitado en este segundo intento.
        try:
            fallback_tools = None if market_query else [{"type": "google_search"}]
            fallback_text = _respuesta_gemini_interactions_rest(
                prompt, GEMINI_MODEL,
                temperature=0.15 if market_query else 0.35,
                system_instruction=system,
                max_output_tokens=600 if market_query else 450,
                timeout=9,
                tools=fallback_tools,
            )
            if fallback_text:
                yield _stream_event(fallback_text)
                yield _stream_event(done=True)
                return
        except Exception as e:
            logger.warning("Gemini fallback no-stream falló: %s", e)

    if market_query:
        yield _stream_event(_respuesta_local_mercado(contexto))
    elif OPENROUTER_API_KEY:
        text = _respuesta_openrouter(prompt, 0.35)
        yield _stream_event(text or "No pude generar una respuesta ahora.")
    else:
        yield _stream_event("No pude responder en este momento. Intenta de nuevo en unos segundos.")
    yield _stream_event(done=True)


def generar_respuesta_ia(mensaje, historial):
    """IA híbrida: Gemini explica; Venbot aporta datos reales y fallback local inmediato."""
    t0 = time.monotonic()
    texto = (mensaje or "").strip()
    low = texto.lower()
    logger.info("AI CHAT: pregunta recibida | chars=%s", len(texto))
    if not GEMINI_API_KEY and not OPENROUTER_API_KEY:
        logger.warning("AI CHAT: sin proveedor configurado")
        return "La IA no tiene proveedor configurado en Render."

    if low in {"hola", "hola!", "hola.", "buenas", "buenas!", "hey", "hey!"}:
        logger.info("AI CHAT: respuesta local inmediata | elapsed=%.2fs", time.monotonic()-t0)
        return "Hola 👋 Soy Venbot AI. Puedo analizar el mercado P2P de USDT/VES, bancos, tendencia, liquidez y proyección de 7 horas, además de responder preguntas generales."
    if any(k in low for k in ("qué puedes hacer", "que puedes hacer", "para qué sirves", "para que sirves")) and len(low) < 100:
        logger.info("AI CHAT: respuesta local de capacidades | elapsed=%.2fs", time.monotonic()-t0)
        return "Puedo explicar temas, responder preguntas y analizar el P2P USDT/VES con datos reales: precios de compra/venta, Mercantil, Provincial y BNC, liquidez, tendencia, soporte/resistencia y escenario estadístico a 7 horas."

    market_query = any(k in low for k in (
        "p2p", "usdt", "ves", "comprar", "vender", "precio", "mercado", "spread", "liquidez",
        "momentum", "soporte", "resistencia", "proyeccion", "proyección", "prediccion", "predicción",
        "tendencia", "bcv", "dolar", "dólar", "euro", "binance", "tasa", "arbitraje", "7h", "7 horas",
        "mercantil", "provincial", "bnc", "banco"
    ))
    contexto = _serializar_contexto_mercado() if market_query else {"modo": "general"}
    if market_query:
        logger.info("AI CHAT: contexto P2P obtenido | bancos=%s | has_analysis=%s", list((contexto.get("bancos") or {}).keys()), bool(contexto.get("analisis_cuantitativo")))
        # Consultas factuales de mercado no dependen de Gemini: la fuente de verdad es Venbot.
        if _pregunta_manipulacion(low):
            logger.info("AI CHAT: detector de anomalías determinístico")
            return _respuesta_manipulacion(contexto)
        natural_bank = _respuesta_momento_banco(contexto, low)
        if natural_bank and any(x in low for x in ("momento", "conviene", "buen momento", "vale la pena", "recomiendas", "recomienda")):
            return natural_bank
        if _pregunta_comparacion_bancos(low):
            logger.info("AI CHAT: comparación bancaria determinística")
            return _respuesta_comparacion_bancos(contexto, _tipo_comparacion_bancos(low))
        banco_directo = _respuesta_banco_individual(contexto, low)
        if banco_directo and any(x in low for x in ("cuánto", "cuanto", "está", "esta", "precio", "cotiza", "vale")):
            return banco_directo
        if any(x in low for x in ("precio actual", "precio de usdt", "cuánto está usdt", "cuanto esta usdt", "cotización actual", "cotizacion actual")):
            return _respuesta_local_mercado(contexto)
        if any(x in low for x in ("próximas 7 horas", "proximas 7 horas", "7 horas", "proyección 7h", "proyeccion 7h", "predicción 7h", "prediccion 7h")):
            return _respuesta_7h_local(contexto)
    prev = []
    for h in (historial or [])[-8:]:
        role = "user" if str(h.get("role", "")).lower() in {"user", "human"} else "assistant"
        content = str(h.get("content", h.get("text", "")))[:1800]
        if content: prev.append({"role": role, "content": content})
    prompt = ("CONTEXTO REAL DE VENBOT:\n" + json.dumps(contexto, ensure_ascii=False, default=str) +
              "\n\nHISTORIAL RECIENTE:\n" + json.dumps(prev, ensure_ascii=False) +
              "\n\nPREGUNTA ACTUAL:\n" + texto)

    if market_query:
        system = VENBOT_AI_SYSTEM + "\n\nPara preguntas por bancos: compara explícitamente los campos bancos.*. Comprar USDT usa comprar_usdt_sell (SELL); vender USDT usa vender_usdt_buy (BUY). Indica el banco ganador y su precio cuando existan datos disponibles. No digas que faltan tasas bancarias si están presentes en CONTEXTO REAL DE VENBOT."
        max_tokens, temperature = 600, 0.15
    else:
        system = VENBOT_AI_SYSTEM + "\n\nPara preguntas generales responde de forma concisa: normalmente 1-3 párrafos. No conviertas una pregunta sencilla en un ensayo."
        max_tokens, temperature = 450, 0.35

    if GEMINI_API_KEY:
        logger.info("AI CHAT: Gemini iniciado | model=%s | market=%s | google_search=%s", GEMINI_MODEL, market_query, not market_query)
        gt0 = time.monotonic()
        tools = None if market_query else [{"type": "google_search"}]
        text = _respuesta_gemini_interactions_rest(
            prompt, GEMINI_MODEL, temperature,
            system_instruction=system, max_output_tokens=max_tokens, timeout=9, tools=tools
        )
        if text:
            logger.info("AI CHAT: Gemini respondió | elapsed=%.2fs | chars=%s", time.monotonic()-gt0, len(text))
            return text
        logger.warning("AI CHAT: Gemini no respondió | elapsed=%.2fs", time.monotonic()-gt0)

    # Fallback útil e inmediato para mercado: nunca deja al usuario sin los datos reales.
    if market_query:
        logger.info("AI CHAT: fallback local P2P | total_elapsed=%.2fs", time.monotonic()-t0)
        return _respuesta_local_mercado(contexto)

    if OPENROUTER_API_KEY:
        logger.info("AI CHAT: OpenRouter fallback iniciado")
        text = _respuesta_openrouter(prompt, temperature)
        if text:
            logger.info("AI CHAT: OpenRouter respondió | total_elapsed=%.2fs", time.monotonic()-t0)
            return text

    logger.warning("AI CHAT: sin respuesta de proveedor | total_elapsed=%.2fs", time.monotonic()-t0)
    return "La IA no pudo responder ahora. El servicio P2P sigue funcionando; vuelve a intentarlo en unos segundos."

class AIChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=6000)
    history: list[dict] = Field(default_factory=list)


@app.post("/api/ai/chat/stream")
async def ai_chat_stream(payload: AIChatRequest):
    return StreamingResponse(_generador_ai_stream(payload.message.strip(), payload.history), media_type="text/event-stream", headers={"Cache-Control":"no-cache", "Connection":"keep-alive", "X-Accel-Buffering":"no"})


@app.get("/api/ai/health")
def ai_health():
    return {"configured": bool(GEMINI_API_KEY or OPENROUTER_API_KEY), "gemini_configured": bool(GEMINI_API_KEY), "openrouter_configured": bool(OPENROUTER_API_KEY), "preferred_models": ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "api": "Interactions API"}


@app.post("/api/ai/chat")
async def ai_chat(payload: AIChatRequest):
    t0 = time.monotonic()
    mensaje = payload.message.strip()
    logger.info("AI CHAT: endpoint recibido")
    respuesta = await asyncio.to_thread(generar_respuesta_ia, mensaje, payload.history)
    logger.info("AI CHAT: respuesta enviada | elapsed=%.2fs | chars=%s", time.monotonic()-t0, len(respuesta or ""))
    return {"ok": True, "answer": respuesta, "model": GEMINI_MODEL if GEMINI_API_KEY else (OPENROUTER_MODEL if OPENROUTER_API_KEY else "not_configured")}


@app.post("/webhook")
async def telegram_webhook(req: Request):
    if not telegram_app:
        return {"ok": False, "error": "Telegram no inicializado"}
    data = await req.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


@app.on_event("startup")
async def startup_event():
    global telegram_app, collector_task
    validar_configuracion()

    if DATABASE_URL:
        await asyncio.to_thread(inicializar_db)
        try:
            await asyncio.to_thread(recolectar_mercado_general)
        except Exception as e:
            logger.exception("Captura inicial falló: %s", e)

    if TELEGRAM_BOT_TOKEN:
        telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).updater(None).build()
        telegram_app.add_handler(CommandHandler("start", start))
        telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("precision", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
        telegram_app.add_handler(CommandHandler("bancos", cmd_bancos))
        telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
        telegram_app.add_handler(CallbackQueryHandler(manejar_botones))

        await telegram_app.initialize()
        await telegram_app.start()

        if RENDER_EXTERNAL_URL:
            webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
            await telegram_app.bot.delete_webhook(drop_pending_updates=False)
            await telegram_app.bot.set_webhook(url=webhook_url)
            logger.info("Webhook Telegram configurado: %s", webhook_url)

    if DATABASE_URL:
        collector_task = asyncio.create_task(tarea_recoleccion_automatica())


@app.on_event("shutdown")
async def shutdown_event():
    global collector_task, telegram_app
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass
    if telegram_app:
        await telegram_app.stop()
        await telegram_app.shutdown()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port, reload=False)
