import os
import io
import asyncio
import logging
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
COLLECT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))
MARKET_MAX_AGE_SECONDS = int(os.getenv("MARKET_MAX_AGE_SECONDS", "90"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.8-flash").strip()
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
ULTIMO_ESTADO_TENDENCIA = "🛡️ ZONA DE PROTECCIÓN ESTABLE"
CONFIGURACION_BANCOS = {}
telegram_app = None
collector_task = None


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


def _binance_fetch_raw(trade_type, rows=20):
    """Consulta el MGS C2C Agent API público actual de Binance.

    Primero se intenta con el filtro BANK documentado. Si Binance devuelve vacío,
    se reintenta sin filtro de método de pago y el banco se filtra localmente.
    Esto evita perder anuncios cuando el identificador del método bancario cambia.
    """
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.binance.com",
        "Referer": "https://www.binance.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139 Safari/537.36",
    }
    agent_url = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
    limit = min(max(int(rows), 1), 20)
    last_error = None

    # 1) Método BANK documentado por Binance.
    attempts = [
        {"fiat": "VES", "asset": "USDT", "tradeType": trade_type,
         "limit": limit, "order": "price", "tradeMethodIdentifiers": "BANK"},
        # 2) Sin filtro de método: filtramos Mercantil/Provincial/BNC localmente.
        {"fiat": "VES", "asset": "USDT", "tradeType": trade_type,
         "limit": limit, "order": "price"},
    ]

    for params in attempts:
        try:
            r = HTTP.get(agent_url, params=params, headers=headers, timeout=12)
            r.raise_for_status()
            data = _extraer_ads_binance(r.json())
            if data:
                logger.info("Binance ad-list %s: %s anuncios recibidos", trade_type, len(data))
                return data
            last_error = RuntimeError("ad-list respondió sin anuncios")
        except Exception as e:
            last_error = e
            logger.warning("Binance ad-list %s falló con params %s: %s", trade_type, params, e)

    # Fallback histórico del sitio web; se evita el endpoint obsoleto.
    fallback = "https://www.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = {
        "asset": "USDT", "fiat": "VES", "page": 1, "rows": limit,
        "tradeType": trade_type, "payTypes": ["BANK"], "publisherType": None,
        "merchantCheck": False, "proMerchantAds": False, "shieldMerchantAds": False,
        "countries": [],
    }
    try:
        r = HTTP.post(fallback, json=payload, headers=headers, timeout=12)
        r.raise_for_status()
        data = _extraer_ads_binance(r.json())
        if data:
            logger.info("Binance fallback adv/search %s: %s anuncios recibidos", trade_type, len(data))
            return data
        raise RuntimeError("fallback adv/search respondió sin anuncios")
    except Exception as fallback_error:
        raise RuntimeError(f"ad-list: {last_error}; fallback adv/search: {fallback_error}")

def _ad_contiene_banco(item, banco_filtro):
    if banco_filtro == "GENERAL":
        return True
    try:
        blob = json.dumps(item, ensure_ascii=False).lower()
    except Exception:
        blob = str(item).lower()
    aliases = {
        "MERCANTIL": ("mercantil",),
        "PROVINCIAL": ("provincial", "bbva provincial", "bbva"),
        "BNC": ("bnc", "banco nacional de credito", "banco nacional de crédito"),
    }
    return any(alias in blob for alias in aliases.get(banco_filtro, (banco_filtro.lower(),)))


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

def obtener_precios_binance_p2p(banco_filtro="GENERAL"):
    """
    Desde la perspectiva del usuario:
    - Comprar USDT => anuncios SELL.
    - Vender USDT  => anuncios BUY.
    """
    global ULTIMO_REGISTRO_VALIDO

    try:
        anuncios_compra_usuario = _binance_search("SELL", banco_filtro)
        anuncios_venta_usuario = _binance_search("BUY", banco_filtro)

        compra = calcular_vwap_con_filtro(anuncios_compra_usuario)
        venta = calcular_vwap_con_filtro(anuncios_venta_usuario)

        if compra > 0 and venta > 0:
            liquidez = len(anuncios_compra_usuario) + len(anuncios_venta_usuario)
            ULTIMO_REGISTRO_VALIDO = {
                "compra": compra,
                "venta": venta,
                "timestamp": datetime.now(VET),
            }
            return round(compra, 2), round(venta, 2), liquidez
    except Exception as e:
        logger.warning("Binance P2P no disponible para %s: %s", banco_filtro, e)

    # Última lectura real persistida. No se fabrican precios.
    if banco_filtro == "GENERAL":
        db = obtener_mercado_actual_db()
        if db and db["compra"] > 0 and db["venta"] > 0:
            return round(db["compra"], 2), round(db["venta"], 2), int(db["liquidez"])

    if ULTIMO_REGISTRO_VALIDO["compra"] > 0 and ULTIMO_REGISTRO_VALIDO["venta"] > 0:
        return (
            round(ULTIMO_REGISTRO_VALIDO["compra"], 2),
            round(ULTIMO_REGISTRO_VALIDO["venta"], 2),
            0,
        )

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
    """Busca el valor más cercano a N horas atrás, sin asumir 1 muestra = 1 minuto."""
    if not fechas or not valores:
        return None
    try:
        objetivo = fechas[-1] - timedelta(hours=horas)
        mejor_i = min(range(len(fechas)), key=lambda i: abs(fechas[i] - objetivo))
        if abs(fechas[mejor_i] - objetivo) > timedelta(minutes=max(10, horas * 15)):
            return None
        return float(valores[mejor_i])
    except Exception:
        return None


def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    """
    Motor estadístico para Telegram.

    No intenta adivinar un precio con una caja negra. Usa la misma serie real
    de muestras P2P y calcula cambios por tiempo real (15m/1h/3h/7h), rango
    reciente, volatilidad y una proyección moderada a 7h.
    """
    filas = obtener_estadisticas_db(limit=2000, banco=banco_filtro)
    total_muestras = len(filas)

    if actual_compra <= 0 or actual_venta <= 0:
        return {
            "pred_compra": 0.0, "pred_venta": 0.0,
            "pred_compra_str": "Sin datos", "pred_venta_str": "Sin datos",
            "tendencia": "⚠️ SIN DATOS DE MERCADO",
            "piso_str": "Sin datos", "techo_str": "Sin datos",
            "muestras": int(total_muestras), "liquidez_actual": int(liquidez_actual),
            "estado_comunidad": "⚠️ Sin lectura", "confianza": 0,
            "cambios": {}, "volatilidad_pct": 0.0, "spread_pct": 0.0,
            "spread_promedio": 0.0, "rango_pct": 0.0,
        }

    now = datetime.now(VET)
    compra = float(actual_compra)
    venta = float(actual_venta)
    mid_actual = (compra + venta) / 2.0

    # Serie histórica en orden cronológico.
    series = []
    for f in filas:
        try:
            c, v, _, fecha = f
            c, v = float(c), float(v)
            if c > 0 and v > 0 and fecha:
                series.append((fecha.astimezone(VET) if getattr(fecha, 'tzinfo', None) else VET.localize(fecha), (c + v) / 2.0, c, v))
        except Exception:
            continue

    # Incorporar la lectura actual al final para que Telegram y el monitor no
    # trabajen con una muestra histórica que quedó 1 minuto atrás.
    if not series or now >= series[-1][0]:
        series.append((now, mid_actual, compra, venta))

    fechas = [x[0] for x in series]
    mids = np.array([x[1] for x in series], dtype=float)
    compras = np.array([x[2] for x in series], dtype=float)
    ventas = np.array([x[3] for x in series], dtype=float)

    cambios = {}
    for horas, etiqueta in [(5/60, "5m"), (0.25, "15m"), (0.5, "30m"), (1.0, "1h"), (3.0, "3h"), (7.0, "7h")]:
        previo = _obtener_punto_cercano_por_horas(fechas, mids, horas)
        cambios[etiqueta] = round(_porcentaje_cambio(previo, mid_actual), 3) if previo else None

    # Ventana temporal real de 7h. No asumimos que una muestra equivale a un minuto.
    cutoff_7h = now - timedelta(hours=7)
    idx_7h = [i for i, dt in enumerate(fechas) if dt >= cutoff_7h]
    if len(idx_7h) >= 2:
        recent_idx = idx_7h
    else:
        recent_idx = list(range(max(0, len(mids) - min(len(mids), 120)), len(mids)))
    recent = mids[recent_idx] if recent_idx else mids
    if len(recent) > 2:
        returns = np.diff(recent) / recent[:-1] * 100.0
        volatilidad = float(np.std(returns))
    else:
        volatilidad = 0.0

    # Rango de 7h usando las mismas marcas temporales.
    recent_buy = compras[recent_idx] if recent_idx else compras
    recent_sell = ventas[recent_idx] if recent_idx else ventas
    min_7h = float(np.min(recent_buy))
    max_7h = float(np.max(recent_sell))
    rango = max_7h - min_7h
    rango_pct = (rango / mid_actual * 100.0) if mid_actual else 0.0

    # Tendencia multi-ventana: evita llamar "lateral" a cualquier movimiento
    # que caiga debajo de un umbral fijo en porcentaje.
    c15, c1h, c3h = cambios.get("15m"), cambios.get("1h"), cambios.get("3h")
    señales = [x for x in (c15, c1h, c3h) if x is not None]
    if señales:
        score = (c15 or 0.0) * 0.45 + (c1h or 0.0) * 0.35 + (c3h or 0.0) * 0.20
    else:
        score = 0.0

    # Umbral adaptativo: evita clasificar como lateral por un límite fijo.
    abs_typical = float(np.median(np.abs(returns))) if len(returns) else 0.0
    umbral = max(0.025, volatilidad * 2.2, abs_typical * 2.5)
    if score > umbral and sum(1 for x in señales if x > 0) >= 2:
        tendencia = "🟢 TENDENCIA ALCISTA"
        detalle = "Impulso comprador confirmado en varias ventanas."
    elif score < -umbral and sum(1 for x in señales if x < 0) >= 2:
        tendencia = "🔴 TENDENCIA BAJISTA"
        detalle = "Presión vendedora confirmada en varias ventanas."
    elif abs(score) <= umbral:
        tendencia = "⚪ RANGO / LATERAL"
        detalle = "Los cambios recientes no superan el ruido estadístico observado."
    else:
        tendencia = "🟡 SEÑAL MIXTA"
        detalle = "Las ventanas temporales no están alineadas."

    # Proyección moderada: extrapola el movimiento medio observado en 1h/3h,
    # pero limita la magnitud para no convertir ruido en una predicción extrema.
    rate_1h = (c1h / 1.0) if c1h is not None else 0.0
    rate_3h = (c3h / 3.0) if c3h is not None else rate_1h
    rate_h = 0.65 * rate_1h + 0.35 * rate_3h
    raw_delta_pct = rate_h * 7.0
    max_delta_pct = max(0.35, min(3.0, max(0.35, rango_pct * 1.25)))
    delta_pct = float(np.clip(raw_delta_pct, -max_delta_pct, max_delta_pct))
    pred_mid = mid_actual * (1.0 + delta_pct / 100.0)

    spread_actual = venta - compra
    spreads = ventas - compras
    recent_spreads = spreads[recent_idx] if recent_idx else spreads
    avg_spread = float(np.mean(recent_spreads)) if len(recent_spreads) else spread_actual
    spread_change_pct = ((spread_actual - avg_spread) / avg_spread * 100.0) if avg_spread > 0 else 0.0
    support_7h = float(np.min(recent_buy)) if len(recent_buy) else mid_actual
    resistance_7h = float(np.max(recent_sell)) if len(recent_sell) else mid_actual
    median_7h = float(np.median(recent)) if len(recent) else mid_actual
    range_size_7h = resistance_7h - support_7h
    range_position_7h = ((mid_actual - support_7h) / range_size_7h * 100.0) if range_size_7h > 0 else 50.0
    # Mantener el spread proyectado cerca del spread observado, sin prometer
    # que el libro de órdenes permanecerá igual durante 7 horas.
    pred_spread = max(0.01, 0.70 * spread_actual + 0.30 * avg_spread)
    pred_compra = pred_mid - pred_spread / 2.0
    pred_venta = pred_mid + pred_spread / 2.0

    spread_pct = (spread_actual / compra * 100.0) if compra else 0.0

    # Confianza orientativa, basada en cantidad de muestras, continuidad y
    # acuerdo entre ventanas; no es probabilidad de acierto.
    acuerdo = 0.0
    if len(señales) >= 3:
        positivos = sum(1 for x in señales if x > 0)
        negativos = sum(1 for x in señales if x < 0)
        acuerdo = max(positivos, negativos) / len(señales)
    continuidad = min(1.0, len(series) / 420.0)
    confianza = int(round(100 * (0.55 * continuidad + 0.45 * acuerdo))) if señales else int(round(55 * continuidad))

    liquidez = int(liquidez_actual)
    if liquidez >= 40:
        estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos"
    elif liquidez >= 20:
        estado_comunidad = "🟡 Liquidez Moderada"
    else:
        estado_comunidad = "🔴 Liquidez Baja / Poca Cobertura"

    return {
        "pred_compra": round(pred_compra, 2),
        "pred_venta": round(pred_venta, 2),
        "pred_compra_str": f"{pred_compra:.2f} Bs",
        "pred_venta_str": f"{pred_venta:.2f} Bs",
        "tendencia": tendencia,
        "detalle_tendencia": detalle,
        "piso_str": f"{min_7h:.2f} Bs",
        "techo_str": f"{max_7h:.2f} Bs",
        "muestras": int(total_muestras),
        "liquidez_actual": liquidez,
        "estado_comunidad": estado_comunidad,
        "confianza": confianza,
        "cambios": cambios,
        "volatilidad_pct": round(volatilidad, 4),
        "spread_pct": round(spread_pct, 3),
        "spread_promedio": round(avg_spread, 2),
        "spread_cambio_pct": round(spread_change_pct, 2),
        "soporte_7h": round(support_7h, 2),
        "resistencia_7h": round(resistance_7h, 2),
        "mediana_7h": round(median_7h, 2),
        "posicion_rango_7h": round(range_position_7h, 1),
        "rango_pct": round(rango_pct, 3),
        "max_delta_pct": round(max_delta_pct, 3),
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


def calcular_analisis_monitor(banco_filtro="GENERAL"):
    """Diagnóstico único y compartido por monitor web, IA y Telegram.

    Usa la misma serie P2P real y las mismas reglas que el motor de Telegram.
    Las ventanas se calculan por tiempo, no por cantidad fija de muestras.
    """
    mercado = obtener_mercado_actual_db() or {}
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
    if not mids or (fechas and now >= (fechas[-1].astimezone(VET) if getattr(fechas[-1], 'tzinfo', None) else VET.localize(fechas[-1]))):
        mids.append(current_mid)
        spreads.append(venta - compra)
        fechas.append(now)

    arr = np.asarray(mids, dtype=float)
    recent = arr[-min(len(arr), 420):]
    support = float(np.min(recent)) if len(recent) else current_mid
    resistance = float(np.max(recent)) if len(recent) else current_mid
    median = float(np.median(recent)) if len(recent) else current_mid
    range_size = resistance - support
    range_pos = ((current_mid - support) / range_size * 100.0) if range_size > 0 else 50.0

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

    if spread_pct >= 1.50:
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

    return {
        "ok": True,
        "banco": banco_filtro,
        "tactica": {"estado": tactica_estado, "detalle": tactica_detail},
        "flujo": {"estado": flujo_estado, "detalle": flujo_detail},
        "niveles": {"estado": niveles_estado, "detalle": niveles_detail},
        "tendencia": q.get("tendencia"),
        "detalle_tendencia": q.get("detalle_tendencia"),
        "proyeccion_7h": {
            "compra": q.get("pred_compra"),
            "venta": q.get("pred_venta"),
            "compra_str": q.get("pred_compra_str"),
            "venta_str": q.get("pred_venta_str"),
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
            "calidad_datos": quality,
        },
    }


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
        return f"{val:+.2f}%"

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
        f"• Canal 7H: `{datos['piso_str']}` / `{datos['techo_str']}`\n"
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
            mercado = await asyncio.to_thread(recolectar_mercado_general)

            # Bancos separados, solo para histórico/Telegram.
            for banco in ("MERCANTIL", "PROVINCIAL", "BNC"):
                c, v, l = await asyncio.to_thread(obtener_precios_binance_p2p, banco)
                if c > 0 and v > 0:
                    await asyncio.to_thread(guardar_muestra_db, c, v, l, banco)

            if mercado["compra"] > 0 and mercado["venta"] > 0:
                datos = await asyncio.to_thread(
                    motor_quant_inteligente,
                    mercado["compra"], mercado["venta"], mercado["liquidez"], "GENERAL"
                )
                tendencia = datos["tendencia"]
                if (
                    TELEGRAM_ALERT_CHAT_ID
                    and telegram_app
                    and tendencia != ULTIMO_ESTADO_TENDENCIA
                ):
                    ULTIMO_ESTADO_TENDENCIA = tendencia
                    await telegram_app.bot.send_message(
                        chat_id=TELEGRAM_ALERT_CHAT_ID,
                        text=(
                            "🚨 *ALERTA PROACTIVA DE MERCADO P2P*\n"
                            f"• Cambio: `{tendencia}`\n"
                            f"• Comprar USDT: `{mercado['compra']:.2f} Bs`\n"
                            f"• Vender USDT: `{mercado['venta']:.2f} Bs`"
                        ),
                        parse_mode="Markdown",
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Error en tarea autónoma: %s", e)

        await asyncio.sleep(max(30, COLLECT_INTERVAL_SECONDS))


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
    mercado = obtener_mercado_actual_db()

    if refresh or _mercado_desactualizado(mercado):
        nuevo = recolectar_mercado_general()
        mercado = obtener_mercado_actual_db()
        if not mercado and nuevo["compra"] > 0:
            mercado = {
                "compra": nuevo["compra"],
                "venta": nuevo["venta"],
                "liquidez": nuevo["liquidez"],
                "bcv": nuevo["bcv"],
                "eur": nuevo["eur"],
                "fuente_bcv": nuevo["fuente_bcv"],
                "fecha": nuevo["timestamp"],
            }

    if not mercado:
        return {
            "ok": False,
            "compra": 0,
            "venta": 0,
            "buy": 0,
            "sell": 0,
            "spread": 0,
            "spread_pct": 0,
            "bcv": 0,
            "eur": 0,
            "liquidez": 0,
            "timestamp": datetime.now(VET).isoformat(),
            "error": "Todavía no existe una lectura real.",
        }

    compra = round(mercado["compra"], 2)
    venta = round(mercado["venta"], 2)
    spread = round(venta - compra, 2)
    spread_pct = round((spread / compra) * 100, 2) if compra > 0 else 0.0
    fecha = mercado["fecha"]
    if fecha and fecha.tzinfo is None:
        fecha = VET.localize(fecha)

    return {
        "ok": compra > 0 and venta > 0,
        "compra": compra,
        "venta": venta,
        "buy": compra,
        "sell": venta,
        "spread": spread,
        "spread_pct": spread_pct,
        "bcv": round(mercado["bcv"], 2),
        "eur": round(mercado["eur"], 2),
        "liquidez": mercado["liquidez"],
        "fuente_bcv": mercado["fuente_bcv"],
        "timestamp": fecha.astimezone(VET).isoformat() if fecha else datetime.now(VET).isoformat(),
    }


@app.get("/api/market")
def obtener_precios_market_alias():
    return obtener_precios_api(False)


@app.get("/api/analysis")
def obtener_analysis_api():
    return calcular_analisis_monitor("GENERAL")


@app.get("/api/history")
def obtener_history(period: str = Query("1d", pattern="^(1d|7d|30d)$")):
    horas = {"1d": 24, "7d": 24 * 7, "30d": 24 * 30}[period]
    desde = datetime.now(VET) - timedelta(hours=horas)
    limite = {"1d": 500, "7d": 2500, "30d": 10000}[period]
    filas = obtener_estadisticas_db(limit=limite, banco="GENERAL", desde=desde)

    puntos = [
        {
            "compra": round(float(c), 2),
            "venta": round(float(v), 2),
            "liquidez": int(l or 0),
            "timestamp": (
                VET.localize(f).isoformat()
                if f and f.tzinfo is None
                else f.astimezone(VET).isoformat()
            ),
        }
        for c, v, l, f in filas
    ]

    # Añadir la lectura actual real al final si es más reciente que el histórico.
    # Así el gráfico, las tarjetas y el análisis parten de la misma serie.
    mercado = obtener_mercado_actual_db() or {}
    mc = float(mercado.get("compra", 0) or 0)
    mv = float(mercado.get("venta", 0) or 0)
    mf = mercado.get("fecha")
    if mc > 0 and mv > 0 and mf:
        live_ts = VET.localize(mf).isoformat() if mf.tzinfo is None else mf.astimezone(VET).isoformat()
        if not puntos or live_ts > puntos[-1]["timestamp"]:
            puntos.append({
                "compra": round(mc, 2),
                "venta": round(mv, 2),
                "liquidez": int(mercado.get("liquidez", 0) or 0),
                "timestamp": live_ts,
            })

    # Reducir carga del navegador en 30D.
    max_points = 800
    if len(puntos) > max_points:
        last_point = puntos[-1]
        step = max(1, len(puntos) // max_points)
        puntos = puntos[::step]
        if not puntos or puntos[-1]["timestamp"] != last_point["timestamp"]:
            puntos.append(last_point)

    return {"ok": True, "period": period, "count": len(puntos), "data": puntos}


VENBOT_AI_SYSTEM = """Eres Venbot AI, un asistente conversacional avanzado en español. Eres el copiloto del usuario: puedes conversar sobre temas generales, explicar conceptos, ayudar con cálculos, planificación y razonamiento, y también analizar el mercado P2P USDT/VES cuando el usuario lo pida. Cuando hables del mercado, usa exclusivamente los datos reales del contexto de Venbot. Nunca inventes precios, tasas, liquidez, noticias o hechos actuales. Distingue dato observado, cálculo, estimación e interpretación. Regla semántica obligatoria: Comprar USDT = anuncios SELL de Binance (el usuario compra USDT); Vender USDT = anuncios BUY (el usuario vende USDT). Mantén continuidad con el historial. Responde de forma natural, útil y con suficiente profundidad; no te limites a respuestas de una línea. Si una pregunta no es de mercado, contéstala normalmente. No prometas ganancias ni certeza financiera."""


def _serializar_contexto_mercado():
    mercado = obtener_mercado_actual_db() or {}
    analisis = calcular_analisis_monitor("GENERAL")
    filas = obtener_estadisticas_db(limit=120, banco="GENERAL")
    hist = []
    for c, v, l, f in filas:
        hist.append({"compra": round(float(c),2), "venta": round(float(v),2), "liquidez": int(l or 0), "fecha": f.isoformat() if f else None})
    compra = float(mercado.get("compra",0) or 0); venta = float(mercado.get("venta",0) or 0)
    spread = venta-compra if compra and venta else 0
    return {
        "mercado_actual": {"comprar_usdt_sell": compra, "vender_usdt_buy": venta, "spread": round(spread,2), "spread_pct": round(spread/compra*100,2) if compra else 0, "liquidez": int(mercado.get("liquidez",0) or 0), "bcv_usd": mercado.get("bcv",0), "euro": mercado.get("eur",0), "timestamp": mercado.get("fecha").isoformat() if mercado.get("fecha") else None},
        "analisis_cuantitativo": analisis,
        "historial_general": hist[-120:]
    }


def _respuesta_gemini(prompt, model):
    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(system_instruction=VENBOT_AI_SYSTEM, temperature=0.55, max_output_tokens=1600),
    )
    return getattr(response, "text", None)


def _respuesta_openrouter(prompt):
    if not OPENROUTER_API_KEY:
        return None
    try:
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json", "HTTP-Referer": RENDER_EXTERNAL_URL or "https://p2p-monitor-binance.onrender.com", "X-Title": "Venbot"},
            json={"model": OPENROUTER_MODEL, "messages": [{"role": "system", "content": VENBOT_AI_SYSTEM}, {"role": "user", "content": prompt}], "temperature": 0.55, "max_tokens": 1600},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip() or None
    except Exception as e:
        logger.warning("OpenRouter fallback falló: %s", e)
        return None


def generar_respuesta_ia(mensaje, historial):
    if not GEMINI_API_KEY and not OPENROUTER_API_KEY:
        return "La IA conversacional todavía no está configurada en el servidor."
    import json
    contexto = _serializar_contexto_mercado()
    prev = []
    for h in (historial or [])[-20:]:
        role = "user" if str(h.get("role", "")).lower() in {"user", "human"} else "assistant"
        content = str(h.get("content", h.get("text", "")))[:5000]
        if content:
            prev.append({"role": role, "content": content})
    prompt = "CONTEXTO REAL DE VENBOT:\n" + json.dumps(contexto, ensure_ascii=False, default=str) + "\n\nHISTORIAL CONVERSACIONAL:\n" + json.dumps(prev, ensure_ascii=False) + "\n\nPREGUNTA ACTUAL:\n" + mensaje

    if GEMINI_API_KEY and genai is not None:
        modelos = [GEMINI_MODEL]
        for fallback in ("gemini-3.7-flash", "gemini-2.5-flash"):
            if fallback not in modelos:
                modelos.append(fallback)
        for model in modelos:
            try:
                text = _respuesta_gemini(prompt, model)
                if text:
                    return text.strip()
            except Exception as e:
                logger.warning("Gemini %s falló: %s", model, e)

    text = _respuesta_openrouter(prompt)
    if text:
        return text
    return "No pude generar una respuesta en este momento. Intenta de nuevo en unos segundos."


class AIChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=6000)
    history: list[dict] = Field(default_factory=list)


@app.post("/api/ai/chat")
async def ai_chat(payload: AIChatRequest):
    mensaje = payload.message.strip()
    respuesta = await asyncio.to_thread(generar_respuesta_ia, mensaje, payload.history)
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
