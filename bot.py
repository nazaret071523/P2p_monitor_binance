import os
import io
import asyncio
import logging
import time
import json
import threading
import secrets
import hashlib
import base64
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

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
from requests.adapters import HTTPAdapter
import numpy as np
import xgboost as xgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
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

# Market Data Layer Spot (solo datos públicos, sin API key).
SPOT_BASE_URL = os.getenv("SPOT_BASE_URL", "https://data-api.binance.vision").strip().rstrip("/")
SPOT_SYMBOLS = tuple(dict.fromkeys(
    x.strip().upper() for x in os.getenv("SPOT_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,SUIUSDT,AAVEUSDT,UNIUSDT,KSMUSDT,ZECUSDT,XRPUSDT").split(",") if x.strip()
)) or ("BTCUSDT", "ETHUSDT", "SOLUSDT")
SPOT_REFRESH_SECONDS = max(10, int(os.getenv("SPOT_REFRESH_SECONDS", "20")))
SPOT_REQUEST_TIMEOUT = max(3, int(os.getenv("SPOT_REQUEST_TIMEOUT", "8")))

# Fundación de producto: los pagos son externos y se habilitan por política de mercado.
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "VE")
VENBOT_COMMUNITY_URL = os.getenv("VENBOT_COMMUNITY_URL", "")
VENBOT_SUPPORT_URL = os.getenv("VENBOT_SUPPORT_URL", "")
VENBOT_BOT_URL = os.getenv("VENBOT_BOT_URL", "").strip().upper() or "VE"
BETA_PREMIUM_ACCESS = os.getenv("BETA_PREMIUM_ACCESS", "false").strip().lower() in {"1", "true", "yes", "on"}
BETA_VIP_ACCESS = os.getenv("BETA_VIP_ACCESS", "false").strip().lower() in {"1", "true", "yes", "on"}
EXTERNAL_BILLING_URL = os.getenv("EXTERNAL_BILLING_URL", "").strip()
BILLING_WEBHOOK_SECRET = os.getenv("BILLING_WEBHOOK_SECRET", "").strip()
PREMIUM_PRICE_LABEL = os.getenv("PREMIUM_PRICE_LABEL", "").strip()
VIP_PRICE_LABEL = os.getenv("VIP_PRICE_LABEL", "").strip()
PREMIUM_DURATION_DAYS = max(1, int(os.getenv("PREMIUM_DURATION_DAYS", "30")))
VIP_DURATION_DAYS = max(1, int(os.getenv("VIP_DURATION_DAYS", "30")))
PRIVACY_CONTACT_EMAIL = os.getenv("PRIVACY_CONTACT_EMAIL", "").strip()

# Billing manual beta: sin pasarela ni comisión. El usuario paga y el administrador valida.
BILLING_PROVIDER = os.getenv("BILLING_PROVIDER", "manual").strip().lower()
PABILO_API_KEY = os.getenv("PABILO_API_KEY", "").strip()
PABILO_USER_BANK_ID = os.getenv("PABILO_USER_BANK_ID", "").strip()
PABILO_WEBHOOK_SECRET = os.getenv("PABILO_WEBHOOK_SECRET", "").strip()
PABILO_EXPIRATION_MINUTES = max(5, int(os.getenv("PABILO_EXPIRATION_MINUTES", "60")))
PABILO_RATE_EXPIRATION_MINUTES = max(1, int(os.getenv("PABILO_RATE_EXPIRATION_MINUTES", "60")))
PREMIUM_PRICE_USDT = float(os.getenv("PREMIUM_PRICE_USDT", "0"))
VIP_PRICE_USDT = float(os.getenv("VIP_PRICE_USDT", "0"))
PREMIUM_PRICE_VES = float(os.getenv("PREMIUM_PRICE_VES", "0"))
VIP_PRICE_VES = float(os.getenv("VIP_PRICE_VES", "0"))
BILLING_ADMIN_TELEGRAM_CHAT_ID = os.getenv("BILLING_ADMIN_TELEGRAM_CHAT_ID", "").strip()
MANUAL_USDT_PAY_ID = os.getenv("MANUAL_USDT_PAY_ID", "").strip()
MANUAL_BS_BANK = os.getenv("MANUAL_BS_BANK", "").strip()
MANUAL_BS_ACCOUNT = os.getenv("MANUAL_BS_ACCOUNT", "").strip()
MANUAL_BS_HOLDER = os.getenv("MANUAL_BS_HOLDER", "").strip()
MANUAL_BS_PHONE = os.getenv("MANUAL_BS_PHONE", "").strip()
MANUAL_BS_ID = os.getenv("MANUAL_BS_ID", "").strip()
MANUAL_ORDER_EXPIRATION_HOURS = max(1, int(os.getenv("MANUAL_ORDER_EXPIRATION_HOURS", "24")))

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
# Motor de Alertas Inteligentes v1: detecta eventos estadísticos y los persiste
# para que no dependan de la memoria del proceso ni se repitan en cada ciclo.
SMART_ALERTS_ENABLED = os.getenv("SMART_ALERTS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
SMART_ALERT_COOLDOWN_SECONDS = max(120, int(os.getenv("SMART_ALERT_COOLDOWN_SECONDS", "1800")))
SMART_ALERT_HIGH_SPREAD_PCT = max(0.50, float(os.getenv("SMART_ALERT_HIGH_SPREAD_PCT", "1.50")))
SMART_ALERT_FAST_MOVE_5M_PCT = max(0.10, float(os.getenv("SMART_ALERT_FAST_MOVE_5M_PCT", "0.35")))
SMART_ALERT_BREAKOUT_BUFFER_PCT = max(0.01, float(os.getenv("SMART_ALERT_BREAKOUT_BUFFER_PCT", "0.05")))
# Calibración Quant 24H: primera fase de ajuste tras acumular al menos un día de datos.
QUANT_24H_CALIBRATION_ENABLED = os.getenv("QUANT_24H_CALIBRATION_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
QUANT_24H_TREND_WEIGHT_MAX = max(0.0, min(0.25, float(os.getenv("QUANT_24H_TREND_WEIGHT_MAX", "0.12"))))
# Seguimiento de predicciones: registra una lectura cada pocos minutos y evalúa
# sus horizontes contra datos P2P reales posteriores. No modifica el motor Quant.
PREDICTION_TRACKING_ENABLED = os.getenv("PREDICTION_TRACKING_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
PREDICTION_TRACKING_INTERVAL_SECONDS = max(60, int(os.getenv("PREDICTION_TRACKING_INTERVAL_SECONDS", "300")))
PREDICTION_EVAL_TOLERANCE_MINUTES = max(2, int(os.getenv("PREDICTION_EVAL_TOLERANCE_MINUTES", "20")))
_LAST_PREDICTION_TRACKING_TS = 0.0
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
SPOT_CACHE = {"value": {}, "expires": 0.0}
SPOT_LOCK = threading.Lock()
_LAST_SPOT_COLLECTION_TS = 0.0


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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_users (
                        id BIGSERIAL PRIMARY KEY,
                        external_user_id TEXT UNIQUE NOT NULL,
                        country_code TEXT NOT NULL DEFAULT 'VE',
                        plan_code TEXT NOT NULL DEFAULT 'FREE',
                        status TEXT NOT NULL DEFAULT 'active',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        deleted_at TIMESTAMPTZ
                    );
                """)
                cur.execute("""
                    ALTER TABLE venbot_users ADD COLUMN IF NOT EXISTS username TEXT;
                    ALTER TABLE venbot_users ADD COLUMN IF NOT EXISTS password_hash TEXT;
                    ALTER TABLE venbot_users ADD COLUMN IF NOT EXISTS telegram_chat_id BIGINT;
                    ALTER TABLE venbot_users ADD COLUMN IF NOT EXISTS plan_expires_at TIMESTAMPTZ;
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_venbot_users_username
                    ON venbot_users(username) WHERE username IS NOT NULL;
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_venbot_users_telegram
                    ON venbot_users(telegram_chat_id) WHERE telegram_chat_id IS NOT NULL;
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_sessions (
                        id BIGSERIAL PRIMARY KEY,
                        external_user_id TEXT NOT NULL,
                        token_hash TEXT UNIQUE NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMPTZ NOT NULL,
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        revoked_at TIMESTAMPTZ
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_sessions_user
                    ON venbot_sessions(external_user_id, expires_at DESC);
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_consents (
                        id BIGSERIAL PRIMARY KEY,
                        external_user_id TEXT NOT NULL,
                        consent_type TEXT NOT NULL,
                        version TEXT NOT NULL,
                        granted BOOLEAN NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(external_user_id, consent_type, version)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_billing_events (
                        id BIGSERIAL PRIMARY KEY,
                        external_user_id TEXT NOT NULL,
                        country_code TEXT NOT NULL,
                        plan_code TEXT NOT NULL,
                        provider TEXT,
                        external_reference TEXT,
                        event_type TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        payload JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_feature_flags (
                        feature_key TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        min_plan TEXT NOT NULL DEFAULT 'FREE',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_usage_daily (
                        external_user_id TEXT NOT NULL,
                        usage_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        ai_requests INTEGER NOT NULL DEFAULT 0,
                        alert_count INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY(external_user_id, usage_date)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_alert_events (
                        id BIGSERIAL PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        banco TEXT NOT NULL DEFAULT 'GENERAL',
                        severity TEXT NOT NULL DEFAULT 'info',
                        title TEXT NOT NULL,
                        message TEXT NOT NULL,
                        signature TEXT NOT NULL,
                        payload JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_alert_events_created
                    ON venbot_alert_events(created_at DESC);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_alert_events_signature_created
                    ON venbot_alert_events(signature, created_at DESC);
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_alert_rules (
                        id BIGSERIAL PRIMARY KEY,
                        external_user_id TEXT NOT NULL,
                        banco TEXT NOT NULL DEFAULT 'GENERAL',
                        rule_type TEXT NOT NULL,
                        target_value DOUBLE PRECISION NOT NULL,
                        direction TEXT,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        cooldown_seconds INTEGER NOT NULL DEFAULT 1800,
                        telegram_chat_id BIGINT,
                        last_triggered_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    ALTER TABLE venbot_alert_rules ADD COLUMN IF NOT EXISTS telegram_chat_id BIGINT;
                    ALTER TABLE venbot_alert_rules ADD COLUMN IF NOT EXISTS last_triggered_at TIMESTAMPTZ;
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_alert_rules_user_enabled
                    ON venbot_alert_rules(external_user_id, enabled);
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_prediction_events (
                        id BIGSERIAL PRIMARY KEY,
                        banco TEXT NOT NULL DEFAULT 'GENERAL',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        actual_compra DOUBLE PRECISION NOT NULL,
                        actual_venta DOUBLE PRECISION NOT NULL,
                        actual_mid DOUBLE PRECISION NOT NULL,
                        pred_compra_1h DOUBLE PRECISION, pred_venta_1h DOUBLE PRECISION,
                        pred_compra_3h DOUBLE PRECISION, pred_venta_3h DOUBLE PRECISION,
                        pred_compra_7h DOUBLE PRECISION, pred_venta_7h DOUBLE PRECISION,
                        pred_compra_24h DOUBLE PRECISION, pred_venta_24h DOUBLE PRECISION,
                        pred_mid_7h DOUBLE PRECISION, forecast_low_mid DOUBLE PRECISION, forecast_high_mid DOUBLE PRECISION,
                        tendencia TEXT, regimen TEXT, confidence INTEGER,
                        support_7h DOUBLE PRECISION, resistance_7h DOUBLE PRECISION, volatility_pct DOUBLE PRECISION,
                        evaluated_1h_at TIMESTAMPTZ, evaluated_3h_at TIMESTAMPTZ, evaluated_7h_at TIMESTAMPTZ, evaluated_24h_at TIMESTAMPTZ,
                        actual_mid_1h DOUBLE PRECISION, actual_mid_3h DOUBLE PRECISION, actual_mid_7h DOUBLE PRECISION, actual_mid_24h DOUBLE PRECISION,
                        error_pct_1h DOUBLE PRECISION, error_pct_3h DOUBLE PRECISION, error_pct_7h DOUBLE PRECISION, error_pct_24h DOUBLE PRECISION,
                        direction_correct_7h BOOLEAN, payload JSONB
                    );
                    CREATE INDEX IF NOT EXISTS idx_venbot_prediction_bank_created
                    ON venbot_prediction_events(banco, created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_venbot_prediction_due
                    ON venbot_prediction_events(created_at DESC, evaluated_7h_at);
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS spot_market_snapshots (
                        id BIGSERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL,
                        bid DOUBLE PRECISION,
                        ask DOUBLE PRECISION,
                        change_24h_pct DOUBLE PRECISION,
                        quote_volume_24h DOUBLE PRECISION,
                        fecha TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_spot_market_symbol_fecha
                    ON spot_market_snapshots(symbol, fecha DESC);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_users_plan ON venbot_users(plan_code, status);
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venbot_billing_orders (
                        id BIGSERIAL PRIMARY KEY, order_id TEXT UNIQUE NOT NULL, external_user_id TEXT NOT NULL, telegram_chat_id BIGINT,
                        plan_code TEXT NOT NULL, base_amount_usdt DOUBLE PRECISION NOT NULL, pay_currency TEXT NOT NULL, provider_currency TEXT NOT NULL,
                        quoted_amount DOUBLE PRECISION NOT NULL, quote_rate DOUBLE PRECISION, quote_source TEXT, provider TEXT NOT NULL, provider_order_id TEXT,
                        checkout_url TEXT, status TEXT NOT NULL DEFAULT 'PAYMENT_PENDING', expires_at TIMESTAMPTZ, paid_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, payload JSONB
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_venbot_billing_orders_provider_id ON venbot_billing_orders(provider, provider_order_id) WHERE provider_order_id IS NOT NULL;
                    CREATE INDEX IF NOT EXISTS idx_venbot_billing_orders_user_created ON venbot_billing_orders(external_user_id, created_at DESC);
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS proof_file_id TEXT;
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS proof_reference TEXT;
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ;
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS reviewed_by TEXT;
                    ALTER TABLE venbot_billing_orders ADD COLUMN IF NOT EXISTS rejection_reason TEXT;
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_venbot_billing_user ON venbot_billing_events(external_user_id, created_at DESC);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_venbot_billing_reference
                    ON venbot_billing_events(external_reference, event_type) WHERE external_reference IS NOT NULL;
                """)
        logger.info("Base de datos inicializada correctamente.")
    except Exception as e:
        logger.exception("Error inicializando DB: %s", e)


def registrar_prediccion_tracking(banco, actual_compra, actual_venta, datos):
    """Guarda una predicción para poder medirla después contra el mercado real."""
    if not DATABASE_URL or not PREDICTION_TRACKING_ENABLED:
        return False
    try:
        mid = (float(actual_compra) + float(actual_venta)) / 2.0
        # Para 1H/3H/24H usamos el mismo escenario central del motor 7H como
        # baseline explícito hasta que el tracking tenga modelos específicos.
        pred_c = float(datos.get("pred_compra", 0) or 0)
        pred_v = float(datos.get("pred_venta", 0) or 0)
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO venbot_prediction_events
                    (banco,actual_compra,actual_venta,actual_mid,
                     pred_compra_1h,pred_venta_1h,pred_compra_3h,pred_venta_3h,
                     pred_compra_7h,pred_venta_7h,pred_compra_24h,pred_venta_24h,
                     pred_mid_7h,forecast_low_mid,forecast_high_mid,tendencia,regimen,
                     confidence,support_7h,resistance_7h,volatility_pct,payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    banco, float(actual_compra), float(actual_venta), mid,
                    pred_c, pred_v, pred_c, pred_v, pred_c, pred_v, pred_c, pred_v,
                    float(datos.get("pred_mid", mid) or mid),
                    float(datos.get("forecast_low_mid", mid) or mid),
                    float(datos.get("forecast_high_mid", mid) or mid),
                    datos.get("tendencia"), datos.get("regimen", {}).get("regimen") if isinstance(datos.get("regimen"), dict) else str(datos.get("regimen") or ""),
                    int(datos.get("confianza", 0) or 0),
                    float(datos.get("soporte_7h", mid) or mid), float(datos.get("resistencia_7h", mid) or mid),
                    float(datos.get("volatilidad_pct", 0) or 0),
                    json.dumps({"calibracion_24h": datos.get("calibracion_24h", {}), "delta_7h_pct": datos.get("delta_7h_pct", 0)}, ensure_ascii=False),
                ))
        return True
    except Exception as e:
        logger.warning("No se pudo registrar predicción %s: %s", banco, e)
        return False


def _buscar_muestra_futura(banco, objetivo, tolerance_minutes=None):
    """Obtiene la primera muestra real posterior al horizonte; evita usar datos previos."""
    if not DATABASE_URL:
        return None
    tol = int(tolerance_minutes or PREDICTION_EVAL_TOLERANCE_MINUTES)
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT compra, venta, fecha FROM muestras_p2p
                    WHERE banco=%s
                      AND fecha >= %s
                      AND fecha <= %s
                    ORDER BY fecha ASC LIMIT 1
                """, (banco, objetivo, objetivo + timedelta(minutes=tol)))
                row = cur.fetchone()
                if not row:
                    return None
                c, v, f = row
                return {"compra": float(c), "venta": float(v), "mid": (float(c)+float(v))/2.0, "fecha": f}
    except Exception as e:
        logger.warning("No se pudo buscar muestra futura %s: %s", banco, e)
        return None


def evaluar_predicciones_pendientes(limit=100):
    """Evalúa horizontes vencidos contra muestras posteriores reales."""
    if not DATABASE_URL or not PREDICTION_TRACKING_ENABLED:
        return {"evaluated": 0}
    horizons = [("1h", 1), ("3h", 3), ("7h", 7), ("24h", 24)]
    evaluated = 0
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id,banco,created_at,pred_mid_7h,pred_compra_7h,pred_venta_7h,
                           evaluated_1h_at,evaluated_3h_at,evaluated_7h_at,evaluated_24h_at
                    FROM venbot_prediction_events
                    WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '14 days'
                      AND (evaluated_1h_at IS NULL OR evaluated_3h_at IS NULL OR evaluated_7h_at IS NULL OR evaluated_24h_at IS NULL)
                    ORDER BY created_at ASC LIMIT %s
                """, (int(limit),))
                rows = cur.fetchall()
                for row in rows:
                    pid,banco,created_at,pred_mid,pred_c,pred_v,*evals = row
                    updates = {}
                    for idx,(label,hours) in enumerate(horizons):
                        if evals[idx] is not None:
                            continue
                        objetivo = created_at + timedelta(hours=hours)
                        if datetime.now(pytz.UTC) < objetivo:
                            continue
                        actual = _buscar_muestra_futura(banco, objetivo)
                        if not actual:
                            continue
                        actual_mid = actual["mid"]
                        # Baseline 7H central para todos los horizontes hasta contar
                        # con suficiente historial para entrenar horizontes propios.
                        pred = float(pred_mid or 0)
                        err = ((actual_mid - pred) / pred * 100.0) if pred else None
                        updates[f"actual_mid_{label}"] = actual_mid
                        updates[f"error_pct_{label}"] = err
                        updates[f"evaluated_{label}_at"] = actual["fecha"]
                    if "actual_mid_7h" in updates:
                        pred = float(pred_mid or 0)
                        actual7 = float(updates["actual_mid_7h"] or 0)
                        direction_correct = None
                        # Compara el signo de la predicción contra el movimiento desde el origen.
                        origin = None
                        cur.execute("SELECT actual_mid FROM venbot_prediction_events WHERE id=%s", (pid,))
                        rr = cur.fetchone()
                        if rr:
                            origin = float(rr[0] or 0)
                        if origin and pred:
                            direction_correct = (pred-origin == 0 and actual7-origin == 0) or ((pred-origin) * (actual7-origin) > 0)
                        updates["direction_correct_7h"] = direction_correct
                    if updates:
                        sets=[]; vals=[]
                        for key,val in updates.items():
                            sets.append(f"{key}=%s"); vals.append(val)
                        vals.append(pid)
                        cur.execute(f"UPDATE venbot_prediction_events SET {', '.join(sets)} WHERE id=%s", vals)
                        evaluated += 1
        return {"evaluated": evaluated}
    except Exception as e:
        logger.warning("Evaluación de predicciones falló: %s", e)
        return {"evaluated": 0, "error": "tracking temporalmente no disponible"}


def obtener_prediction_performance(banco="GENERAL", limit=100):
    """Resumen auditable del tracking ya evaluado; no altera el motor."""
    if not DATABASE_URL:
        return {"ok": False, "message": "Sin base de datos"}
    banco = (banco or "GENERAL").upper().strip()
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT error_pct_1h,error_pct_3h,error_pct_7h,error_pct_24h,direction_correct_7h,regimen,confidence,created_at
                    FROM venbot_prediction_events
                    WHERE banco=%s ORDER BY created_at DESC LIMIT %s
                """, (banco,int(limit)))
                rows=cur.fetchall()
        def stats(idx):
            vals=[float(r[idx]) for r in rows if r[idx] is not None]
            if not vals: return {"evaluated":0,"mean_abs_error_pct":None,"median_abs_error_pct":None}
            return {"evaluated":len(vals),"mean_abs_error_pct":round(float(np.mean(np.abs(vals))),4),"median_abs_error_pct":round(float(np.median(np.abs(vals))),4)}
        decisive=[bool(r[4]) for r in rows if r[4] is not None]
        by_reg={}
        for r in rows:
            reg=str(r[5] or "SIN_DATOS")
            if r[4] is not None: by_reg.setdefault(reg,[]).append(bool(r[4]))
        return {"ok":True,"bank":banco,"tracked":len(rows),
                "horizons":{"1h":stats(0),"3h":stats(1),"7h":stats(2),"24h":stats(3)},
                "direction_accuracy_7h_pct":round(sum(decisive)/len(decisive)*100,2) if decisive else None,
                "by_regime":{"regimen":{k:round(sum(v)/len(v)*100,2) for k,v in by_reg.items()}},
                "last_prediction_at":rows[0][7].isoformat() if rows else None}
    except Exception as e:
        logger.warning("Performance tracking falló: %s", e)
        return {"ok":False,"message":"Performance temporalmente no disponible"}


def evaluar_senal_operativa(datos, actual_compra, actual_venta):
    """Capa interpretativa conservadora sobre el motor Quant existente."""
    try:
        mid=(float(actual_compra)+float(actual_venta))/2.0
        support=float(datos.get("soporte_7h",mid) or mid)
        resistance=float(datos.get("resistencia_7h",mid) or mid)
        pos=float(datos.get("posicion_rango_7h",50) or 50)
        trend=str(datos.get("tendencia", ""))
        conf=int(datos.get("confianza",0) or 0)
        vol=float(datos.get("volatilidad_pct",0) or 0)
        spread=float(datos.get("spread_pct",0) or 0)
        if conf < 45:
            return {"code":"NO_SIGNAL","label":"⚪ SIN SEÑAL SUFICIENTE","reason":"La calidad estadística todavía es insuficiente para una lectura operativa."}
        if "ALCISTA" in trend and pos < 45:
            return {"code":"BUY_ZONE","label":"🟢 ZONA FAVORABLE / SESGO ALCISTA","reason":"Momentum alcista con espacio respecto a la parte alta del rango."}
        if "BAJISTA" in trend and pos > 55:
            return {"code":"SELL_ZONE","label":"🔴 ZONA DE PRECAUCIÓN / SESGO BAJISTA","reason":"Momentum bajista con precio elevado dentro del rango reciente."}
        if pos <= 25 and mid <= support * 1.0025:
            return {"code":"WATCH_BUY","label":"🟡 VIGILAR ZONA DE COMPRA","reason":"Precio cercano al soporte; falta confirmación direccional."}
        if pos >= 75 and mid >= resistance * 0.9975:
            return {"code":"WATCH_SELL","label":"🟡 VIGILAR ZONA DE VENTA","reason":"Precio cercano a resistencia; falta confirmación direccional."}
        if "RANGO" in trend:
            return {"code":"WAIT","label":"🟡 ESPERAR / NO PERSEGUIR PRECIO","reason":"El modelo identifica un mercado lateral y no confirma ruptura."}
        if vol > 0.08 or spread > 1.2:
            return {"code":"CAUTION","label":"🟠 PRECAUCIÓN","reason":"Volatilidad o spread elevados reducen la calidad de una entrada inmediata."}
        return {"code":"WAIT","label":"🟡 ESPERAR CONFIRMACIÓN","reason":"Las señales no están suficientemente alineadas para una lectura operativa fuerte."}
    except Exception:
        return {"code":"NO_SIGNAL","label":"⚪ SIN SEÑAL","reason":"No se pudo calcular la capa operativa."}


def registrar_evento_alerta(event_type, banco, severity, title, message, signature, payload=None, cooldown_seconds=None):
    """Persiste una alerta solo si no existe el mismo evento dentro del cooldown."""
    if not DATABASE_URL:
        return False
    cooldown = int(cooldown_seconds or SMART_ALERT_COOLDOWN_SECONDS)
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM venbot_alert_events
                    WHERE signature=%s AND created_at >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 second')
                    LIMIT 1
                """, (signature, cooldown))
                if cur.fetchone():
                    return False
                cur.execute("""
                    INSERT INTO venbot_alert_events
                    (event_type,banco,severity,title,message,signature,payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (event_type,banco,severity,title,message,signature,json.dumps(payload or {}, ensure_ascii=False)))
        return True
    except Exception as e:
        logger.warning("No se pudo persistir alerta inteligente: %s", e)
        return False


def obtener_eventos_alerta(limit=50, banco="GENERAL"):
    if not DATABASE_URL:
        return []
    limit = max(1, min(int(limit), 200))
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                if banco == "GENERAL":
                    cur.execute("""
                        SELECT id,event_type,banco,severity,title,message,payload,created_at
                        FROM venbot_alert_events ORDER BY created_at DESC LIMIT %s
                    """, (limit,))
                else:
                    cur.execute("""
                        SELECT id,event_type,banco,severity,title,message,payload,created_at
                        FROM venbot_alert_events WHERE banco=%s ORDER BY created_at DESC LIMIT %s
                    """, (banco, limit))
                rows = cur.fetchall()
        out=[]
        for r in rows:
            out.append({"id":r[0],"event_type":r[1],"banco":r[2],"severity":r[3],"title":r[4],"message":r[5],"payload":r[6] or {},"created_at":r[7].astimezone(VET).isoformat() if getattr(r[7], "tzinfo", None) else str(r[7])})
        return out
    except Exception as e:
        logger.warning("No se pudieron leer alertas: %s", e)
        return []


def evaluar_alertas_inteligentes(mercado, datos, banco="GENERAL"):
    """Genera eventos estadísticos conservadores. No modifica el motor Quant ni sus predicciones."""
    if not SMART_ALERTS_ENABLED or not mercado or not datos:
        return []
    compra=float(mercado.get("compra") or 0)
    venta=float(mercado.get("venta") or 0)
    if compra <= 0 or venta <= 0:
        return []
    spread_pct=float(datos.get("spread_pct") or 0)
    cambios=datos.get("cambios") or {}
    tendencia=str(datos.get("tendencia") or "")
    niveles=datos.get("niveles_dinamicos") or {}
    soporte=float(niveles.get("soporte") or datos.get("soporte_7h") or 0)
    resistencia=float(niveles.get("resistencia") or datos.get("resistencia_7h") or 0)
    mid=(compra+venta)/2.0
    events=[]

    if spread_pct >= SMART_ALERT_HIGH_SPREAD_PCT:
        sig=f"spread_high:{banco}"
        msg=f"Spread elevado en {banco}: {spread_pct:.2f}% (Comprar {compra:.2f} Bs / Vender {venta:.2f} Bs)."
        if registrar_evento_alerta("spread_high", banco, "warning", "Spread elevado", msg, sig):
            events.append(("warning", "⚠️ SPREAD ELEVADO", msg))

    move5=cambios.get("5m")
    if move5 is not None and abs(float(move5)) >= SMART_ALERT_FAST_MOVE_5M_PCT:
        direction="alcista" if float(move5)>0 else "bajista"
        sig=f"fast_move_5m:{banco}:{direction}"
        msg=f"Movimiento rápido de 5m: {float(move5):+.3f}% ({direction}). Comprar {compra:.2f} Bs / Vender {venta:.2f} Bs."
        if registrar_evento_alerta("fast_move_5m", banco, "warning", "Movimiento rápido", msg, sig):
            events.append(("warning", "🚨 MOVIMIENTO RÁPIDO", msg))

    if soporte > 0 and mid < soporte * (1.0 - SMART_ALERT_BREAKOUT_BUFFER_PCT/100.0):
        sig=f"break_support:{banco}"
        msg=f"El midpoint {mid:.2f} Bs está por debajo del soporte dinámico {soporte:.2f} Bs."
        if registrar_evento_alerta("break_support", banco, "critical", "Ruptura de soporte", msg, sig):
            events.append(("critical", "🔻 RUPTURA DE SOPORTE", msg))
    elif resistencia > 0 and mid > resistencia * (1.0 + SMART_ALERT_BREAKOUT_BUFFER_PCT/100.0):
        sig=f"break_resistance:{banco}"
        msg=f"El midpoint {mid:.2f} Bs está por encima de la resistencia dinámica {resistencia:.2f} Bs."
        if registrar_evento_alerta("break_resistance", banco, "critical", "Ruptura de resistencia", msg, sig):
            events.append(("critical", "🔺 RUPTURA DE RESISTENCIA", msg))

    return events


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
# El recolector P2P hace varias lecturas en paralelo. Un pool mayor evita
# descartar conexiones reutilizables bajo carga sin aumentar el número de consultas.
_HTTP_ADAPTER = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=1, pool_block=False)
HTTP.mount("https://", _HTTP_ADAPTER)
HTTP.mount("http://", _HTTP_ADAPTER)


def _float_positivo(value):
    try:
        x = float(value)
        return x if x > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


# ==========================================
# MARKET DATA LAYER: BINANCE SPOT
# ==========================================
def _normalizar_spot_symbol(symbol):
    sym = str(symbol or "").strip().upper()
    if not sym or len(sym) > 24 or not sym.isalnum():
        raise ValueError("Símbolo Spot inválido")
    return sym


def obtener_spot_binance(symbol):
    """Obtiene una lectura pública Spot desde Binance Market Data Only.

    No usa credenciales ni endpoints de cuenta/trading. Devuelve None si la
    fuente no responde para que el resto de Venbot continúe funcionando.
    """
    sym = _normalizar_spot_symbol(symbol)
    try:
        r = HTTP.get(
            f"{SPOT_BASE_URL}/api/v3/ticker/24hr",
            params={"symbol": sym},
            timeout=SPOT_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        price = _float_positivo(data.get("lastPrice"))
        if price <= 0:
            return None
        bid = _float_positivo(data.get("bidPrice"))
        ask = _float_positivo(data.get("askPrice"))
        try:
            change_pct = float(data.get("priceChangePercent") or 0.0)
        except Exception:
            change_pct = 0.0
        try:
            quote_volume = float(data.get("quoteVolume") or 0.0)
        except Exception:
            quote_volume = 0.0
        return {
            "symbol": sym,
            "price": price,
            "bid": bid,
            "ask": ask,
            "spread": max(0.0, ask - bid) if bid and ask else 0.0,
            "change_24h_pct": change_pct,
            "quote_volume_24h": quote_volume,
            "timestamp": datetime.now(VET),
            "source": "Binance Spot public market data",
        }
    except Exception as e:
        logger.warning("Binance Spot %s no disponible: %s", sym, e)
        return None


def guardar_spot_snapshot_db(item):
    if not DATABASE_URL or not item or not item.get("price"):
        return False
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO spot_market_snapshots
                    (symbol, price, bid, ask, change_24h_pct, quote_volume_24h, fecha)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        item["symbol"], float(item["price"]), float(item.get("bid") or 0),
                        float(item.get("ask") or 0), float(item.get("change_24h_pct") or 0),
                        float(item.get("quote_volume_24h") or 0), item.get("timestamp") or datetime.now(VET),
                    ),
                )
        return True
    except Exception as e:
        logger.warning("No se pudo guardar Spot %s: %s", item.get("symbol"), e)
        return False


def obtener_spot_snapshot_db(symbol):
    if not DATABASE_URL:
        return None
    sym = _normalizar_spot_symbol(symbol)
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT symbol, price, bid, ask, change_24h_pct, quote_volume_24h, fecha
                       FROM spot_market_snapshots WHERE symbol=%s ORDER BY fecha DESC LIMIT 1""",
                    (sym,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "symbol": row[0], "price": float(row[1]), "bid": float(row[2] or 0),
            "ask": float(row[3] or 0), "change_24h_pct": float(row[4] or 0),
            "quote_volume_24h": float(row[5] or 0), "timestamp": row[6],
            "source": "última lectura Spot real persistida",
        }
    except Exception as e:
        logger.warning("No se pudo leer Spot %s: %s", sym, e)
        return None


def recolectar_spot(symbols=None, persist=True):
    symbols = tuple(symbols or SPOT_SYMBOLS)
    results = {}
    # Pocos símbolos por defecto; paralelizar reduce latencia sin abrir un pool enorme.
    with ThreadPoolExecutor(max_workers=min(6, max(1, len(symbols)))) as ex:
        futures = {sym: ex.submit(obtener_spot_binance, sym) for sym in symbols}
        for sym, fut in futures.items():
            try:
                item = fut.result(timeout=SPOT_REQUEST_TIMEOUT + 2)
            except Exception:
                item = None
            if item is None:
                item = obtener_spot_snapshot_db(sym)
            if item:
                results[sym] = item
                if persist and item.get("source") == "Binance Spot public market data":
                    guardar_spot_snapshot_db(item)
    with SPOT_LOCK:
        SPOT_CACHE["value"] = results
        SPOT_CACHE["expires"] = time.monotonic() + min(10.0, SPOT_REFRESH_SECONDS / 2.0)
    return results


def obtener_spot_klines(symbol, interval="5m", limit=170):
    sym = _normalizar_spot_symbol(symbol)
    allowed = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
    if interval not in allowed:
        raise ValueError("Intervalo Spot no permitido")
    limit = max(10, min(500, int(limit)))
    r = HTTP.get(
        f"{SPOT_BASE_URL}/api/v3/klines",
        params={"symbol": sym, "interval": interval, "limit": limit},
        timeout=SPOT_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    out = []
    for k in r.json():
        if not isinstance(k, list) or len(k) < 7:
            continue
        out.append({
            "x": int(k[0]), "o": float(k[1]), "h": float(k[2]),
            "l": float(k[3]), "c": float(k[4]), "volume": float(k[5]),
            "close_time": int(k[6]),
        })
    return out


def _spot_context_for_quant():
    with SPOT_LOCK:
        cached = dict(SPOT_CACHE.get("value") or {})
    if not cached:
        cached = recolectar_spot(persist=False)
    return {
        sym: {
            "price": round(float(v.get("price") or 0), 8),
            "change_24h_pct": round(float(v.get("change_24h_pct") or 0), 3),
            "bid": round(float(v.get("bid") or 0), 8),
            "ask": round(float(v.get("ask") or 0), 8),
        }
        for sym, v in cached.items() if v
    }


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


def clasificar_regimen_quant(returns, drift_h, rango_pct, regression_r2):
    """Clasifica el régimen observable sin convertirlo en una predicción garantizada."""
    try:
        arr = np.asarray(returns, dtype=float)
        vol = float(np.std(arr)) if len(arr) else 0.0
        typical = float(np.median(np.abs(arr))) if len(arr) else 0.0
        trend_strength = abs(float(drift_h))
        noise_floor = max(0.008, typical * 1.2, vol * 0.8)
        if trend_strength > noise_floor * 1.8 and regression_r2 >= 0.35:
            regime = "ALCISTA" if drift_h > 0 else "BAJISTA"
            return {"regimen": regime, "fuerza": round(min(1.0, trend_strength / max(noise_floor * 4.0, 1e-9)), 3), "volatilidad": round(vol, 5)}
        if rango_pct >= 0.55 and trend_strength <= noise_floor * 1.2:
            return {"regimen": "RANGO", "fuerza": round(min(1.0, rango_pct / 2.0), 3), "volatilidad": round(vol, 5)}
        return {"regimen": "TRANSICION", "fuerza": round(min(1.0, trend_strength / max(noise_floor * 3.0, 1e-9)), 3), "volatilidad": round(vol, 5)}
    except Exception:
        return {"regimen": "TRANSICION", "fuerza": 0.0, "volatilidad": 0.0}


def niveles_dinamicos_quant(recent, mid_actual, volatilidad_pct):
    """Niveles robustos: extremos + cuantiles, para evitar que un solo outlier domine."""
    try:
        arr = np.asarray(recent, dtype=float)
        if len(arr) < 5:
            return {"soporte": float(mid_actual), "resistencia": float(mid_actual), "p25": float(mid_actual), "p75": float(mid_actual)}
        p10, p25, p75, p90 = [float(x) for x in np.percentile(arr, [10, 25, 75, 90])]
        margen = max(mid_actual * 0.0005, mid_actual * float(volatilidad_pct or 0) / 100.0 * 1.5)
        soporte = min(p25, mid_actual - margen) if mid_actual > p25 else p10
        resistencia = max(p75, mid_actual + margen) if mid_actual < p75 else p90
        if soporte >= resistencia:
            soporte, resistencia = p10, p90
        return {"soporte": round(soporte, 2), "resistencia": round(resistencia, 2), "p25": round(p25, 2), "p75": round(p75, 2)}
    except Exception:
        return {"soporte": round(float(mid_actual), 2), "resistencia": round(float(mid_actual), 2), "p25": round(float(mid_actual), 2), "p75": round(float(mid_actual), 2)}


def backtest_quant_7h(banco_filtro="GENERAL", max_evaluaciones=24, spacing_minutes=60):
    """Backtest 7H con separación temporal entre orígenes.

    La predicción de cada origen usa únicamente datos disponibles hasta ese
    instante. Los orígenes se seleccionan desde el tramo más reciente que ya
    tiene 7H de futuro observado y se separan por ``spacing_minutes`` para
    evitar la concentración artificial de evaluaciones consecutivas.

    Nota metodológica: con una separación menor a 7H, las ventanas futuras
    todavía se solapan. Por eso el resultado se etiqueta como ``temporally_spaced``
    y no como muestras estadísticamente independientes en sentido estricto.
    """
    try:
        max_evaluaciones = max(1, min(int(max_evaluaciones), 100))
        spacing_minutes = max(15, min(int(spacing_minutes), 24 * 60))
        spacing = timedelta(minutes=spacing_minutes)
        prehistory = timedelta(hours=3)
        future_horizon = timedelta(hours=7)

        # 30k muestras cubren de sobra ~48H con el colector P2P de 10s y
        # dejan margen para que el backtest pueda seleccionar orígenes separados.
        filas = obtener_estadisticas_db(limit=30000, banco=banco_filtro)
        series = []
        for c, v, _, fecha in filas:
            try:
                if not fecha or float(c) <= 0 or float(v) <= 0:
                    continue
                dt = fecha.astimezone(VET) if getattr(fecha, "tzinfo", None) else VET.localize(fecha)
                series.append((dt, (float(c) + float(v)) / 2.0))
            except Exception:
                continue

        series.sort(key=lambda x: x[0])
        if len(series) < 80:
            return {
                "status": "insufficient_history",
                "evaluations": 0,
                "spacing_minutes": spacing_minutes,
                "message": "Se necesitan más muestras históricas para medir el motor sin sesgo.",
            }

        # Solo son elegibles los orígenes que tienen al menos 3H de historia
        # y una observación real 7H después.
        eligible = []
        first_allowed = series[0][0] + prehistory
        last_allowed = series[-1][0] - future_horizon
        for i, (origin_t, _) in enumerate(series):
            if origin_t < first_allowed or origin_t > last_allowed:
                continue
            eligible.append(i)

        if not eligible:
            return {
                "status": "insufficient_history",
                "evaluations": 0,
                "spacing_minutes": spacing_minutes,
                "message": "No hubo ventanas completas de 7H con 3H de historia previa.",
            }

        # Seleccionar desde el final hacia atrás. Así las evaluaciones son
        # recientes y están separadas temporalmente en lugar de ser puntos
        # consecutivos del mismo movimiento.
        selected_indices = []
        cursor = eligible[-1]
        while cursor is not None and len(selected_indices) < max_evaluaciones:
            selected_indices.append(cursor)
            cursor = next(
                (
                    j for j in reversed(eligible)
                    if series[j][0] <= series[cursor][0] - spacing
                ),
                None,
            )
        selected_indices.reverse()

        rows = []
        for i in selected_indices:
            origin_t, origin_mid = series[i]
            target_t = origin_t + future_horizon

            # Primera muestra disponible en o después del horizonte de 7H.
            future = next(
                (
                    (j, x) for j, x in enumerate(series[i + 1:], start=i + 1)
                    if x[0] >= target_t
                ),
                None,
            )
            if future is None:
                continue

            # La historia termina exactamente en el origen: no hay leakage.
            hist = [
                x for x in series[: i + 1]
                if x[0] >= origin_t - prehistory
            ]
            if len(hist) < 12:
                continue

            def point(hours):
                target = origin_t - timedelta(hours=hours)
                candidates = [
                    x for x in hist
                    if abs((x[0] - target).total_seconds())
                    <= max(900, hours * 3600 * 0.35)
                ]
                if not candidates:
                    return None
                return min(
                    candidates,
                    key=lambda x: abs((x[0] - target).total_seconds())
                )[1]

            p1, p3 = point(1.0), point(3.0)
            if p1 is None or p3 is None:
                continue

            c1 = (origin_mid - p1) / p1 * 100.0
            c3 = (origin_mid - p3) / p3 * 100.0
            signal = c1 * 0.45 + c3 * 0.55
            pred_dir = 1 if signal > 0.01 else (-1 if signal < -0.01 else 0)

            actual_mid = future[1][1]
            actual_change = (actual_mid - origin_mid) / origin_mid * 100.0
            actual_dir = (
                1 if actual_mid > origin_mid * (1 + 0.0001)
                else (-1 if actual_mid < origin_mid * (1 - 0.0001) else 0)
            )

            # Error de magnitud de señal: compara el movimiento que el
            # momentum sugería con el movimiento observado a 7H. No se presenta
            # como un forecast de precio completo; es una métrica de calibración.
            signal_error = abs(actual_change - signal)

            rows.append({
                "origin": origin_t.isoformat(),
                "target": future[1][0].isoformat(),
                "pred_dir": pred_dir,
                "actual_dir": actual_dir,
                "signal_pct": round(signal, 4),
                "actual_change_pct": round(actual_change, 4),
                "signal_error_pct": round(signal_error, 4),
            })

        if not rows:
            return {
                "status": "insufficient_history",
                "evaluations": 0,
                "spacing_minutes": spacing_minutes,
                "message": "No hubo ventanas completas de 7H con datos suficientes.",
            }

        decisive = [
            r for r in rows
            if r["pred_dir"] != 0 and r["actual_dir"] != 0
        ]
        hits = sum(
            1 for r in decisive
            if r["pred_dir"] == r["actual_dir"]
        )
        mean_abs_move = (
            float(np.mean([abs(r["actual_change_pct"]) for r in decisive]))
            if decisive else 0.0
        )
        mean_abs_signal_error = (
            float(np.mean([r["signal_error_pct"] for r in decisive]))
            if decisive else 0.0
        )

        coverage_hours = (
            (series[-1][0] - series[0][0]).total_seconds() / 3600.0
        )

        return {
            "status": "ok",
            "evaluation_mode": "temporally_spaced",
            "spacing_minutes": spacing_minutes,
            "future_horizon_hours": 7,
            "history_coverage_hours": round(coverage_hours, 2),
            "evaluations": len(rows),
            "decisive": len(decisive),
            "direction_accuracy_pct": round(
                hits / len(decisive) * 100, 2
            ) if decisive else None,
            "mean_abs_move_pct": round(mean_abs_move, 4),
            "mean_abs_signal_error_pct": round(mean_abs_signal_error, 4),
            "future_windows_overlap": spacing_minutes < 420,
            "recent": rows[-10:],
        }
    except Exception as e:
        logger.warning("Backtest Quant 7H falló: %s", e)
        return {
            "status": "error",
            "evaluations": 0,
            "message": "Backtest temporalmente no disponible",
        }

def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL", _from_v2=False):
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

    # Primera calibración de 24H: añade una referencia de tendencia de mayor
    # horizonte sin sustituir el motor de 7H. El peso es pequeño y solo se
    # activa cuando existe cobertura suficiente, para no sobreajustar un día
    # de datos. A las 48H esta referencia podrá validarse con el backtest 7H.
    calibracion_24h = {
        "activa": False, "cobertura_horas": 0.0, "drift_24h_pct_h": 0.0,
        "r2_24h": 0.0, "peso_aplicado": 0.0, "motivo": "sin_cobertura_suficiente",
    }
    drift_24h = 0.0
    regression_r2_24h = 0.0
    cobertura_24h = 0.0
    if QUANT_24H_CALIBRATION_ENABLED and len(series) >= 12:
        cutoff_24h = now - timedelta(hours=24)
        idx24 = [i for i, dt in enumerate(fechas) if dt >= cutoff_24h]
        if len(idx24) >= 12:
            r24_dates = [fechas[i] for i in idx24]
            x24 = np.asarray([(dt - r24_dates[0]).total_seconds()/3600.0 for dt in r24_dates], dtype=float)
            y24 = np.asarray([mids[i] for i in idx24], dtype=float)
            cobertura_24h = max(0.0, (r24_dates[-1] - r24_dates[0]).total_seconds()/3600.0)
            if cobertura_24h >= 12.0 and len(np.unique(x24)) >= 4 and float(np.ptp(x24)) >= 1.0:
                try:
                    slope24, _ = np.polyfit(x24, y24, 1)
                    yhat24 = slope24*x24 + _
                    ss_res24 = float(np.sum((y24-yhat24)**2))
                    ss_tot24 = float(np.sum((y24-np.mean(y24))**2))
                    regression_r2_24h = max(0.0, min(1.0, 1.0 - ss_res24/ss_tot24)) if ss_tot24 > 0 else 0.0
                    drift_24h = float(slope24 / mid_actual * 100.0) if mid_actual else 0.0
                    peso24 = min(QUANT_24H_TREND_WEIGHT_MAX, max(0.0, (cobertura_24h-12.0)/12.0) * QUANT_24H_TREND_WEIGHT_MAX)
                    # Un R² bajo no debe arrastrar la señal de 7H.
                    peso24 *= min(1.0, regression_r2_24h / 0.35)
                    calibracion_24h = {
                        "activa": peso24 > 0.0, "cobertura_horas": round(cobertura_24h, 2),
                        "drift_24h_pct_h": round(drift_24h, 5), "r2_24h": round(regression_r2_24h, 3),
                        "peso_aplicado": round(peso24, 4),
                        "motivo": "tendencia_24h_estable" if peso24 > 0 else "r2_24h_insuficiente",
                    }
                except Exception:
                    pass

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
    # Ajuste de 24H: solo una corrección pequeña sobre el drift de 7H.
    peso24 = float(calibracion_24h.get("peso_aplicado", 0.0) or 0.0)
    if peso24 > 0.0:
        drift_h = (1.0 - peso24) * drift_h + peso24 * drift_24h

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
    # La calibración 24H suma estabilidad solo si realmente aporta información;
    # si contradice fuertemente la señal corta, reducimos confianza en lugar de
    # forzar una dirección.
    acuerdo_24h = 1.0
    if peso24 > 0.0 and drift_h != 0 and drift_24h != 0 and (drift_h > 0) != (drift_24h > 0):
        acuerdo_24h = 0.45
    confianza = int(round(100 * (0.30*coverage_score + 0.22*density_score + 0.23*agreement + 0.17*regression_r2 + 0.08*acuerdo_24h)))
    confianza = max(15, min(92, confianza)) if len(series) >= 3 else 0

    manipulacion = detectar_manipulacion_mercado(mids, spreads, mid_actual, spread_actual)
    regimen = clasificar_regimen_quant(returns, drift_h, rango_pct, regression_r2)
    niveles_dinamicos = niveles_dinamicos_quant(recent, mid_actual, volatilidad)

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
        "regimen": regimen, "niveles_dinamicos": niveles_dinamicos,
        "calibracion_24h": calibracion_24h,
        "manipulacion": manipulacion,
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
        [InlineKeyboardButton("🛠 Estado del sistema", callback_data="cmd_estado"), InlineKeyboardButton("📊 Rendimiento", callback_data="cmd_rendimiento")],
        [InlineKeyboardButton("💎 Muestra los planes VIP y PREMIUM", callback_data="cmd_suscribir")],
        [InlineKeyboardButton("👤 Mi cuenta", callback_data="cmd_cuenta"), InlineKeyboardButton("🔐 Mis credenciales", callback_data="cmd_credenciales")],
        [InlineKeyboardButton("📊 Gráfica de Protección Temporal", callback_data="cmd_grafica")],
        [InlineKeyboardButton("🏦 Configurar Filtro de Bancos", callback_data="cmd_bancos")],
    ])


async def cmd_cuenta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        account, _ = await asyncio.to_thread(_create_or_get_telegram_account, chat_id, DEFAULT_COUNTRY_CODE)
        exp = account.get("plan_expires_at")
        exp_text = exp.astimezone(VET).strftime("%d/%m/%Y") if exp else "No definido"
        texto = f"👤 *Mi cuenta Venbot*\n\n🔐 Usuario: `{account.get('username')}`\n💎 Plan: *{_plan_vigente(account.get('plan_code'), account.get('plan_expires_at'))}*\n📅 Vencimiento: `{exp_text}`\n\nUsa /credenciales para generar tus credenciales de acceso a la interfaz."
        if update.callback_query and update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Generar credenciales", callback_data="cmd_credenciales")],[InlineKeyboardButton("⬅️ Volver al menú", callback_data="cmd_menu")]]))
        else:
            await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Generar credenciales", callback_data="cmd_credenciales")]]))
    except Exception:
        logger.exception("Error en /cuenta")
        await update.message.reply_text("⚠️ No pude consultar tu cuenta ahora. Intenta nuevamente.")

async def cmd_credenciales(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        account, _ = await asyncio.to_thread(_create_or_get_telegram_account, chat_id, DEFAULT_COUNTRY_CODE)
        credentials = await asyncio.to_thread(_set_new_password, account["external_user_id"])
        texto = f"🔐 *Credenciales Venbot*\n\nUsuario: `{credentials['username']}`\nContraseña: `{credentials['password']}`\n\n⚠️ Guarda estas credenciales. La contraseña se entrega por Telegram y se almacena en Venbot únicamente como hash.\n\nEn la interfaz pulsa *Entrar* para iniciar sesión."
        teclado = InlineKeyboardMarkup([[InlineKeyboardButton("👤 Mi cuenta", callback_data="cmd_cuenta")],[InlineKeyboardButton("⬅️ Volver al menú", callback_data="cmd_menu")]])
        if update.callback_query and update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=teclado)
        else:
            await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=teclado)
    except Exception:
        logger.exception("Error en /credenciales")
        await update.message.reply_text("⚠️ No pude generar tus credenciales ahora. Intenta nuevamente.")


async def cmd_miid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(f"🆔 Tu ID de chat de Telegram es: {update.effective_chat.id}\nÚsalo en Venbot para recibir tus alertas personales.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = "🦜 *VENBOT PREDICCIONES - SISTEMA DE PROTECCIÓN*\nSelecciona una opción del menú táctico:"
    if update.callback_query:
        await update.callback_query.answer()
        if update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())


async def cmd_rendimiento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    try:
        banco=CONFIGURACION_BANCOS.get(update.effective_chat.id,"GENERAL")
        perf=await asyncio.to_thread(obtener_prediction_performance,banco,100)
        h=perf.get("horizons",{})
        acc=perf.get("direction_accuracy_7h_pct")
        reg=(perf.get("by_regime",{}).get("regimen",{}) or {})
        texto=(f"📊 *VENBOT · RENDIMIENTO*\n"
               f"🏦 Banco: `{banco}`\n\n"
               f"Predicciones registradas: `{perf.get('tracked',0)}`\n"
               f"🎯 Dirección correcta 7H: `{acc if acc is not None else 'n/d'}%`\n\n"
               f"*Error del escenario central 7H medido en cada corte*\n"
               f"• 1H: `{h.get('1h',{}).get('mean_abs_error_pct') if h.get('1h',{}).get('mean_abs_error_pct') is not None else 'n/d'}%`\n"
               f"• 3H: `{h.get('3h',{}).get('mean_abs_error_pct') if h.get('3h',{}).get('mean_abs_error_pct') is not None else 'n/d'}%`\n"
               f"• 7H: `{h.get('7h',{}).get('mean_abs_error_pct') if h.get('7h',{}).get('mean_abs_error_pct') is not None else 'n/d'}%`\n"
               f"• 24H: `{h.get('24h',{}).get('mean_abs_error_pct') if h.get('24h',{}).get('mean_abs_error_pct') is not None else 'n/d'}%`\n\n"
               f"*Precisión por régimen*\n"
               + ("\n".join(f"• {k}: `{v}%`" for k,v in reg.items()) if reg else "• Aún no hay suficientes evaluaciones")
               + "\n\n⚠️ Métricas descriptivas del historial; no representan garantía de resultados futuros.")
        keyboard=InlineKeyboardMarkup([[InlineKeyboardButton("🔮 Predicción",callback_data="cmd_prediccion")],[InlineKeyboardButton("🛠 Estado",callback_data="cmd_estado")]])
        if update.callback_query and update.callback_query.message: await update.callback_query.message.edit_text(texto,parse_mode="Markdown",reply_markup=keyboard)
        else: await update.message.reply_text(texto,parse_mode="Markdown",reply_markup=keyboard)
    except Exception:
        logger.exception("Error en /rendimiento")
        await update.effective_message.reply_text("⚠️ No pude consultar el rendimiento ahora.")


async def cmd_estado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    try:
        banco = CONFIGURACION_BANCOS.get(update.effective_chat.id, "GENERAL")
        mercado = await asyncio.to_thread(obtener_ultimo_mercado_banco, banco)
        c=float(mercado.get("compra",0) or 0); v=float(mercado.get("venta",0) or 0)
        fecha=mercado.get("fecha")
        if fecha and getattr(fecha,"tzinfo",None): age=max(0,(datetime.now(VET)-fecha.astimezone(VET)).total_seconds())
        else: age=None
        q=await asyncio.to_thread(motor_quant_inteligente,c,v,int(mercado.get("liquidez",0) or 0),banco) if c>0 and v>0 else {}
        perf=await asyncio.to_thread(obtener_prediction_performance,banco,100)
        cov=float(q.get("cobertura_horas",0) or 0)
        cal=q.get("calibracion_24h",{}) or {}
        texto=(f"🦜 *VENBOT · ESTADO DEL SISTEMA*\n\n"
               f"🏦 Banco: `{banco}`\n"
               f"🟢 P2P Binance: OK\n🟢 Quant Engine: OK\n🟢 Alertas: OK\n🟢 Billing manual: OK\n"
               f"🟢 Prediction Tracking: {'OK' if PREDICTION_TRACKING_ENABLED else 'OFF'}\n\n"
               f"📊 Muestras actuales: `{q.get('muestras',0)}`\n"
               f"⏱ Cobertura: `{cov:.1f} h`\n"
               f"🧠 Calibración 24H: `{'ACTIVA' if cal.get('activa') else 'RECOLECTANDO'}`\n"
               f"📈 Predicciones registradas: `{perf.get('tracked',0)}`\n"
               f"🎯 Precisión 7H: `{perf.get('direction_accuracy_7h_pct') if perf.get('direction_accuracy_7h_pct') is not None else 'n/d'}%`\n"
               f"💵 Comprar/Vender: `{c:.2f} / {v:.2f}`\n"
               f"🔄 Última lectura: `{age:.0f}s`" if age is not None else f"🔄 Última lectura: `n/d`")
        keyboard=InlineKeyboardMarkup([[InlineKeyboardButton("🔮 Predicción",callback_data="cmd_prediccion")],[InlineKeyboardButton("⬅️ Volver al menú",callback_data="cmd_menu")]])
        if update.callback_query and update.callback_query.message: await update.callback_query.message.edit_text(texto,parse_mode="Markdown",reply_markup=keyboard)
        else: await update.message.reply_text(texto,parse_mode="Markdown",reply_markup=keyboard)
    except Exception:
        logger.exception("Error en /estado")
        await update.effective_message.reply_text("⚠️ No pude consultar el estado ahora.")


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
    senal_operativa = evaluar_senal_operativa(datos, c_real, v_real)

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
        f"• 🎯 Señal operativa: `{senal_operativa['label']}`\n"
        f"• Lectura operativa: {senal_operativa['reason']}\n"
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


async def _plan_catalogo_para_telegram(chat_id: int):
    account,_=await asyncio.to_thread(_create_or_get_telegram_account,chat_id,DEFAULT_COUNTRY_CODE); return account

async def _crear_checkout_telegram(chat_id:int,plan_code:str,pay_currency:str):
    account=await _plan_catalogo_para_telegram(chat_id); return await asyncio.to_thread(_create_billing_order,account["external_user_id"],chat_id,plan_code,pay_currency)

async def _enviar_checkout_telegram(update:Update,context:ContextTypes.DEFAULT_TYPE,plan_code:str,pay_currency:str):
    chat_id=update.effective_chat.id
    if update.callback_query: await update.callback_query.answer("Creando orden…")
    try:
        account=await _plan_catalogo_para_telegram(chat_id)
        if BILLING_PROVIDER == "manual":
            order=await asyncio.to_thread(_create_manual_billing_order,account["external_user_id"],chat_id,plan_code,pay_currency)
            currency=pay_currency.upper()
            if currency=="USDT":
                instructions=(f"💰 *Pago USDT*\n• Monto: `{order['quoted_amount']:.2f} USDT`\n• Pay ID/correo: `{MANUAL_USDT_PAY_ID or 'Configurar en Render'}`\n• Método: *Binance Pay*")
            else:
                instructions=(f"🇻🇪 *Pago en Bolívares*\n• Monto exacto: `{order['quoted_amount']:.2f} Bs`\n• Banco: `{MANUAL_BS_BANK or 'Configurar en Render'}`\n• Cuenta: `{MANUAL_BS_ACCOUNT or 'Configurar en Render'}`\n• Titular: `{MANUAL_BS_HOLDER or 'Configurar en Render'}`\n• Teléfono: `{MANUAL_BS_PHONE or 'Configurar en Render'}`\n• C.I./RIF: `{MANUAL_BS_ID or 'Configurar en Render'}`")
            texto=(f"🧾 *ORDEN VENBOT*\n\n💎 Plan: *{plan_code}*\n🔖 Orden: `{order['order_id']}`\n\n{instructions}\n\n📸 Después de pagar, pulsa *Enviar comprobante*. Necesito la captura y la referencia para validar manualmente.\n\n⏳ Válida hasta: `{order['expires_at']}`\n\n⚠️ El plan NO se activa hasta que el pago sea revisado y aprobado.")
            markup=InlineKeyboardMarkup([[InlineKeyboardButton("📸 Enviar comprobante",callback_data=f"proof_{order['order_id']}")],[InlineKeyboardButton("💎 Volver a planes",callback_data="cmd_suscribir")]])
        else:
            order=await asyncio.to_thread(_create_billing_order,account["external_user_id"],chat_id,plan_code,pay_currency); mode="USDT" if pay_currency=="USDT" else "Bolívares (Bs)"; amount=f"{order['quoted_amount']:.2f} USDT" if pay_currency=="USDT" else "equivalente en Bs dentro del checkout"
            texto=(f"🧾 *Orden Venbot {plan_code}*\n\n💰 Modalidad: *{mode}*\n• Precio base: `{order['base_amount_usdt']:.2f} USDT`\n• Cobro: *{amount}*\n• Orden: `{order['order_id']}`\n\nAbre el checkout y completa el pago. Venbot activa el plan únicamente cuando el proveedor confirme el pago automáticamente.")
            markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Abrir checkout",url=order["checkout_url"])],[InlineKeyboardButton("💎 Volver a planes",callback_data="cmd_suscribir")]])
    except HTTPException as e:
        texto=f"⚠️ {e.detail}"; markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver a planes",callback_data="cmd_suscribir")]])
    except Exception:
        logger.exception("Error creando orden Telegram"); texto="⚠️ No pude crear la orden ahora. Intenta nuevamente en unos minutos."; markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver a planes",callback_data="cmd_suscribir")]])
    if update.callback_query and update.callback_query.message: await update.callback_query.message.edit_text(texto,parse_mode="Markdown",reply_markup=markup)
    else: await context.bot.send_message(chat_id=chat_id,text=texto,parse_mode="Markdown",reply_markup=markup)

async def recibir_comprobante(update:Update,context:ContextTypes.DEFAULT_TYPE):
    chat_id=update.effective_chat.id
    order_id=context.user_data.get("manual_proof_order_id")
    if not order_id: return
    account=await _plan_catalogo_para_telegram(chat_id)
    external_id=account["external_user_id"]
    if update.message and update.message.photo:
        file_id=update.message.photo[-1].file_id
        context.user_data["manual_proof_file_id"]=file_id
        await update.message.reply_text(f"📸 Captura recibida para `{order_id}`. Ahora envíame *solo la referencia del pago* (texto).",parse_mode="Markdown")
        return
    if update.message and update.message.text:
        reference=update.message.text.strip()
        if len(reference)<3:
            await update.message.reply_text("⚠️ La referencia es demasiado corta. Envíame el código de referencia del pago."); return
        file_id=context.user_data.get("manual_proof_file_id")
        if not file_id:
            await update.message.reply_text("Primero envíame la *captura del comprobante* y luego la referencia.",parse_mode="Markdown"); return
        ok=await asyncio.to_thread(_save_manual_proof,order_id,external_id,file_id,reference)
        context.user_data.pop("manual_proof_order_id",None); context.user_data.pop("manual_proof_file_id",None)
        if ok:
            await update.message.reply_text(f"✅ Comprobante recibido.\n\n🔖 Orden: `{order_id}`\n🧾 Referencia: `{reference}`\n\nTu pago quedó *pendiente de validación manual*. Te avisaré por Telegram cuando sea aprobado.",parse_mode="Markdown")
            if telegram_app and BILLING_ADMIN_TELEGRAM_CHAT_ID:
                try:
                    await telegram_app.bot.send_message(chat_id=int(BILLING_ADMIN_TELEGRAM_CHAT_ID),text=f"🔔 *NUEVO PAGO PENDIENTE*\n\nOrden: `{order_id}`\nUsuario: `{external_id}`\nReferencia: `{reference}`\n\nUsa `/pagos` para revisar y `/aprobar {order_id}` para activar.",parse_mode="Markdown")
                    await telegram_app.bot.send_photo(chat_id=int(BILLING_ADMIN_TELEGRAM_CHAT_ID),photo=file_id,caption=f"Comprobante {order_id}")
                except Exception: logger.exception("No se pudo notificar comprobante al admin")
        else:
            await update.message.reply_text("⚠️ La orden ya no está disponible para recibir comprobante.")

async def cmd_pagos(update:Update,context:ContextTypes.DEFAULT_TYPE):
    chat_id=update.effective_chat.id
    if not _is_billing_admin(chat_id): return
    rows=await asyncio.to_thread(_pending_manual_orders,30)
    if not rows:
        await update.message.reply_text("✅ No hay pagos manuales pendientes."); return
    lines=["💳 *PAGOS PENDIENTES*\n"]
    for r in rows:
        oid,uid,tg,plan,currency,amount,status,ref,submitted,created=r
        lines.append(f"• `{oid}` · {plan} · {amount:.2f} {currency}\n  Usuario: `{uid}`\n  Ref: `{ref or 'sin referencia'}`\n  Enviado: `{submitted.isoformat() if submitted else 'sin comprobante'}`")
    lines.append("\nPara aprobar: `/aprobar ID_ORDEN`\nPara rechazar: `/rechazar ID_ORDEN motivo`")
    await update.message.reply_text("\n".join(lines),parse_mode="Markdown")

async def cmd_aprobar(update:Update,context:ContextTypes.DEFAULT_TYPE):
    chat_id=update.effective_chat.id
    if not _is_billing_admin(chat_id): return
    if not context.args: await update.message.reply_text("Uso: /aprobar VB-..."); return
    try:
        result=await asyncio.to_thread(_approve_manual_order,context.args[0].strip(),chat_id)
        await update.message.reply_text(f"✅ Orden `{result['order_id']}` aprobada. Plan *{result['plan']}* activo hasta `{result['plan_expires_at']}`.",parse_mode="Markdown")
        tg=result.get("telegram_chat_id")
        if tg and telegram_app:
            await telegram_app.bot.send_message(chat_id=int(tg),text=f"🎉 *Pago aprobado*\n\n💎 Plan: *{result['plan']}*\n📅 Válido hasta: `{result['plan_expires_at']}`\n\nTu cuenta ya está activa. Usa /cuenta para consultar tu plan.",parse_mode="Markdown")
    except HTTPException as e: await update.message.reply_text(f"⚠️ {e.detail}")
    except Exception: logger.exception("Error aprobando orden"); await update.message.reply_text("⚠️ Error aprobando la orden.")

async def cmd_rechazar(update:Update,context:ContextTypes.DEFAULT_TYPE):
    chat_id=update.effective_chat.id
    if not _is_billing_admin(chat_id): return
    if not context.args: await update.message.reply_text("Uso: /rechazar VB-... motivo"); return
    reason=" ".join(context.args[1:]).strip() or "Pago no validado"
    try:
        result=await asyncio.to_thread(_reject_manual_order,context.args[0].strip(),chat_id,reason)
        await update.message.reply_text(f"↩️ Orden `{result['order_id']}` rechazada.",parse_mode="Markdown")
        tg=result.get("telegram_chat_id")
        if tg and telegram_app:
            await telegram_app.bot.send_message(chat_id=int(tg),text=f"⚠️ *Pago no aprobado*\n\nOrden: `{result['order_id']}`\nMotivo: {reason}\n\nPuedes volver a /planes y generar una nueva orden si deseas intentarlo nuevamente.",parse_mode="Markdown")
    except HTTPException as e: await update.message.reply_text(f"⚠️ {e.detail}")
    except Exception: logger.exception("Error rechazando orden"); await update.message.reply_text("⚠️ Error rechazando la orden.")

async def cmd_suscribir(update:Update,context:ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    chat_id=update.effective_chat.id; account=await _plan_catalogo_para_telegram(chat_id); country=DEFAULT_COUNTRY_CODE; policy=_billing_policy(country)
    premium_price=f"{PREMIUM_PRICE_USDT:.2f} USDT" if PREMIUM_PRICE_USDT>0 else "Precio no configurado"; vip_price=f"{VIP_PRICE_USDT:.2f} USDT" if VIP_PRICE_USDT>0 else "Precio no configurado"
    texto=("💎 *PLANES VENBOT*\n\n🆓 *FREE*\n• Monitor P2P y BCV\n• Calculadora y análisis básico\n• 5 consultas IA/día\n• 2 alertas\n\n"+f"⭐ *PREMIUM* — `{premium_price}`\n• Todo FREE\n• IA avanzada\n• 30 consultas IA/día\n• Hasta 10 alertas\n• Historial 30 días\n\n"+f"👑 *VIP* — `{vip_price}`\n• Todo PREMIUM\n• Spot y funciones Quant\n• Predicción avanzada\n• 100 consultas IA/día\n• Hasta 50 alertas\n• Historial 365 días\n\n"+f"🔐 Cuenta: `{account.get('username')}`\n📍 Mercado de cuenta: `{country}`\n\n")
    if BILLING_PROVIDER == "manual":
        ready = bool(PREMIUM_PRICE_USDT>0 and VIP_PRICE_USDT>0 and PREMIUM_PRICE_VES>0 and VIP_PRICE_VES>0)
        if ready:
            texto+="Puedes pagar en *USDT* o en *Bolívares (Bs)*. El pago se valida manualmente: debes enviar captura + referencia. El plan se activa solo después de mi aprobación."
            botones=[[InlineKeyboardButton("⭐ PREMIUM · ₮ USDT",callback_data="buy_PREMIUM_USDT"),InlineKeyboardButton("🇻🇪 PREMIUM · Bs",callback_data="buy_PREMIUM_VES")],[InlineKeyboardButton("👑 VIP · ₮ USDT",callback_data="buy_VIP_USDT"),InlineKeyboardButton("🇻🇪 VIP · Bs",callback_data="buy_VIP_VES")],[InlineKeyboardButton("👤 Mi cuenta",callback_data="cmd_cuenta")],[InlineKeyboardButton("⬅️ Volver al menú",callback_data="cmd_menu")]]
        else:
            texto+="El pago manual todavía no está configurado. Faltan los precios PREMIUM/VIP en USDT y Bs en Render."
            botones=[[InlineKeyboardButton("👤 Mi cuenta",callback_data="cmd_cuenta")],[InlineKeyboardButton("⬅️ Volver al menú",callback_data="cmd_menu")]]
    else:
        ready=policy.get("external_checkout") and BILLING_PROVIDER=="pabilo" and bool(PABILO_API_KEY and PABILO_USER_BANK_ID and PABILO_WEBHOOK_SECRET and RENDER_EXTERNAL_URL and PREMIUM_PRICE_USDT>0 and VIP_PRICE_USDT>0)
        if ready:
            texto+="Puedes pagar en *USDT* o en *Bolívares (Bs)*. La activación es automática tras confirmación del proveedor."
            botones=[[InlineKeyboardButton("⭐ PREMIUM · ₮ USDT",callback_data="buy_PREMIUM_USDT"),InlineKeyboardButton("🇻🇪 PREMIUM · Bs",callback_data="buy_PREMIUM_VES")],[InlineKeyboardButton("👑 VIP · ₮ USDT",callback_data="buy_VIP_USDT"),InlineKeyboardButton("🇻🇪 VIP · Bs",callback_data="buy_VIP_VES")],[InlineKeyboardButton("👤 Mi cuenta",callback_data="cmd_cuenta")],[InlineKeyboardButton("⬅️ Volver al menú",callback_data="cmd_menu")]]
        else:
            texto+="El checkout todavía no está listo en producción."
            botones=[[InlineKeyboardButton("👤 Mi cuenta",callback_data="cmd_cuenta")],[InlineKeyboardButton("⬅️ Volver al menú",callback_data="cmd_menu")]]
    markup=InlineKeyboardMarkup(botones)
    if update.callback_query and update.callback_query.message: await update.callback_query.message.edit_text(texto,parse_mode="Markdown",reply_markup=markup)
    else: await context.bot.send_message(chat_id=chat_id,text=texto,parse_mode="Markdown",reply_markup=markup)


async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data
    chat_id = update.effective_chat.id
    if data == "cmd_estado":
        await cmd_estado(update, context)
    elif data == "cmd_rendimiento":
        await cmd_rendimiento(update, context)
    elif data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_cuenta":
        await cmd_cuenta(update, context)
    elif data == "cmd_credenciales":
        await cmd_credenciales(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_bancos":
        await cmd_bancos(update, context)
    elif data == "cmd_suscribir":
        await cmd_suscribir(update, context)
    elif data.startswith("buy_"):
        try:
            _, plan, currency = data.split("_", 2)
            await _enviar_checkout_telegram(update, context, plan, currency)
        except Exception:
            logger.exception("Error procesando compra Telegram: %s", data)
            await query.answer("No se pudo crear la orden", show_alert=True)
    elif data.startswith("proof_"):
        order_id=data.replace("proof_", "", 1)
        context.user_data["manual_proof_order_id"]=order_id
        context.user_data.pop("manual_proof_file_id",None)
        await query.answer()
        await query.message.reply_text(f"📸 *Comprobante para {order_id}*\n\n1. Envíame la captura del pago.\n2. Después envíame el código de referencia en otro mensaje.\n\nNo envíes datos bancarios adicionales ni contraseñas.",parse_mode="Markdown")
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

            global _LAST_SPOT_COLLECTION_TS
            if time.monotonic() - _LAST_SPOT_COLLECTION_TS >= SPOT_REFRESH_SECONDS:
                try:
                    spot_now = await asyncio.to_thread(recolectar_spot, SPOT_SYMBOLS, True)
                    if spot_now:
                        logger.info("Spot listo: %s", ", ".join(f"{k}={v['price']:.4f}" for k,v in spot_now.items()))
                    _LAST_SPOT_COLLECTION_TS = time.monotonic()
                except Exception as e:
                    logger.warning("Ciclo Spot falló sin afectar P2P: %s", e)

            if mercado and mercado["compra"] > 0 and mercado["venta"] > 0:
                datos = await asyncio.to_thread(motor_quant_inteligente, mercado["compra"], mercado["venta"], mercado["liquidez"], "GENERAL")
                global _LAST_PREDICTION_TRACKING_TS
                if PREDICTION_TRACKING_ENABLED:
                    try:
                        await asyncio.to_thread(evaluar_predicciones_pendientes, 120)
                        if time.monotonic() - _LAST_PREDICTION_TRACKING_TS >= PREDICTION_TRACKING_INTERVAL_SECONDS:
                            for _banco, (_c, _v, _l) in resultados.items():
                                if _c > 0 and _v > 0:
                                    _qtrack = await asyncio.to_thread(motor_quant_inteligente, _c, _v, _l, _banco)
                                    await asyncio.to_thread(registrar_prediccion_tracking, _banco, _c, _v, _qtrack)
                            _LAST_PREDICTION_TRACKING_TS = time.monotonic()
                    except Exception as e:
                        logger.warning("Prediction tracking falló sin afectar P2P: %s", e)
                tendencia = datos["tendencia"]
                manip = datos.get("manipulacion") or {}
                nuevos_eventos = await asyncio.to_thread(evaluar_alertas_inteligentes, mercado, datos, "GENERAL")
                if nuevos_eventos and TELEGRAM_ALERT_CHAT_ID and telegram_app:
                    for severity, titulo, msg in nuevos_eventos:
                        try:
                            await telegram_app.bot.send_message(
                                chat_id=TELEGRAM_ALERT_CHAT_ID,
                                text=f"{titulo}\n• {msg}\n• Señal estadística; no garantiza un resultado futuro.",
                            )
                        except Exception as e:
                            logger.warning("No se pudo enviar alerta inteligente a Telegram: %s", e)

                # Alertas personales: cada regla usa el último dato persistido de su banco.
                try:
                    await _evaluar_alertas_personales()
                except Exception as e:
                    logger.warning("Evaluación de alertas personales falló sin afectar P2P: %s", e)

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
# FUNDACIÓN DE PRODUCTO / PLANES / PERMISOS
# ==========================================
PLAN_ORDER = {"FREE": 0, "PREMIUM": 1, "VIP": 2}
PLAN_LIMITS = {
    "FREE": {"ai_daily": 5, "alerts": 2, "history_days": 1},
    "PREMIUM": {"ai_daily": 30, "alerts": 10, "history_days": 30},
    "VIP": {"ai_daily": 100, "alerts": 50, "history_days": 365},
}
FEATURE_MIN_PLAN = {
    "monitor": "FREE", "p2p": "FREE", "bcv": "FREE", "calculator": "FREE",
    "basic_analysis": "FREE", "telegram": "PREMIUM", "advanced_analysis": "PREMIUM",
    "advanced_alerts": "PREMIUM", "prediction_bot": "VIP", "spot": "VIP",
}

# Política de cobro: la aplicación no procesa tarjetas ni pagos dentro del cliente.
# El backend solo prepara la integración; la habilitación final depende del país y
# de las reglas de distribución aplicables en la tienda.
BILLING_POLICY = {
    "VE": {"external_checkout": True, "provider": "external_web", "note": "Checkout externo sujeto a la normativa aplicable."},
    "US": {"external_checkout": True, "provider": "external_web", "note": "Usar solo bajo el programa/política aplicable de Google Play."},
    "DEFAULT": {"external_checkout": False, "provider": None, "note": "No habilitar checkout externo desde la app hasta verificar la política local."},
}

def _plan_efectivo(plan):
    plan = (plan or "FREE").upper()
    return plan if plan in PLAN_ORDER else "FREE"

def _plan_vigente(plan, expires_at=None):
    plan = _plan_efectivo(plan)
    if plan == "FREE" or not expires_at:
        return plan
    try:
        exp = expires_at
        if isinstance(exp, str):
            exp = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = VET.localize(exp)
        if exp <= datetime.now(VET):
            return "FREE"
    except Exception:
        return plan
    return plan

def _billing_policy(country):
    return BILLING_POLICY.get((country or DEFAULT_COUNTRY_CODE).upper(), BILLING_POLICY["DEFAULT"])

def _foundation_user(external_user_id, country_code=None):
    if not DATABASE_URL:
        return {"external_user_id": external_user_id, "country_code": country_code or DEFAULT_COUNTRY_CODE, "plan_code": "FREE", "status": "active"}
    country = (country_code or DEFAULT_COUNTRY_CODE).upper()[:8]
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""\n                INSERT INTO venbot_users(external_user_id, country_code) VALUES (%s,%s)\n                ON CONFLICT (external_user_id) DO UPDATE SET updated_at=CURRENT_TIMESTAMP, country_code=COALESCE(NULLIF(EXCLUDED.country_code,''), venbot_users.country_code)\n                RETURNING external_user_id,country_code,plan_code,status,created_at,updated_at,deleted_at\n            """, (external_user_id, country))
            row=cur.fetchone()
    return dict(zip(["external_user_id","country_code","plan_code","status","created_at","updated_at","deleted_at"], row))

AUTH_SESSION_DAYS = max(1, int(os.getenv("AUTH_SESSION_DAYS", "30")))
AUTH_LOGIN_MAX_ATTEMPTS = max(3, int(os.getenv("AUTH_LOGIN_MAX_ATTEMPTS", "8")))
AUTH_LOGIN_WINDOW_SECONDS = max(60, int(os.getenv("AUTH_LOGIN_WINDOW_SECONDS", "900")))
TELEGRAM_ACCOUNT_SETUP_SECRET = os.getenv("TELEGRAM_ACCOUNT_SETUP_SECRET", "").strip()
_AUTH_LOGIN_ATTEMPTS = {}

def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 210_000)
    return "pbkdf2_sha256$210000$%s$%s" % (base64.urlsafe_b64encode(salt).decode(), base64.urlsafe_b64encode(digest).decode())

def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, rounds, salt_b64, digest_b64 = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode())
        expected = base64.urlsafe_b64decode(digest_b64.encode())
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(rounds))
        return secrets.compare_digest(actual, expected)
    except Exception:
        return False

def _new_venbot_credentials() -> tuple[str, str]:
    return "VEN-" + secrets.token_hex(4).upper(), secrets.token_urlsafe(9)

def _create_or_get_telegram_account(chat_id: int, country_code: str = "VE"):
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT external_user_id,username,plan_code,status,plan_expires_at FROM venbot_users WHERE telegram_chat_id=%s LIMIT 1", (int(chat_id),))
            row = cur.fetchone()
            if row:
                return dict(zip(["external_user_id","username","plan_code","status","plan_expires_at"], row)), None
            external_id = str(uuid.uuid4())
            username = None
            for _ in range(8):
                candidate = "VEN-" + secrets.token_hex(4).upper()
                cur.execute("SELECT 1 FROM venbot_users WHERE username=%s", (candidate,))
                if not cur.fetchone():
                    username = candidate
                    break
            if not username:
                raise RuntimeError("No se pudo generar username Venbot")
            cur.execute("""INSERT INTO venbot_users(external_user_id,country_code,username,telegram_chat_id) VALUES (%s,%s,%s,%s) RETURNING external_user_id,username,plan_code,status,plan_expires_at""", (external_id, (country_code or DEFAULT_COUNTRY_CODE).upper()[:8], username, int(chat_id)))
            row = cur.fetchone()
    return dict(zip(["external_user_id","username","plan_code","status","plan_expires_at"], row)), None

def _set_new_password(external_user_id: str):
    password = secrets.token_urlsafe(10)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE venbot_users SET password_hash=%s,updated_at=CURRENT_TIMESTAMP WHERE external_user_id=%s RETURNING username,plan_code,status,plan_expires_at", (_hash_password(password), external_user_id))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="account_not_found")
    return {"username": row[0], "plan_code": row[1], "status": row[2], "plan_expires_at": row[3], "password": password}

def _account_from_session(token: str):
    if not token or not DATABASE_URL:
        return None
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT u.external_user_id,u.username,u.country_code,u.plan_code,u.status,u.plan_expires_at,s.expires_at FROM venbot_sessions s JOIN venbot_users u ON u.external_user_id=s.external_user_id WHERE s.token_hash=%s AND s.revoked_at IS NULL AND s.expires_at>CURRENT_TIMESTAMP AND u.status='active' LIMIT 1""", (token_hash,))
            row = cur.fetchone()
            if not row:
                return None
            cur.execute("UPDATE venbot_sessions SET last_seen_at=CURRENT_TIMESTAMP WHERE token_hash=%s", (token_hash,))
    return dict(zip(["external_user_id","username","country_code","plan_code","status","plan_expires_at","session_expires_at"], row))

def _create_session(external_user_id: str):
    token = secrets.token_urlsafe(48)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    expires = datetime.now(VET) + timedelta(days=AUTH_SESSION_DAYS)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO venbot_sessions(external_user_id,token_hash,expires_at) VALUES (%s,%s,%s)", (external_user_id, token_hash, expires))
            cur.execute("DELETE FROM venbot_sessions WHERE external_user_id=%s AND (revoked_at IS NOT NULL OR expires_at<=CURRENT_TIMESTAMP)", (external_user_id,))
    return token, expires

def _foundation_entitlements(user):
    plan = _plan_vigente(user.get("plan_code"), user.get("plan_expires_at"))
    limits = dict(PLAN_LIMITS[plan])
    features = {k: PLAN_ORDER[plan] >= PLAN_ORDER[v] for k,v in FEATURE_MIN_PLAN.items()}
    return {"plan": plan, "limits": limits, "features": features}

def _require_plan_user(request: Request, minimum_plan: str):
    user = _require_session_user(request)
    current = _plan_vigente(user.get("plan_code"), user.get("plan_expires_at"))
    if PLAN_ORDER[current] < PLAN_ORDER[minimum_plan]:
        raise HTTPException(status_code=403, detail={"error":"plan_required", "required_plan":minimum_plan, "current_plan":current})
    return user

def _consume_ai_quota(user):
    plan = _plan_vigente(user.get("plan_code"), user.get("plan_expires_at"))
    limit = int(PLAN_LIMITS[plan]["ai_daily"])
    if not DATABASE_URL:
        return {"allowed": True, "used": 0, "limit": limit, "remaining": limit}
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO venbot_usage_daily(external_user_id,usage_date,ai_requests)
                VALUES (%s,CURRENT_DATE,1)
                ON CONFLICT (external_user_id,usage_date) DO UPDATE
                SET ai_requests = venbot_usage_daily.ai_requests + 1
                WHERE venbot_usage_daily.ai_requests < %s
                RETURNING ai_requests
            """, (user["external_user_id"], limit))
            row=cur.fetchone()
    used=int(row[0]) if row else limit
    allowed=bool(row)
    return {"allowed":allowed,"used":used,"limit":limit,"remaining":max(0,limit-used)}

def _country_from_request(request):
    return (request.headers.get("X-Venbot-Country") or DEFAULT_COUNTRY_CODE).strip().upper()[:8]

class FoundationBootstrapRequest(BaseModel):
    external_user_id: str = Field(min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")
    country_code: str = Field(default="VE", min_length=2, max_length=8)

class LoginRequest(BaseModel):
    username: str = Field(min_length=6, max_length=40)
    password: str = Field(min_length=8, max_length=200)

class SessionRequest(BaseModel):
    session_token: str = Field(min_length=20, max_length=200)

class TelegramAccountRequest(BaseModel):
    telegram_chat_id: int
    country_code: str = Field(default="VE", min_length=2, max_length=8)

class ConsentRequest(BaseModel):
    external_user_id: str = Field(min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")
    consent_type: str = Field(min_length=2, max_length=50)
    version: str = Field(min_length=1, max_length=30)
    granted: bool

class AccountDeleteRequest(BaseModel):
    external_user_id: str = Field(min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")

class BillingWebhookRequest(BaseModel):
    external_user_id: Optional[str] = Field(default=None, min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")
    telegram_chat_id: Optional[int] = None
    username: Optional[str] = Field(default=None, min_length=6, max_length=40)
    plan_code: str = Field(min_length=4, max_length=12)
    event_type: str = Field(min_length=4, max_length=40)
    external_reference: str = Field(min_length=3, max_length=160)
    provider: str = Field(default="external_web", min_length=2, max_length=60)
    country_code: str = Field(default="VE", min_length=2, max_length=8)
    duration_days: Optional[int] = Field(default=None, ge=1, le=3660)
    payload: Optional[dict] = None

class BillingOrderCreateRequest(BaseModel):
    plan_code: str = Field(min_length=4, max_length=12)
    pay_currency: str = Field(min_length=3, max_length=8)

class AlertRuleCreateRequest(BaseModel):
    external_user_id: str = Field(min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")
    banco: str = Field(default="GENERAL", min_length=3, max_length=12)
    target_value: float = Field(gt=0)
    direction: str = Field(default="above", min_length=3, max_length=10)
    telegram_chat_id: Optional[int] = None
    cooldown_seconds: int = Field(default=1800, ge=300, le=86400)

class AlertRuleUpdateRequest(BaseModel):
    external_user_id: str = Field(min_length=16, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")
    banco: Optional[str] = Field(default=None, min_length=3, max_length=12)
    target_value: Optional[float] = Field(default=None, gt=0)
    direction: Optional[str] = Field(default=None, min_length=3, max_length=10)
    telegram_chat_id: Optional[int] = None
    cooldown_seconds: Optional[int] = Field(default=None, ge=300, le=86400)
    enabled: Optional[bool] = None

# ==========================================
# FASTAPI
# ==========================================
app = FastAPI(title="Venbot API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "status": "ok",
        "service": "Venbot",
        "api": "/api/precios",
        "history": "/api/history?period=1d",
        "spot": "/api/spot",
        "quant_v2": "/api/quant/v2",
        "quant_backtest": "/api/quant/backtest",
        "plans": "/api/plans",
        "billing_webhook": "/api/billing/webhook",
        "billing_orders": "/api/billing/orders",
        "billing_provider": BILLING_PROVIDER,
        "smart_alerts": "/api/alerts/smart",
        "system_status": "/api/estado",
        "prediction_signal": "/api/predictions/signal",
        "prediction_performance": "/api/predictions/performance",
        "prediction_recent": "/api/predictions/recent",
        "community": {"url": VENBOT_COMMUNITY_URL, "support_url": VENBOT_SUPPORT_URL, "bot_url": VENBOT_BOT_URL},
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
        "spot": {"enabled": True, "symbols": list(SPOT_SYMBOLS), "source": "Binance public market data"},
        "quant_engine": QUANT_ENGINE_V2.name,
        "smart_alerts": {"enabled": SMART_ALERTS_ENABLED, "cooldown_seconds": SMART_ALERT_COOLDOWN_SECONDS},
        "prediction_tracking": {"enabled": PREDICTION_TRACKING_ENABLED, "interval_seconds": PREDICTION_TRACKING_INTERVAL_SECONDS},
        "timestamp": datetime.now(VET).isoformat(),
    }

def _validar_alerta_banco(banco):
    banco = (banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL", "MERCANTIL", "PROVINCIAL", "BNC"}:
        raise HTTPException(status_code=400, detail="Banco no soportado")
    return banco

def _validar_direccion_alerta(direction):
    direction = (direction or "above").lower().strip()
    if direction not in {"above", "below"}:
        raise HTTPException(status_code=400, detail="direction debe ser above o below")
    return direction

def _alert_rules_for_user(external_user_id, include_disabled=True):
    if not DATABASE_URL:
        return []
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            sql = """SELECT id, external_user_id, banco, rule_type, target_value, direction, enabled, cooldown_seconds, telegram_chat_id, last_triggered_at, created_at, updated_at
                     FROM venbot_alert_rules WHERE external_user_id=%s"""
            if not include_disabled:
                sql += " AND enabled=TRUE"
            sql += " ORDER BY created_at DESC"
            cur.execute(sql, (external_user_id,))
            rows = cur.fetchall()
    keys = ["id","external_user_id","banco","rule_type","target_value","direction","enabled","cooldown_seconds","telegram_chat_id","last_triggered_at","created_at","updated_at"]
    return [dict(zip(keys, r)) for r in rows]

def _alert_rule_count(external_user_id):
    if not DATABASE_URL:
        return 0
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM venbot_alert_rules WHERE external_user_id=%s", (external_user_id,))
            return int(cur.fetchone()[0] or 0)

async def _evaluar_alertas_personales():
    if not DATABASE_URL or not telegram_app:
        return 0
    ahora = datetime.now(VET)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT id, external_user_id, banco, target_value, direction, cooldown_seconds, telegram_chat_id, last_triggered_at
                          FROM venbot_alert_rules WHERE enabled=TRUE AND telegram_chat_id IS NOT NULL
                          ORDER BY id ASC""")
            rules = cur.fetchall()
    disparadas = 0
    for rid, external_user_id, banco, target, direction, cooldown, chat_id, last_triggered in rules:
        mercado = obtener_ultimo_mercado_banco(str(banco).upper())
        compra = float(mercado.get("compra") or 0)
        venta = float(mercado.get("venta") or 0)
        if compra <= 0 or venta <= 0:
            continue
        precio = venta if direction == "above" else compra
        hit = precio >= float(target) if direction == "above" else precio <= float(target)
        if not hit:
            continue
        if last_triggered:
            try:
                lt = last_triggered if last_triggered.tzinfo else last_triggered.replace(tzinfo=VET)
                if (ahora - lt).total_seconds() < int(cooldown or 1800):
                    continue
            except Exception:
                pass
        try:
            await telegram_app.bot.send_message(
                chat_id=int(chat_id),
                text=(f"🔔 VENBOT · Alerta personal\n"
                      f"• Banco: {banco}\n"
                      f"• Condición: {'Vender USDT' if direction == 'above' else 'Comprar USDT'} {'≥' if direction == 'above' else '≤'} {float(target):.2f} Bs\n"
                      f"• Precio actual: {precio:.2f} Bs\n"
                      f"• Comprar USDT: {compra:.2f} Bs\n"
                      f"• Vender USDT: {venta:.2f} Bs\n"
                      f"• Señal informativa; no garantiza un resultado futuro."))
            with obtener_conexion() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE venbot_alert_rules SET last_triggered_at=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s", (ahora, rid))
            disparadas += 1
            logger.info("Alerta personal disparada: rule=%s user=%s banco=%s", rid, external_user_id, banco)
        except Exception as e:
            logger.warning("No se pudo disparar alerta personal %s: %s", rid, e)
    return disparadas

@app.get("/api/alerts/rules")
def alert_rules_list(request: Request):
    user = _require_session_user(request)
    external_user_id = user["external_user_id"]
    rules = _alert_rules_for_user(external_user_id)
    limit = int(_foundation_entitlements(user)["limits"]["alerts"])
    return {"ok": True, "plan": _plan_vigente(user.get("plan_code"), user.get("plan_expires_at")), "limit": limit, "count": len(rules), "rules": rules}

@app.post("/api/alerts/rules")
def alert_rule_create(payload: AlertRuleCreateRequest, request: Request):
    user = _require_session_user(request)
    external_user_id = user["external_user_id"]
    limit = int(_foundation_entitlements(user)["limits"]["alerts"])
    if _alert_rule_count(external_user_id) >= limit:
        raise HTTPException(status_code=403, detail=f"Límite de alertas alcanzado para {_plan_efectivo(user.get('plan_code'))}: {limit}")
    banco = _validar_alerta_banco(payload.banco)
    direction = _validar_direccion_alerta(payload.direction)
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO venbot_alert_rules(external_user_id,banco,rule_type,target_value,direction,enabled,cooldown_seconds,telegram_chat_id)
                           VALUES(%s,%s,'price_target',%s,%s,TRUE,%s,%s)
                           RETURNING id""", (external_user_id,banco,payload.target_value,direction,payload.cooldown_seconds,payload.telegram_chat_id))
            rid = int(cur.fetchone()[0])
    return {"ok": True, "id": rid, "rules": _alert_rules_for_user(external_user_id)}

@app.patch("/api/alerts/rules/{rule_id}")
def alert_rule_update(rule_id: int, payload: AlertRuleUpdateRequest, request: Request):
    user = _require_session_user(request)
    external_user_id = user["external_user_id"]
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM venbot_alert_rules WHERE id=%s AND external_user_id=%s", (rule_id,external_user_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Alerta no encontrada")
            fields=[]; vals=[]
            if payload.banco is not None: fields.append("banco=%s"); vals.append(_validar_alerta_banco(payload.banco))
            if payload.target_value is not None: fields.append("target_value=%s"); vals.append(payload.target_value)
            if payload.direction is not None: fields.append("direction=%s"); vals.append(_validar_direccion_alerta(payload.direction))
            if payload.telegram_chat_id is not None: fields.append("telegram_chat_id=%s"); vals.append(payload.telegram_chat_id)
            if payload.cooldown_seconds is not None: fields.append("cooldown_seconds=%s"); vals.append(payload.cooldown_seconds)
            if payload.enabled is not None: fields.append("enabled=%s"); vals.append(payload.enabled)
            fields.append("updated_at=CURRENT_TIMESTAMP")
            vals.append(rule_id); vals.append(external_user_id)
            cur.execute(f"UPDATE venbot_alert_rules SET {', '.join(fields)} WHERE id=%s AND external_user_id=%s", tuple(vals))
    return {"ok": True, "rules": _alert_rules_for_user(external_user_id)}

@app.delete("/api/alerts/rules/{rule_id}")
def alert_rule_delete(rule_id: int, request: Request):
    user = _require_session_user(request)
    external_user_id = user["external_user_id"]
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM venbot_alert_rules WHERE id=%s AND external_user_id=%s", (rule_id,external_user_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Alerta no encontrada")
    return {"ok": True, "rules": _alert_rules_for_user(external_user_id)}

@app.get("/api/alerts/smart")
def smart_alerts(limit: int = Query(50, ge=1, le=200), banco: str = Query("GENERAL")):
    banco = (banco or "GENERAL").upper()
    if banco not in {"GENERAL", "MERCANTIL", "PROVINCIAL", "BNC"}:
        raise HTTPException(status_code=400, detail="Banco no soportado")
    return {"ok": True, "enabled": SMART_ALERTS_ENABLED, "cooldown_seconds": SMART_ALERT_COOLDOWN_SECONDS, "events": obtener_eventos_alerta(limit, banco)}


@app.post("/api/alerts/smart/evaluate")
def smart_alerts_evaluate():
    """Evalúa una vez el mercado actual; útil para dashboard y pruebas sin esperar al collector."""
    mercado = obtener_mercado_actual_db() or {}
    if not mercado:
        return {"ok": False, "error": "Sin mercado real disponible"}
    datos = motor_quant_inteligente(float(mercado.get("compra") or 0), float(mercado.get("venta") or 0), int(mercado.get("liquidez") or 0), "GENERAL")
    eventos = evaluar_alertas_inteligentes(mercado, datos, "GENERAL")
    return {"ok": True, "events_created": len(eventos), "events": obtener_eventos_alerta(20, "GENERAL"), "timestamp": datetime.now(VET).isoformat()}


@app.post("/api/auth/login")
def auth_login(payload: LoginRequest, request: Request):
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    now_mono = time.monotonic()
    client_key = request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or (request.client.host if request.client else "unknown")
    bucket = _AUTH_LOGIN_ATTEMPTS.setdefault(client_key, [])
    bucket[:] = [t for t in bucket if now_mono - t < AUTH_LOGIN_WINDOW_SECONDS]
    if len(bucket) >= AUTH_LOGIN_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Demasiados intentos de inicio de sesión. Intenta nuevamente más tarde.")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT external_user_id,username,password_hash,country_code,plan_code,status,plan_expires_at FROM venbot_users WHERE username=%s LIMIT 1", (payload.username.strip(),))
            row = cur.fetchone()
    if not row or not row[2] or not _verify_password(payload.password, row[2]) or row[5] != "active":
        bucket.append(now_mono)
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    _AUTH_LOGIN_ATTEMPTS.pop(client_key, None)
    token, expires = _create_session(row[0])
    user = dict(zip(["external_user_id","username","password_hash","country_code","plan_code","status","plan_expires_at"], row))
    return {"ok": True, "session_token": token, "expires_at": expires.isoformat(), "user": {"external_user_id": user["external_user_id"], "username": user["username"], "country_code": user["country_code"], "plan": _plan_vigente(user["plan_code"], user.get("plan_expires_at")), "status": user["status"], "plan_expires_at": user["plan_expires_at"].isoformat() if user["plan_expires_at"] else None}, "entitlements": _foundation_entitlements(user)}

@app.post("/api/auth/me")
def auth_me(payload: SessionRequest):
    user = _account_from_session(payload.session_token)
    if not user:
        raise HTTPException(status_code=401, detail="session_expired")
    return {"ok": True, "user": {"external_user_id": user["external_user_id"], "username": user["username"], "country_code": user["country_code"], "plan": _plan_vigente(user["plan_code"], user.get("plan_expires_at")), "status": user["status"], "plan_expires_at": user["plan_expires_at"].isoformat() if user["plan_expires_at"] else None}, "entitlements": _foundation_entitlements(user)}

@app.post("/api/auth/logout")
def auth_logout(payload: SessionRequest):
    if DATABASE_URL:
        token_hash = hashlib.sha256(payload.session_token.encode("utf-8")).hexdigest()
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE venbot_sessions SET revoked_at=CURRENT_TIMESTAMP WHERE token_hash=%s", (token_hash,))
    return {"ok": True}

@app.post("/api/auth/telegram-account")
def auth_telegram_account(payload: TelegramAccountRequest, request: Request):
    supplied = request.headers.get("X-Venbot-Telegram-Setup-Secret", "")
    if not TELEGRAM_ACCOUNT_SETUP_SECRET or not secrets.compare_digest(supplied, TELEGRAM_ACCOUNT_SETUP_SECRET):
        raise HTTPException(status_code=401, detail="telegram_account_setup_not_authorized")
    account, _ = _create_or_get_telegram_account(payload.telegram_chat_id, payload.country_code)
    credentials = _set_new_password(account["external_user_id"])
    return {"ok": True, "account": {"external_user_id": account["external_user_id"], "username": credentials["username"], "plan": _plan_efectivo(credentials["plan_code"]), "status": credentials["status"], "plan_expires_at": credentials["plan_expires_at"].isoformat() if credentials["plan_expires_at"] else None}, "password": credentials["password"]}

@app.post("/api/foundation/bootstrap")
def foundation_bootstrap(payload: FoundationBootstrapRequest, request: Request):
    session_token = request.headers.get("X-Venbot-Session", "").strip()
    authenticated = _account_from_session(session_token) if session_token else None
    if authenticated:
        user = authenticated
    else:
        user = _foundation_user(payload.external_user_id, payload.country_code)
    ent = _foundation_entitlements(user)
    policy = _billing_policy(user.get("country_code"))
    return {
        "ok": True,
        "user": {"external_user_id": user["external_user_id"], "username": user.get("username"), "country_code": user["country_code"], "plan": _plan_vigente(user.get("plan_code"), user.get("plan_expires_at")), "status": user["status"], "plan_expires_at": user.get("plan_expires_at").isoformat() if user.get("plan_expires_at") else None},
        "authenticated": bool(authenticated),
        "entitlements": ent,
        "billing": {"external_checkout_allowed": policy["external_checkout"], "provider": policy["provider"], "checkout_url_configured": bool(EXTERNAL_BILLING_URL)},
        "legal": {"privacy_url": "/legal/privacy", "terms_url": "/legal/terms"},
    }

    user = _foundation_user(payload.external_user_id, payload.country_code)
    ent = _foundation_entitlements(user)
    policy = _billing_policy(user.get("country_code"))
    return {
        "ok": True,
        "user": {"external_user_id": user["external_user_id"], "country_code": user["country_code"], "status": user["status"]},
        "entitlements": ent,
        "billing": {"external_checkout_allowed": policy["external_checkout"], "provider": policy["provider"], "checkout_url_configured": bool(EXTERNAL_BILLING_URL)},
        "legal": {"privacy_url": "/legal/privacy", "terms_url": "/legal/terms"},
    }

@app.post("/api/foundation/consent")
def foundation_consent(payload: ConsentRequest):
    if not DATABASE_URL:
        return {"ok": False, "error": "database_not_configured"}
    _foundation_user(payload.external_user_id)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO venbot_consents(external_user_id,consent_type,version,granted) VALUES (%s,%s,%s,%s) ON CONFLICT (external_user_id,consent_type,version) DO UPDATE SET granted=EXCLUDED.granted,created_at=CURRENT_TIMESTAMP""", (payload.external_user_id,payload.consent_type,payload.version,payload.granted))
    return {"ok": True}

@app.post("/api/account/delete")
def account_delete(payload: AccountDeleteRequest):
    if not DATABASE_URL:
        return {"ok": False, "error": "database_not_configured"}
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE venbot_users SET status='deleted', deleted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE external_user_id=%s", (payload.external_user_id,))
            cur.execute("DELETE FROM venbot_consents WHERE external_user_id=%s", (payload.external_user_id,))
            cur.execute("UPDATE venbot_sessions SET revoked_at=CURRENT_TIMESTAMP WHERE external_user_id=%s AND revoked_at IS NULL", (payload.external_user_id,))
    return {"ok": True, "status": "deleted"}

# ==========================================
# BILLING ADAPTER v1
# ==========================================
class BillingAdapter:
    name = "base"
    def create_checkout(self, **kwargs): raise NotImplementedError
    def get_checkout(self, provider_order_id): raise NotImplementedError

class PabiloBillingAdapter(BillingAdapter):
    name = "pabilo"
    base_url = "https://api.pabilo.app"
    def _headers(self):
        if not PABILO_API_KEY: raise RuntimeError("PABILO_API_KEY no configurada")
        return {"Authorization": f"Bearer {PABILO_API_KEY}", "Content-Type": "application/json"}
    def _webhook_url(self):
        if not RENDER_EXTERNAL_URL: raise RuntimeError("RENDER_EXTERNAL_URL no configurado")
        if not PABILO_WEBHOOK_SECRET: raise RuntimeError("PABILO_WEBHOOK_SECRET no configurado")
        return f"{RENDER_EXTERNAL_URL}/api/billing/webhook/pabilo?{urlencode({'secret': PABILO_WEBHOOK_SECRET})}"
    def create_checkout(self, *, order_id, plan_code, description, pay_currency, amount_usdt, external_user_id):
        if not PABILO_USER_BANK_ID: raise RuntimeError("PABILO_USER_BANK_ID no configurado")
        if pay_currency not in {"USDT", "VES"}: raise RuntimeError("Moneda no soportada")
        provider_currency = "USDT" if pay_currency == "USDT" else "USD"
        body = {"amount": round(float(amount_usdt), 2), "currency": provider_currency, "description": description, "user_bank_id": PABILO_USER_BANK_ID, "webhook_url": self._webhook_url(), "expiration_time": PABILO_EXPIRATION_MINUTES, "rate_expiration_time": PABILO_RATE_EXPIRATION_MINUTES}
        r = requests.post(f"{self.base_url}/v1/paymentlink", headers=self._headers(), json=body, timeout=15)
        if r.status_code >= 400: raise RuntimeError(f"Pabilo HTTP {r.status_code}: {r.text[:500]}")
        data=r.json(); provider_id=data.get("id"); url=data.get("url")
        if not provider_id or not url: raise RuntimeError("Pabilo no devolvió id/url de checkout")
        rate=data.get("rate_exchange")
        return {"provider":self.name,"provider_order_id":provider_id,"checkout_url":url,"provider_currency":provider_currency,"quoted_amount":float(data.get("amount") or amount_usdt),"quote_rate":float(rate) if rate is not None else None,"quote_source":"pabilo_rate_exchange" if rate is not None else None,"expires_at":datetime.now(VET)+timedelta(minutes=PABILO_EXPIRATION_MINUTES),"payload":data}
    def get_checkout(self, provider_order_id):
        r=requests.get(f"{self.base_url}/paymentlink/{provider_order_id}",headers=self._headers(),timeout=10)
        if r.status_code>=400: raise RuntimeError(f"Pabilo HTTP {r.status_code}: {r.text[:500]}")
        return r.json()

def _billing_adapter():
    if BILLING_PROVIDER == "pabilo": return PabiloBillingAdapter()
    raise RuntimeError(f"BILLING_PROVIDER no soportado: {BILLING_PROVIDER}")

def _plan_price_usdt(plan_code):
    plan=_plan_efectivo(plan_code)
    return PREMIUM_PRICE_USDT if plan=="PREMIUM" else VIP_PRICE_USDT if plan=="VIP" else 0.0

def _billing_order_row(row):
    keys=["order_id","external_user_id","telegram_chat_id","plan_code","base_amount_usdt","pay_currency","provider_currency","quoted_amount","quote_rate","quote_source","provider","provider_order_id","checkout_url","status","expires_at","paid_at","created_at","updated_at","payload"]
    out=dict(zip(keys,row))
    for k in ["expires_at","paid_at","created_at","updated_at"]:
        if out.get(k): out[k]=out[k].isoformat()
    return out

def _manual_price(plan_code, currency):
    if currency == "USDT":
        return _plan_price_usdt(plan_code)
    if currency == "VES":
        plan = _plan_efectivo(plan_code)
        return PREMIUM_PRICE_VES if plan == "PREMIUM" else VIP_PRICE_VES if plan == "VIP" else 0.0
    return 0.0

def _create_manual_billing_order(external_user_id, telegram_chat_id, plan_code, pay_currency):
    plan = _plan_efectivo(plan_code); currency = (pay_currency or "").upper().strip()
    if plan not in {"PREMIUM", "VIP"}: raise HTTPException(status_code=400, detail="Solo PREMIUM o VIP pueden comprarse")
    if currency not in {"VES", "USDT"}: raise HTTPException(status_code=400, detail="pay_currency debe ser VES o USDT")
    amount_usdt = _plan_price_usdt(plan)
    quoted = _manual_price(plan, currency)
    if amount_usdt <= 0: raise HTTPException(status_code=503, detail=f"Precio {plan} en USDT no configurado")
    if quoted <= 0: raise HTTPException(status_code=503, detail=f"Precio {plan} en {currency} no configurado")
    order_id = "VB-" + datetime.now(VET).strftime("%Y%m%d") + "-" + secrets.token_hex(5).upper()
    expires = datetime.now(VET) + timedelta(hours=MANUAL_ORDER_EXPIRATION_HOURS)
    payload = {"mode":"manual", "instructions_version":1}
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO venbot_billing_orders(order_id,external_user_id,telegram_chat_id,plan_code,base_amount_usdt,pay_currency,provider_currency,quoted_amount,quote_rate,quote_source,provider,provider_order_id,checkout_url,status,expires_at,payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'manual',NULL,NULL,'PAYMENT_PENDING',%s,%s)""",
                        (order_id,external_user_id,telegram_chat_id,plan,amount_usdt,currency,currency,quoted, (quoted/amount_usdt if currency=="VES" else 1.0), "manual_config", expires, json.dumps(payload)))
    return {"ok":True,"order_id":order_id,"plan_code":plan,"pay_currency":currency,"base_amount_usdt":amount_usdt,"quoted_amount":quoted,"status":"PAYMENT_PENDING","expires_at":expires.isoformat()}

def _create_billing_order(external_user_id, telegram_chat_id, plan_code, pay_currency):
    if BILLING_PROVIDER == "manual":
        return _create_manual_billing_order(external_user_id, telegram_chat_id, plan_code, pay_currency)
    plan=_plan_efectivo(plan_code); currency=(pay_currency or "").upper().strip()
    if plan not in {"PREMIUM","VIP"}: raise HTTPException(status_code=400,detail="Solo PREMIUM o VIP pueden comprarse")
    if currency not in {"VES","USDT"}: raise HTTPException(status_code=400,detail="pay_currency debe ser VES o USDT")
    amount=_plan_price_usdt(plan)
    if amount<=0: raise HTTPException(status_code=503,detail=f"Precio {plan} no configurado. Define {plan}_PRICE_USDT en Render.")
    if not DATABASE_URL: raise HTTPException(status_code=503,detail="database_not_configured")
    order_id="VB-"+datetime.now(VET).strftime("%Y%m%d")+"-"+secrets.token_hex(5).upper()
    try: checkout=_billing_adapter().create_checkout(order_id=order_id,plan_code=plan,description=f"Venbot {plan} · {order_id}",pay_currency=currency,amount_usdt=amount,external_user_id=external_user_id)
    except Exception as e: logger.exception("Error creando checkout %s",order_id); raise HTTPException(status_code=502,detail=f"No se pudo crear el checkout: {e}")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO venbot_billing_orders(order_id,external_user_id,telegram_chat_id,plan_code,base_amount_usdt,pay_currency,provider_currency,quoted_amount,quote_rate,quote_source,provider,provider_order_id,checkout_url,status,expires_at,payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PAYMENT_PENDING',%s,%s)""",(order_id,external_user_id,telegram_chat_id,plan,amount,currency,checkout["provider_currency"],checkout["quoted_amount"],checkout.get("quote_rate"),checkout.get("quote_source"),checkout["provider"],checkout["provider_order_id"],checkout["checkout_url"],checkout.get("expires_at"),json.dumps(checkout.get("payload") or {})))
    return {"ok":True,"order_id":order_id,"plan_code":plan,"pay_currency":currency,"base_amount_usdt":amount,"quoted_amount":checkout["quoted_amount"],"checkout_url":checkout["checkout_url"],"status":"PAYMENT_PENDING","expires_at":checkout.get("expires_at").isoformat() if checkout.get("expires_at") else None}

def _is_billing_admin(chat_id):
    return bool(BILLING_ADMIN_TELEGRAM_CHAT_ID and str(chat_id) == BILLING_ADMIN_TELEGRAM_CHAT_ID)

def _save_manual_proof(order_id, external_user_id, file_id=None, reference=None):
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE venbot_billing_orders SET proof_file_id=COALESCE(%s,proof_file_id), proof_reference=COALESCE(%s,proof_reference), submitted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP, status='PAYMENT_PENDING' WHERE order_id=%s AND external_user_id=%s AND status IN ('PAYMENT_PENDING','PAYMENT_FAILED') RETURNING order_id""", (file_id, reference, order_id, external_user_id))
            row=cur.fetchone()
    return bool(row)

def _pending_manual_orders(limit=20):
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT order_id,external_user_id,telegram_chat_id,plan_code,pay_currency,quoted_amount,status,proof_reference,submitted_at,created_at FROM venbot_billing_orders WHERE provider='manual' AND status='PAYMENT_PENDING' ORDER BY submitted_at DESC NULLS LAST,created_at DESC LIMIT %s""", (limit,))
            rows=cur.fetchall()
    return rows

def _approve_manual_order(order_id, admin_chat_id):
    if not _is_billing_admin(admin_chat_id): raise HTTPException(status_code=403, detail="not_billing_admin")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT external_user_id,telegram_chat_id,plan_code,pay_currency,quoted_amount,status,proof_reference,proof_file_id,expires_at FROM venbot_billing_orders WHERE order_id=%s AND provider='manual' LIMIT 1""", (order_id,))
            row=cur.fetchone()
            if not row: raise HTTPException(status_code=404, detail="order_not_found")
            external_id,tg_id,plan,currency,amount,status,reference,proof_file,expires_at=row
            if status == "PAYMENT_PAID": return {"ok":True,"idempotent":True,"order_id":order_id}
            if status != "PAYMENT_PENDING": raise HTTPException(status_code=400, detail="order_not_pending")
            if not proof_file or not reference:
                raise HTTPException(status_code=400, detail="proof_and_reference_required")
            if expires_at and expires_at <= datetime.now(VET):
                cur.execute("UPDATE venbot_billing_orders SET status='PAYMENT_EXPIRED',updated_at=CURRENT_TIMESTAMP WHERE order_id=%s AND status='PAYMENT_PENDING'", (order_id,))
                raise HTTPException(status_code=400, detail="order_expired")
            cur.execute("UPDATE venbot_billing_orders SET status='PAYMENT_PAID',paid_at=COALESCE(paid_at,CURRENT_TIMESTAMP),reviewed_at=CURRENT_TIMESTAMP,reviewed_by=%s,updated_at=CURRENT_TIMESTAMP WHERE order_id=%s AND status='PAYMENT_PENDING'", (str(admin_chat_id),order_id))
            if cur.rowcount != 1:
                raise HTTPException(status_code=409, detail="order_state_changed")
    activation=BillingWebhookRequest(external_user_id=external_id,telegram_chat_id=tg_id,plan_code=plan,event_type="payment_succeeded",external_reference=order_id,provider="manual",country_code="VE",payload={"pay_currency":currency,"amount":amount,"reference":reference,"proof_file_id":proof_file,"reviewed_by":str(admin_chat_id)})
    result=_billing_activate_account(activation)
    result.update({"order_id":order_id,"pay_currency":currency,"quoted_amount":amount})
    return result

def _reject_manual_order(order_id, admin_chat_id, reason="Pago no validado"):
    if not _is_billing_admin(admin_chat_id): raise HTTPException(status_code=403, detail="not_billing_admin")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE venbot_billing_orders SET status='PAYMENT_FAILED',reviewed_at=CURRENT_TIMESTAMP,reviewed_by=%s,rejection_reason=%s,updated_at=CURRENT_TIMESTAMP WHERE order_id=%s AND provider='manual' AND status='PAYMENT_PENDING' RETURNING telegram_chat_id,plan_code", (str(admin_chat_id),reason[:500],order_id))
            row=cur.fetchone()
    if not row: raise HTTPException(status_code=404, detail="pending_order_not_found")
    return {"ok":True,"order_id":order_id,"status":"PAYMENT_FAILED","telegram_chat_id":row[0],"plan_code":row[1]}

def _mark_billing_order_from_pabilo(payload):
    provider_id=payload.get("payment_link_id") or (payload.get("payment_link") or {}).get("id"); status=str(payload.get("status") or (payload.get("payment_link") or {}).get("status") or "").lower()
    if not provider_id: raise HTTPException(status_code=400,detail="payment_link_id faltante")
    normalized={"paid":"PAYMENT_PAID","failed":"PAYMENT_FAILED","expired":"PAYMENT_EXPIRED","cancelled":"PAYMENT_EXPIRED","canceled":"PAYMENT_EXPIRED","active":"PAYMENT_PENDING","pending":"PAYMENT_PENDING"}.get(status,"PAYMENT_PENDING")
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT order_id,external_user_id,telegram_chat_id,plan_code,base_amount_usdt,pay_currency,provider_currency,quoted_amount,status,expires_at FROM venbot_billing_orders WHERE provider=%s AND provider_order_id=%s LIMIT 1",("pabilo",provider_id)); row=cur.fetchone()
            if not row: raise HTTPException(status_code=404,detail="Orden Venbot no encontrada")
            order_id,external_id,tg_id,plan,base_amount,pay_currency,provider_currency,quoted_amount,current_status,expires_at=row
            payment=payload.get("user_bank_payment") or {}; paid_amount=payment.get("amount")
            if normalized=="PAYMENT_PAID" and paid_amount is not None and abs(float(paid_amount)-float(quoted_amount))>max(0.01,float(quoted_amount)*0.005): logger.error("Pago Pabilo con monto no coincidente: order=%s expected=%s received=%s",order_id,quoted_amount,paid_amount); normalized="PAYMENT_FAILED"
            cur.execute("UPDATE venbot_billing_orders SET status=%s,paid_at=CASE WHEN %s='PAYMENT_PAID' THEN COALESCE(paid_at,CURRENT_TIMESTAMP) ELSE paid_at END,updated_at=CURRENT_TIMESTAMP,payload=%s WHERE order_id=%s",(normalized,normalized,json.dumps(payload),order_id))
    if normalized=="PAYMENT_PAID":
        activation=BillingWebhookRequest(external_user_id=external_id,telegram_chat_id=tg_id,plan_code=plan,event_type="payment_succeeded",external_reference=order_id,provider="pabilo",country_code="VE",payload=payload)
        return {"ok":True,"order_id":order_id,"status":normalized,"activation":_billing_activate_account(activation)}
    return {"ok":True,"order_id":order_id,"status":normalized}


def _billing_duration_days(plan_code: str, requested: Optional[int] = None) -> int:
    if requested:
        return int(requested)
    return PREMIUM_DURATION_DAYS if plan_code == "PREMIUM" else VIP_DURATION_DAYS

def _billing_activate_account(payload: BillingWebhookRequest):
    plan = _plan_efectivo(payload.plan_code)
    if plan not in {"PREMIUM", "VIP"}:
        raise HTTPException(status_code=400, detail="plan_code debe ser PREMIUM o VIP")
    event = payload.event_type.lower().strip()
    if event not in {"payment_succeeded", "subscription_renewed", "subscription_cancelled", "payment_failed"}:
        raise HTTPException(status_code=400, detail="event_type no soportado")
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="database_not_configured")
    duration = _billing_duration_days(plan, payload.duration_days)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            # Resolve account from stable identifiers. Telegram is the commercial identity.
            if payload.external_user_id:
                cur.execute("SELECT external_user_id,username,telegram_chat_id,plan_code,plan_expires_at,status FROM venbot_users WHERE external_user_id=%s LIMIT 1", (payload.external_user_id,))
            elif payload.telegram_chat_id is not None:
                cur.execute("SELECT external_user_id,username,telegram_chat_id,plan_code,plan_expires_at,status FROM venbot_users WHERE telegram_chat_id=%s LIMIT 1", (int(payload.telegram_chat_id),))
            elif payload.username:
                cur.execute("SELECT external_user_id,username,telegram_chat_id,plan_code,plan_expires_at,status FROM venbot_users WHERE username=%s LIMIT 1", (payload.username.strip(),))
            else:
                raise HTTPException(status_code=400, detail="Falta identificador de cuenta")
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Cuenta Venbot no encontrada")
            external_id, username, tg_id, current_plan, current_exp, status = row
            cur.execute("SELECT 1 FROM venbot_billing_events WHERE external_reference=%s AND event_type=%s LIMIT 1", (payload.external_reference, event))
            if cur.fetchone():
                return {"ok": True, "idempotent": True, "external_user_id": external_id, "plan": _plan_vigente(current_plan, current_exp), "plan_expires_at": current_exp.isoformat() if current_exp else None}
            new_plan = plan
            new_exp = current_exp
            if event in {"payment_succeeded", "subscription_renewed"}:
                base = current_exp if current_exp and current_exp > datetime.now(VET) else datetime.now(VET)
                new_exp = base + timedelta(days=duration)
                cur.execute("UPDATE venbot_users SET plan_code=%s,status='active',plan_expires_at=%s,updated_at=CURRENT_TIMESTAMP WHERE external_user_id=%s", (plan, new_exp, external_id))
            elif event == "subscription_cancelled":
                # Cancellation stops future renewal but preserves the already-paid period.
                cur.execute("UPDATE venbot_users SET updated_at=CURRENT_TIMESTAMP WHERE external_user_id=%s", (external_id,))
                new_plan = current_plan
                new_exp = current_exp
            elif event == "payment_failed":
                cur.execute("UPDATE venbot_users SET updated_at=CURRENT_TIMESTAMP WHERE external_user_id=%s", (external_id,))
            cur.execute("INSERT INTO venbot_billing_events(external_user_id,country_code,plan_code,provider,external_reference,event_type,status,payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (external_id, payload.country_code.upper(), plan, payload.provider, payload.external_reference, event, "processed", json.dumps(payload.payload or {})))
    return {"ok": True, "idempotent": False, "external_user_id": external_id, "username": username, "telegram_chat_id": tg_id, "plan": _plan_efectivo(new_plan), "plan_expires_at": new_exp.isoformat() if new_exp else None}

@app.get("/api/plans")
def api_plans(request: Request):
    country = _country_from_request(request)
    policy = _billing_policy(country)
    return {
        "ok": True,
        "country_code": country,
        "plans": {
            "FREE": {"price_label": "Gratis", "duration_days": None, "limits": PLAN_LIMITS["FREE"]},
            "PREMIUM": {"price_label": PREMIUM_PRICE_LABEL or "Consultar checkout", "duration_days": PREMIUM_DURATION_DAYS, "limits": PLAN_LIMITS["PREMIUM"]},
            "VIP": {"price_label": VIP_PRICE_LABEL or "Consultar checkout", "duration_days": VIP_DURATION_DAYS, "limits": PLAN_LIMITS["VIP"]},
        },
        "billing": {"channel": "telegram", "external_checkout_allowed": policy["external_checkout"], "provider": BILLING_PROVIDER, "checkout_url_configured": BILLING_PROVIDER == "manual" or bool(PABILO_API_KEY and PABILO_USER_BANK_ID and PABILO_WEBHOOK_SECRET), "dual_currency": True, "manual_validation": BILLING_PROVIDER == "manual", "prices_configured": bool(PREMIUM_PRICE_USDT > 0 and VIP_PRICE_USDT > 0 and PREMIUM_PRICE_VES > 0 and VIP_PRICE_VES > 0)},
    }

def _require_session_user(request: Request):
    token=request.headers.get("X-Venbot-Session","").strip(); user=_account_from_session(token)
    if not user: raise HTTPException(status_code=401,detail="unauthorized")
    return user

@app.post("/api/billing/orders")
def billing_create_order(payload:BillingOrderCreateRequest,request:Request):
    user=_require_session_user(request)
    if user.get("country_code",DEFAULT_COUNTRY_CODE).upper()!="VE": raise HTTPException(status_code=403,detail="billing_market_not_enabled")
    if not _billing_policy(user.get("country_code")).get("external_checkout"): raise HTTPException(status_code=403,detail="external_checkout_not_allowed")
    return _create_billing_order(user["external_user_id"],None,payload.plan_code,payload.pay_currency)

@app.get("/api/billing/orders/{order_id}")
def billing_get_order(order_id:str,request:Request):
    user=_require_session_user(request)
    with obtener_conexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT order_id,external_user_id,telegram_chat_id,plan_code,base_amount_usdt,pay_currency,provider_currency,quoted_amount,quote_rate,quote_source,provider,provider_order_id,checkout_url,status,expires_at,paid_at,created_at,updated_at,payload FROM venbot_billing_orders WHERE order_id=%s AND external_user_id=%s LIMIT 1",(order_id,user["external_user_id"]))
            row=cur.fetchone()
    if not row: raise HTTPException(status_code=404,detail="order_not_found")
    return {"ok":True,"order":_billing_order_row(row)}

@app.get("/api/billing/manual/pending")
def billing_manual_pending(request: Request):
    secret=request.headers.get("X-Venbot-Billing-Admin", "")
    if not BILLING_WEBHOOK_SECRET or not secrets.compare_digest(secret, BILLING_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")
    rows=_pending_manual_orders(100)
    return {"ok":True,"orders":[{"order_id":r[0],"external_user_id":r[1],"telegram_chat_id":r[2],"plan_code":r[3],"pay_currency":r[4],"quoted_amount":r[5],"status":r[6],"proof_reference":r[7],"submitted_at":r[8].isoformat() if r[8] else None,"created_at":r[9].isoformat() if r[9] else None} for r in rows]}

class ManualReviewRequest(BaseModel):
    order_id: str = Field(min_length=8, max_length=80)
    action: str = Field(min_length=6, max_length=10)
    reason: Optional[str] = Field(default=None, max_length=500)

@app.post("/api/billing/manual/review")
def billing_manual_review(payload: ManualReviewRequest, request: Request):
    secret=request.headers.get("X-Venbot-Billing-Admin", "")
    if not BILLING_WEBHOOK_SECRET or not secrets.compare_digest(secret, BILLING_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")
    if payload.action.lower()=="approve":
        return _approve_manual_order(payload.order_id, BILLING_ADMIN_TELEGRAM_CHAT_ID or "api")
    if payload.action.lower()=="reject":
        return _reject_manual_order(payload.order_id, BILLING_ADMIN_TELEGRAM_CHAT_ID or "api", payload.reason or "Pago no validado")
    raise HTTPException(status_code=400, detail="action debe ser approve o reject")

@app.post("/api/billing/webhook/pabilo")
async def billing_webhook_pabilo(request:Request):
    if not PABILO_WEBHOOK_SECRET: raise HTTPException(status_code=503,detail="pabilo_webhook_not_configured")
    supplied=request.query_params.get("secret","")
    if not supplied or not secrets.compare_digest(supplied,PABILO_WEBHOOK_SECRET): raise HTTPException(status_code=401,detail="unauthorized")
    payload=await request.json(); result=await asyncio.to_thread(_mark_billing_order_from_pabilo,payload); activation=result.get("activation") if isinstance(result,dict) else None
    if activation and activation.get("telegram_chat_id") and telegram_app and activation.get("idempotent") is False:
        try:
            exp=activation.get("plan_expires_at") or "sin fecha"
            asyncio.create_task(telegram_app.bot.send_message(chat_id=int(activation["telegram_chat_id"]),text=f"✅ *Venbot: pago confirmado*\n\n💎 Plan: *{activation['plan']}*\n📅 Válido hasta: `{exp}`\n\nTu acceso ya está activo. Usa /cuenta para consultar tu plan.",parse_mode="Markdown"))
        except Exception: logger.exception("No se pudo notificar pago Pabilo por Telegram")
    return result


@app.post("/api/billing/webhook")
def billing_webhook(payload: BillingWebhookRequest, request: Request):
    if not BILLING_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="billing_webhook_not_configured")
    supplied = request.headers.get("X-Venbot-Billing-Secret", "")
    if not supplied or not secrets.compare_digest(supplied, BILLING_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")
    result = _billing_activate_account(payload)
    tg_id = result.get("telegram_chat_id")
    if tg_id and telegram_app and result.get("idempotent") is False:
        try:
            if payload.event_type in {"payment_succeeded", "subscription_renewed"}:
                exp = result.get("plan_expires_at") or "sin fecha"
                asyncio.create_task(telegram_app.bot.send_message(chat_id=int(tg_id), text=f"✅ *Venbot: plan activado*\n\n💎 Plan: *{result['plan']}*\n📅 Válido hasta: `{exp}`\n\nUsa /cuenta para consultar tu cuenta y /credenciales para obtener acceso a la interfaz.", parse_mode="Markdown"))
        except Exception:
            logger.exception("No se pudo notificar activación por Telegram")
    return result

@app.get("/api/foundation/config")
def foundation_config(request: Request):
    country = _country_from_request(request)
    policy = _billing_policy(country)
    return {
        "ok": True,
        "country_code": country,
        "plans": PLAN_LIMITS,
        "features": FEATURE_MIN_PLAN,
        "billing": {"external_checkout_allowed": policy["external_checkout"], "provider": policy["provider"], "checkout_url_configured": bool(EXTERNAL_BILLING_URL)},
        "beta": {"premium_access": BETA_PREMIUM_ACCESS, "vip_access": BETA_VIP_ACCESS},
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
    contact = PRIVACY_CONTACT_EMAIL or "el canal oficial de soporte publicado dentro de la aplicación"
    return _legal_html("Política de privacidad", f"""<h1>Política de privacidad</h1><p>Venbot procesa únicamente los datos necesarios para operar las funciones que el usuario solicite, mantener la seguridad, medir el funcionamiento del servicio y, cuando corresponda, administrar una cuenta, alertas, suscripciones o publicidad.</p><h2>Datos que podemos procesar</h2><ul><li>Identificadores técnicos o de sesión necesarios para el funcionamiento y el contador aproximado de usuarios conectados.</li><li>Datos de cuenta que el usuario proporcione voluntariamente cuando se habiliten cuentas.</li><li>Preferencias de funciones, alertas y plan.</li><li>Datos técnicos necesarios para prevenir abuso, errores y problemas de seguridad.</li></ul><h2>Proveedores externos</h2><p>Venbot puede utilizar Binance P2P, fuentes oficiales del BCV, alojamiento, bases de datos, proveedores de IA, analítica y publicidad. Cada proveedor puede procesar datos conforme a sus propias condiciones y políticas.</p><h2>Publicidad y consentimiento</h2><p>La versión gratuita podrá mostrar publicidad. Cuando la normativa lo requiera, Venbot solicitará el consentimiento correspondiente antes de utilizar tecnologías publicitarias que lo necesiten. Las preferencias podrán cambiarse mediante los mecanismos disponibles en la aplicación.</p><h2>Suscripciones y pagos</h2><p>Los planes Premium y VIP están diseñados para utilizar un checkout externo cuando la normativa y las reglas de distribución aplicables lo permitan. Venbot no almacena números completos de tarjetas ni credenciales de pago.</p><h2>Conservación y eliminación</h2><p>Conservaremos los datos durante el tiempo necesario para prestar el servicio, cumplir obligaciones legales, resolver disputas y proteger el sistema. Cuando exista una cuenta, el usuario podrá solicitar su eliminación mediante el mecanismo de eliminación de cuenta disponible en el servicio.</p><h2>Contacto</h2><p>Contacto de privacidad: {contact}</p>""")

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


@app.get("/api/spot")
def obtener_spot_api(request: Request, symbol: Optional[str] = Query(None), refresh: bool = Query(False)):
    _require_plan_user(request, "VIP")
    requested = [_normalizar_spot_symbol(symbol)] if symbol else list(SPOT_SYMBOLS)
    with SPOT_LOCK:
        cached = dict(SPOT_CACHE.get("value") or {})
        valid_cache = time.monotonic() < float(SPOT_CACHE.get("expires") or 0)
    if refresh or not valid_cache or any(sym not in cached for sym in requested):
        cached = recolectar_spot(requested, persist=bool(DATABASE_URL))
    data = {sym: cached.get(sym) or obtener_spot_snapshot_db(sym) for sym in requested}
    clean = {}
    for sym, item in data.items():
        if not item:
            continue
        ts = item.get("timestamp")
        clean[sym] = {
            "symbol": sym, "price": round(float(item.get("price") or 0), 8),
            "bid": round(float(item.get("bid") or 0), 8), "ask": round(float(item.get("ask") or 0), 8),
            "change_24h_pct": round(float(item.get("change_24h_pct") or 0), 3),
            "quote_volume_24h": round(float(item.get("quote_volume_24h") or 0), 2),
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else ts,
            "source": item.get("source"),
        }
    return {"ok": bool(clean), "symbols": clean, "configured_symbols": list(SPOT_SYMBOLS)}


@app.get("/api/spot/history")
def obtener_spot_history(
    request: Request,
    symbol: str = Query("BTCUSDT"),
    interval: str = Query("5m", pattern="^(1m|5m|15m|30m|1h|4h|1d)$"),
    limit: int = Query(170, ge=10, le=500),
):
    _require_plan_user(request, "VIP")
    try:
        candles = obtener_spot_klines(symbol, interval, limit)
        return {"ok": True, "symbol": _normalizar_spot_symbol(symbol), "interval": interval, "count": len(candles), "candles": candles, "source": "Binance Spot public klines"}
    except Exception as e:
        logger.warning("Spot history falló: %s", e)
        return {"ok": False, "symbol": str(symbol).upper(), "interval": interval, "count": 0, "candles": [], "error": "Fuente Spot temporalmente no disponible"}


@app.get("/api/quant/v2")
def obtener_quant_v2_api(include_spot: bool = Query(True)):
    mercado = obtener_mercado_actual_db() or {}
    compra = float(mercado.get("compra", 0) or 0)
    venta = float(mercado.get("venta", 0) or 0)
    liq = int(mercado.get("liquidez", 0) or 0)
    if compra <= 0 or venta <= 0:
        return {"ok": False, "error": "Sin lectura P2P válida"}
    result = QUANT_ENGINE_V2.analyze(compra, venta, liq, "GENERAL", include_spot)
    return {"ok": True, **result}


@app.get("/api/quant/backtest")
def obtener_quant_backtest(
    banco: str = Query("GENERAL"),
    max_evaluaciones: int = Query(24, ge=1, le=100),
    spacing_minutes: int = Query(60, ge=15, le=1440),
):
    banco = (banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL", "MERCANTIL", "PROVINCIAL", "BNC"}:
        banco = "GENERAL"
    return {
        "ok": True,
        "bank": banco,
        "backtest_7h": backtest_quant_7h(
            banco,
            max_evaluaciones,
            spacing_minutes,
        ),
    }


@app.get("/api/predictions/performance")
def obtener_prediction_performance_api(
    banco: str = Query("GENERAL"),
    limit: int = Query(100, ge=10, le=1000),
):
    banco=(banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL","MERCANTIL","PROVINCIAL","BNC"}: banco="GENERAL"
    return obtener_prediction_performance(banco, limit)


@app.get("/api/predictions/recent")
def obtener_prediction_recent_api(
    banco: str = Query("GENERAL"),
    limit: int = Query(20, ge=1, le=100),
):
    banco=(banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL","MERCANTIL","PROVINCIAL","BNC"}: banco="GENERAL"
    if not DATABASE_URL: return {"ok":False,"predictions":[]}
    try:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id,created_at,actual_compra,actual_venta,pred_compra_7h,pred_venta_7h,tendencia,regimen,confidence,actual_mid_7h,error_pct_7h,direction_correct_7h
                    FROM venbot_prediction_events WHERE banco=%s ORDER BY created_at DESC LIMIT %s
                """,(banco,int(limit)))
                rows=cur.fetchall()
        return {"ok":True,"bank":banco,"predictions":[{"id":r[0],"created_at":r[1].isoformat(),"actual_compra":r[2],"actual_venta":r[3],"pred_compra_7h":r[4],"pred_venta_7h":r[5],"tendencia":r[6],"regimen":r[7],"confidence":r[8],"actual_mid_7h":r[9],"error_pct_7h":r[10],"direction_correct_7h":r[11]} for r in rows]}
    except Exception:
        return {"ok":False,"predictions":[],"message":"Historial de predicciones temporalmente no disponible"}


@app.get("/api/predictions/signal")
def obtener_prediction_signal_api(banco: str = Query("GENERAL")):
    banco=(banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL","MERCANTIL","PROVINCIAL","BNC"}: banco="GENERAL"
    mercado=obtener_ultimo_mercado_banco(banco)
    c=float(mercado.get("compra",0) or 0); v=float(mercado.get("venta",0) or 0)
    if c<=0 or v<=0: return {"ok":False,"error":"Sin lectura P2P válida"}
    q=motor_quant_inteligente(c,v,int(mercado.get("liquidez",0) or 0),banco)
    return {"ok":True,"bank":banco,"signal":evaluar_senal_operativa(q,c,v),"quant":q}


@app.get("/api/estado")
def obtener_estado_sistema_api(banco: str = Query("GENERAL")):
    banco=(banco or "GENERAL").upper().strip()
    if banco not in {"GENERAL","MERCANTIL","PROVINCIAL","BNC"}: banco="GENERAL"
    mercado=obtener_ultimo_mercado_banco(banco) or {}
    c=float(mercado.get("compra",0) or 0); v=float(mercado.get("venta",0) or 0)
    q=motor_quant_inteligente(c,v,int(mercado.get("liquidez",0) or 0),banco) if c>0 and v>0 else {}
    perf=obtener_prediction_performance(banco,100)
    return {"ok":True,"bank":banco,"services":{"p2p":c>0 and v>0,"quant":True,"alerts":True,"billing_manual":BILLING_PROVIDER=="manual","prediction_tracking":PREDICTION_TRACKING_ENABLED},"data":{"muestras":q.get("muestras",0),"coverage_hours":q.get("cobertura_horas",0),"calibration_24h":q.get("calibracion_24h",{}),"performance":perf}}


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

Mantén continuidad real con el historial y responde de forma natural, cálida y fluida, como un buen asistente humano. Si la consulta NO es de mercado, eres un asistente general completo: puedes conversar y ayudar con cualquier tema permitido del mundo —ciencia, historia, tecnología, programación, matemáticas, viajes, educación, actualidad, cultura, redacción, ideas y tareas cotidianas— sin intentar llevar la conversación a P2P. Para preguntas actuales o sobre noticias, usa la búsqueda web cuando esté disponible y diferencia claramente hechos recientes de conocimiento general.

Para consultas de mercado, usa exactamente el mismo motor cuantitativo que alimenta monitor y Telegram y, cuando en el futuro exista contexto Spot o mercados exteriores, intégralo sin mezclar mercados ni inventar datos. Concluye primero, después explica los datos relevantes y termina con una lectura práctica. Escribe en párrafos cortos y cómodos de leer; usa listas solo cuando realmente ayuden. No repitas el contexto completo, no cortes frases, no dejes oraciones incompletas y no sacrifiques claridad por meter más números. Si una respuesta puede ser sencilla, sé sencilla; si el usuario pide profundidad, profundiza. Si te preguntan por una predicción a 7H, usa proyeccion_7h y explica que es un escenario estadístico central con rango estimado, no certeza. No prometas ganancias ni certeza financiera."""


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
                max_output_tokens=950 if market_query else 850,
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
    else:
        text = None
        if GEMINI_API_KEY:
            text = _respuesta_gemini_interactions_rest(
                prompt, GEMINI_MODEL, temperature=0.38, system_instruction=system,
                max_output_tokens=850, timeout=12, tools=None
            )
        if not text and OPENROUTER_API_KEY:
            text = _respuesta_openrouter(prompt, 0.38)
        yield _stream_event(text or "No pude responder en este momento. Intenta de nuevo en unos segundos.")
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
        max_tokens, temperature = 950, 0.18
    else:
        system = VENBOT_AI_SYSTEM + "\n\nPara preguntas generales responde de forma concisa: normalmente 1-3 párrafos. No conviertas una pregunta sencilla en un ensayo."
        max_tokens, temperature = 850, 0.38

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

    if GEMINI_API_KEY:
        logger.info("AI CHAT: Gemini general sin búsqueda como fallback")
        text = _respuesta_gemini_interactions_rest(
            prompt, GEMINI_MODEL, temperature=0.38, system_instruction=system,
            max_output_tokens=850, timeout=12, tools=None
        )
        if text:
            return text

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
async def ai_chat_stream(payload: AIChatRequest, request: Request):
    user = _require_session_user(request)
    quota = _consume_ai_quota(user)
    if not quota["allowed"]:
        raise HTTPException(status_code=429, detail={"error":"ai_daily_limit", "limit":quota["limit"], "used":quota["used"]})
    return StreamingResponse(_generador_ai_stream(payload.message.strip(), payload.history), media_type="text/event-stream", headers={"Cache-Control":"no-cache", "Connection":"keep-alive", "X-Accel-Buffering":"no"})


@app.get("/api/ai/usage")
def ai_usage(request: Request):
    user = _require_session_user(request)
    plan = _plan_vigente(user.get("plan_code"), user.get("plan_expires_at"))
    limit = int(PLAN_LIMITS[plan]["ai_daily"])
    used = 0
    if DATABASE_URL:
        with obtener_conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ai_requests FROM venbot_usage_daily WHERE external_user_id=%s AND usage_date=CURRENT_DATE", (user["external_user_id"],))
                row=cur.fetchone(); used=int(row[0]) if row else 0
    return {"ok":True,"plan":plan,"used":used,"limit":limit,"remaining":max(0,limit-used)}

@app.get("/api/ai/health")
def ai_health():
    return {"configured": bool(GEMINI_API_KEY or OPENROUTER_API_KEY), "gemini_configured": bool(GEMINI_API_KEY), "openrouter_configured": bool(OPENROUTER_API_KEY), "preferred_models": ["gemini-3.6-flash", "gemini-3.5-flash-lite"], "api": "Interactions API"}


@app.post("/api/ai/chat")
async def ai_chat(payload: AIChatRequest, request: Request):
    user = _require_session_user(request)
    quota = _consume_ai_quota(user)
    if not quota["allowed"]:
        raise HTTPException(status_code=429, detail={"error":"ai_daily_limit", "limit":quota["limit"], "used":quota["used"]})
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
        telegram_app.add_handler(CommandHandler("miid", cmd_miid))
        telegram_app.add_handler(CommandHandler("cuenta", cmd_cuenta))
        telegram_app.add_handler(CommandHandler("credenciales", cmd_credenciales))
        telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("estado", cmd_estado))
        telegram_app.add_handler(CommandHandler("rendimiento", cmd_rendimiento))
        telegram_app.add_handler(CommandHandler("precision", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
        telegram_app.add_handler(CommandHandler("bancos", cmd_bancos))
        telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
        telegram_app.add_handler(CommandHandler("planes", cmd_suscribir))
        telegram_app.add_handler(CommandHandler("pagos", cmd_pagos))
        telegram_app.add_handler(CommandHandler("aprobar", cmd_aprobar))
        telegram_app.add_handler(CommandHandler("rechazar", cmd_rechazar))
        telegram_app.add_handler(MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND), recibir_comprobante))
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
