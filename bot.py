import os
import io
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

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


def _binance_search(trade_type, banco_filtro="GENERAL"):
    # Endpoint P2P vigente usado por clientes públicos de Binance.
    # El endpoint antiguo /bapi/c2c/v1/... en www.binance.com devuelve 404.
    urls = [
        "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
        "https://www.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
    ]

    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "page": 1,
        "rows": 20,
        "tradeType": trade_type,
        "payTypes": [] if banco_filtro == "GENERAL" else [banco_filtro],
        "publisherType": None,
        "merchantCheck": False,
        "countries": [],
        "proMerchantAds": False,
        "shieldMerchantAds": False,
    }

    last_error = None
    for endpoint in urls:
        try:
            r = HTTP.post(
                endpoint,
                json=payload,
                headers={
                    "Accept": "*/*",
                    "Content-Type": "application/json",
                    "Origin": "https://p2p.binance.com",
                    "Referer": "https://p2p.binance.com/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
                },
                timeout=10,
            )
            r.raise_for_status()
            body = r.json()
            data = body.get("data") or []
            if data:
                return data
            last_error = f"respuesta sin anuncios en {endpoint}"
        except Exception as e:
            last_error = str(e)

    if last_error:
        raise RuntimeError(last_error)
    return []


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
def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    filas = obtener_estadisticas_db(banco=banco_filtro)
    total_muestras = len(filas)

    if actual_compra <= 0 or actual_venta <= 0:
        return {
            "pred_compra": 0.0,
            "pred_venta": 0.0,
            "pred_compra_str": "Sin datos",
            "pred_venta_str": "Sin datos",
            "tendencia": "⚠️ SIN DATOS DE MERCADO",
            "piso_str": "Sin datos",
            "techo_str": "Sin datos",
            "muestras": int(total_muestras),
            "liquidez_actual": int(liquidez_actual),
            "estado_comunidad": "⚠️ Sin lectura",
        }

    if total_muestras < 15:
        pred_c = round(float(actual_compra), 2)
        pred_v = round(float(actual_venta), 2)
        tendencia = "🛡️ PROTECCIÓN ESTABLE"
        piso, techo = float(actual_compra), float(actual_venta)
    else:
        compras = np.array([f[0] for f in filas], dtype=float)
        ventas = np.array([f[1] for f in filas], dtype=float)
        fechas = [f[3] for f in filas]
        piso, techo = float(np.min(compras)), float(np.max(ventas))

        window_size = min(total_muestras - 1, 5)
        X, y = [], []
        for i in range(window_size, len(compras)):
            dt_muestra = fechas[i]
            hora_feat = dt_muestra.hour if dt_muestra else 12
            vals = list(compras[i - window_size:i]) + [float(hora_feat)]
            X.append(vals)
            y.append(compras[i])

        X = np.array(X, dtype=float)
        y = np.array(y, dtype=float)
        if len(X):
            model = xgb.XGBRegressor(
                n_estimators=50, max_depth=3, learning_rate=0.1,
                verbosity=0, random_state=42
            )
            model.fit(X, y)
            vector_actual = list(compras[-window_size:]) + [float(datetime.now(VET).hour)]
            pred_c = round(float(model.predict(np.array([vector_actual], dtype=float))[0]), 2)
        else:
            pred_c = round(float(actual_compra), 2)

        spread_promedio = float(np.mean(ventas - compras))
        pred_v = round(pred_c + spread_promedio, 2)
        delta = ((pred_c - actual_compra) / actual_compra) * 100
        if delta > 0.4:
            tendencia = "🟢 TENDENCIA ALCISTA PROTEGIDA"
        elif delta < -0.4:
            tendencia = "🛡️ SOPORTE DE PROTECCIÓN ACTIVO"
        else:
            tendencia = "🛡️ ZONA DE PROTECCIÓN ESTABLE"

    estado = "🟢 Alta Liquidez y Anunciantes Activos" if int(liquidez_actual) >= 12 else "🟡 Liquidez Moderada"
    return {
        "pred_compra": pred_c,
        "pred_venta": pred_v,
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": int(total_muestras),
        "liquidez_actual": int(liquidez_actual),
        "estado_comunidad": estado,
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


# ==========================================
# TELEGRAM
# ==========================================
def obtener_teclado_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Análisis P2P y Protección Quant", callback_data="cmd_prediccion")],
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

    if banco == "GENERAL" and c_real > 0 and v_real > 0:
        tasas = await asyncio.to_thread(obtener_tasas_bcv_oficiales)
        await asyncio.to_thread(guardar_muestra_db, c_real, v_real, liquidez, "GENERAL")
        await asyncio.to_thread(
            guardar_mercado_actual, c_real, v_real, liquidez,
            tasas["usd"], tasas["eur"], tasas["source"]
        )

    datos = await asyncio.to_thread(motor_quant_inteligente, c_real, v_real, liquidez, banco)
    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    spread_actual = v_real - c_real if c_real and v_real else 0
    if spread_actual > 15:
        analisis = "📈 *Spread Amplio:* Alta volatilidad."
    elif 0 < spread_actual < 8:
        analisis = "📉 *Spread Estrecho:* Mercado comprimido."
    else:
        analisis = "⚖️ *Spread Estable:* Liquidez normal."

    texto = (
        f"🦜 *VENBOT PREDICCIONES // TENDENCIA P2P*\n"
        f"🏦 *Filtro Banco:* `{banco}`\n"
        f"──────────────────────────────\n"
        f"⏱ *Sincronización:* `{hora_actual}` ➔ `{hora_objetivo}`\n\n"
        f"📊 *PRECIOS VWAP ACTUALES*\n"
        f"• Comprar USDT: `{c_real:.2f} Bs`\n"
        f"• Vender USDT: `{v_real:.2f} Bs`\n"
        f"• Spread: `{spread_actual:.2f} Bs`\n\n"
        f"🔍 *DIAGNÓSTICO DE MERCADO*\n"
        f"• {analisis}\n"
        f"• Liquidez: `{datos['estado_comunidad']}`\n"
        f"• Muestras: `{datos['muestras']}`\n"
        f"• Canal: `{datos['piso_str']}` / `{datos['techo_str']}`\n\n"
        f"🔮 *PROYECCIÓN CUÁNTICA (7H)*\n"
        f"• Compra estimada: `{datos['pred_compra_str']}`\n"
        f"• Venta estimada: `{datos['pred_venta_str']}`\n"
        f"• Estado: `{datos['tendencia']}`"
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
        [InlineKeyboardButton("BBVA", callback_data="banco_BBVA"), InlineKeyboardButton("Mercantil", callback_data="banco_MERCANTIL")],
        [InlineKeyboardButton("General", callback_data="banco_GENERAL")],
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
            for banco in ("BBVA", "MERCANTIL"):
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

    # Reducir carga del navegador en 30D.
    max_points = 800
    if len(puntos) > max_points:
        step = max(1, len(puntos) // max_points)
        puntos = puntos[::step]
        if filas:
            ultimo = filas[-1]
            last_point = {
                "compra": round(float(ultimo[0]), 2),
                "venta": round(float(ultimo[1]), 2),
                "liquidez": int(ultimo[2] or 0),
                "timestamp": (
                    VET.localize(ultimo[3]).isoformat()
                    if ultimo[3].tzinfo is None
                    else ultimo[3].astimezone(VET).isoformat()
                ),
            }
            if not puntos or puntos[-1]["timestamp"] != last_point["timestamp"]:
                puntos.append(last_point)

    return {"ok": True, "period": period, "count": len(puntos), "data": puntos}


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
