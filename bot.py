import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

VET = timezone(timedelta(hours=-4))
DB_FILE = "p2p_data.db"

# ==========================================
# BASE DE DATOS LOCAL
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            compra REAL,
            venta REAL
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def guardar_muestra_db(compra, venta):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        hora_str = datetime.now(VET).isoformat()
        cursor.execute("INSERT INTO historial (timestamp, compra, venta) VALUES (?, ?, ?)", (hora_str, compra, venta))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error guardando DB: {e}")

def obtener_estadisticas_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT compra, venta FROM historial ORDER BY id ASC")
        filas = cursor.fetchall()
        conn.close()
        return filas
    except Exception as e:
        print(f"Error leyendo DB: {e}")
        return []

# ==========================================
# LECTURA P2P BINANCE (CORREGIDA)
# ==========================================
def get_p2p_rates():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    # Filtro interno de bancos (Provincial/BBVA, Mercantil, BNC) sin mostrarlos
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    # En Binance API:
    # "SELL" muestra anuncios para COMPRAR USDT (Precio mas bajo)
    # "BUY" muestra anuncios para VENDER USDT (Precio mas alto)
    
    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000",
        "payTypes": bancos_filtro
    }

    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000",
        "payTypes": bancos_filtro
    }

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=8).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=8).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if not data_c or not data_v:
            return None, None, None, None

        # Extraer precios de las listas
        precios_compra = [float(item["adv"]["price"]) for item in data_c]
        precios_venta = [float(item["adv"]["price"]) for item in data_v]

        # Para comprar (10k), tomamos el menor valor disponible
        tasa_compra = min(precios_compra)
        # Para vender (300k), tomamos el mayor valor disponible
        tasa_venta = max(precios_venta)

        # Asegurar margen positivo coherente
        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = min(precios_compra[0], precios_venta[0]), max(precios_compra[0], precios_venta[0])

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2) if tasa_compra > 0 else 0.0

        guardar_muestra_db(tasa_compra, tasa_venta)
        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return None, None, None, None

# ==========================================
# CÁLCULOS IA Y BASE DE DATOS
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras <= 1:
        piso = actual_compra
        techo = actual_venta
        pred_c = actual_compra * 0.999
        pred_v = actual_venta * 1.001
        tendencia = "↔️ ESTABLE / LATERAL"
    else:
        compras = [f[0] for f in filas]
        ventas = [f[1] for f in filas]

        piso = min(compras)
        techo = max(ventas)

        media_c = sum(compras) / total_muestras
        media_v = sum(ventas) / total_muestras

        pred_c = (actual_compra * 0.70) + (media_c * 0.30)
        pred_v = (actual_venta * 0.70) + (media_v * 0.30)

        diff = compras[-1] - compras[0]
        if diff > 0.20:
            tendencia = "🚀 ALCISTA"
        elif diff < -0.20:
            tendencia = "🔻 BAJISTA"
        else:
            tendencia = "↔️ ESTABLE / LATERAL"

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": total_muestras
    }

# ==========================================
# COMANDO TELEGRAM
# ==========================================
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, pct = get_p2p_rates()
    if not compra:
        await update.message.reply_text("❌ Error al consultar la API de Binance.")
        return

    pred = motor_quant_inteligente(compra, venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"<b>VENBOT PREDICCIONES</b>\n"
        f"⏰ {hora_ve} | BLOQUE 4\n\n"
        f"🟢 <b>COMPRA (10k):</b> {compra:.2f} Bs\n"
        f"🔴 <b>VENTA (300k):</b> {venta:.2f} Bs\n"
        f"⚡ <b>MARGEN:</b> {spread:.2f} Bs ({pct:.2f}%)\n"
        f"──────────────────\n"
        f"🔮 <b>PROYECCIÓN +7H (IA QUANT)</b>\n"
        f"• Recompra Esperada: <b>{pred['pred_compra_str']}</b>\n"
        f"• Venta Esperada: <b>{pred['pred_venta_str']}</b>\n"
        f"• Dirección: <b>{pred['tendencia']}</b>\n"
        f"──────────────────\n"
        f"📊 Piso: <b>{pred['piso_str']}</b> | Techo: <b>{pred['techo_str']}</b>\n"
        f"🧠 Base de Datos: <b>{pred['muestras']} Muestras</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

# ==========================================
# SERVIDOR FASTAPI
# ==========================================
telegram_app = None

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    global telegram_app
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    if token:
        telegram_app = Application.builder().token(token).build()
        telegram_app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        await telegram_app.initialize()
        await telegram_app.start()
        await telegram_app.updater.start_polling()
    yield
    if telegram_app:
        await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/actual")
def get_actual():
    compra, venta, spread, pct = get_p2p_rates()
    if not compra:
        return {"error": "Sin datos"}
    pred = motor_quant_inteligente(compra, venta)
    return {"compra": compra, "venta": venta, "spread": spread, "pct_bruto": pct, "pred": pred}

@app.get("/api/historial")
def get_historial():
    filas = obtener_estadisticas_db()
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    return {"compras": compras, "ventas": ventas, "total": len(filas)}
