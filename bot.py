import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VET = timezone(timedelta(hours=-4))
DB_PATH = os.path.join(BASE_DIR, "market_data.db")

# --- BASE DE DATOS ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TIMESTAMP,
            compra REAL,
            venta REAL,
            spread REAL
        )
    """)
    conn.commit()
    conn.close()

init_db()

def guardar_lectura(compra, venta, spread):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    fecha_actual = datetime.now(VET)
    cursor.execute(
        "INSERT INTO historial (fecha, compra, venta, spread) VALUES (?, ?, ?, ?)",
        (fecha_actual, compra, venta, spread)
    )
    conn.commit()
    conn.close()

def obtener_historial(limite=200):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT fecha, compra, venta, spread FROM historial ORDER BY id DESC LIMIT ?", (limite,))
    registros = cursor.fetchall()
    conn.close()
    return registros

# --- CONSULTA BINANCE P2P ---
def get_p2p_rates():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 15, "tradeType": "SELL", "transAmount": "10000",
        "payTypes": bancos_filtro
    }
    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 15, "tradeType": "BUY", "transAmount": "300000",
        "payTypes": bancos_filtro
    }

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=8).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=8).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if not data_c or not data_v:
            return None, None, None, None

        precios_c = [float(adv["adv"]["price"]) for adv in data_c if adv.get("adv")]
        precios_v = [float(adv["adv"]["price"]) for adv in data_v if adv.get("adv")]

        tasa_compra = round(max(precios_c), 2)
        tasa_venta = round(min(precios_v), 2)

        if tasa_venta <= tasa_compra:
            tasa_venta = round(tasa_compra + 7.00, 2)

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2)

        guardar_lectura(tasa_compra, tasa_venta, spread)
        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return None, None, None, None

# --- MOTOR IA ---
def motor_quant_inteligente(actual_compra, actual_venta):
    historial = obtener_historial(200)
    muestras = len(historial)

    if muestras < 2:
        return {
            "pred_compra_str": f"{actual_compra * 0.998:.2f} Bs",
            "pred_venta_str": f"{actual_venta * 1.002:.2f} Bs",
            "tendencia": "↔️ ESTABLE / LATERAL",
            "piso_str": f"{actual_compra * 0.995:.2f} Bs",
            "techo_str": f"{actual_venta * 1.005:.2f} Bs",
            "muestras": muestras
        }

    compras = [h[1] for h in historial]
    ventas = [h[2] for h in historial]

    piso = min(compras)
    techo = max(ventas)

    media_c = sum(compras) / muestras
    media_v = sum(ventas) / muestras

    pred_c = round((actual_compra * 0.65) + (media_c * 0.35), 2)
    pred_v = round((actual_venta * 0.65) + (media_v * 0.35), 2)

    diff = compras[0] - compras[-1]
    if diff > 0.30:
        tendencia = "🚀 ALCISTA"
    elif diff < -0.30:
        tendencia = "🔻 BAJISTA"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": muestras
    }

# --- TELEGRAM BOT ---
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, pct = get_p2p_rates()
    if not compra:
        await update.message.reply_text("❌ Error al obtener datos de Binance P2P.")
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
        f"──────────────────\n\n"
        f"📊 Piso: <b>{pred['piso_str']}</b> | Techo: <b>{pred['techo_str']}</b>\n"
        f"🧠 Base de Datos: <b>{pred['muestras']} Muestras</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

# --- FASTAPI APPS ---
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
def get_historial(rango: str = "1d"):
    limite = 24 if rango == "1d" else (168 if rango == "7d" else 720)
    data = obtener_historial(limite)
    data.reverse()

    labels = [h[0].split()[1][:5] if " " in str(h[0]) else str(h[0]) for h in data]
    compras = [h[1] for h in data]
    ventas = [h[2] for h in data]

    return {"labels": labels, "compras": compras, "ventas": ventas}
