import os
import asyncio
import sqlite3
import requests
import statistics
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VET = timezone(timedelta(hours=-4))

# --- BASE DE DATOS SQLITE ---
def init_db():
    db_path = os.path.join(BASE_DIR, "market_data.db")
    conn = sqlite3.connect(db_path)
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
    db_path = os.path.join(BASE_DIR, "market_data.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fecha_actual = datetime.now(VET)
    cursor.execute(
        "INSERT INTO historial (fecha, compra, venta, spread) VALUES (?, ?, ?, ?)",
        (fecha_actual, compra, venta, spread)
    )
    conn.commit()
    conn.close()

def obtener_historial(limite=2000):
    db_path = os.path.join(BASE_DIR, "market_data.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT fecha, compra, venta, spread FROM historial ORDER BY id DESC LIMIT ?", (limite,))
    registros = cursor.fetchall()
    conn.close()
    
    datos = []
    for r in registros:
        try:
            f = datetime.fromisoformat(r[0])
        except Exception:
            f = datetime.now(VET)
        datos.append((f, r[1], r[2], r[3]))
    return datos

# --- CONSULTA BINANCE P2P FILTRADA (PROVINCIAL / MERCANTIL / BNC) ---
def get_p2p_rates():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    # Métodos de pago solicitados: Provincial, Mercantil, BNC
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    # Recompra (Pestaña "Vender" en App Binance)
    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 15, "tradeType": "SELL", "transAmount": "10000",
        "payTypes": bancos_filtro
    }
    
    # Venta (Pestaña "Comprar" en App Binance)
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

        if not precios_c or not precios_v:
            return None, None, None, None

        # Tasa más competitiva de Recompra (Mayor valor)
        tasa_compra = round(max(precios_c), 2)
        
        # Tasa más competitiva de Venta (Menor valor)
        tasa_venta = round(min(precios_v), 2)

        if tasa_venta <= tasa_compra:
            tasa_venta = round(tasa_compra + 7.00, 2)

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2)
        
        guardar_lectura(tasa_compra, tasa_venta, spread)
        return tasa_compra, tasa_venta, spread, pct_bruto

    except Exception as e:
        print(f"Error Binance API: {e}")
    
    return None, None, None, None

# --- MOTOR IA QUANT ---
def motor_quant_inteligente(actual_compra, actual_venta):
    historial = obtener_historial(500)
    
    if len(historial) < 5:
        pred_c = round(actual_compra + 0.15, 2)
        pred_v = round(actual_venta + 0.15, 2)
        return {
            "pred_compra_str": f"{pred_c:.2f} Bs",
            "pred_venta_str": f"{pred_v:.2f} Bs",
            "tendencia": "↔️ ESTABLE / LATERAL",
            "piso_str": f"{actual_compra:.2f} Bs",
            "techo_str": f"{actual_venta:.2f} Bs",
            "muestras": len(historial)
        }

    compras = [h[1] for h in historial]
    ventas = [h[2] for h in historial]

    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)

    def calcular_ema(datos, periodo=20):
        k = 2 / (periodo + 1)
        ema = datos[0]
        for val in datos[1:]:
            ema = (val * k) + (ema * (1 - k))
        return ema

    ema_c = calcular_ema(compras)
    ema_v = calcular_ema(ventas)

    delta_c = (actual_compra - ema_c) * 0.35
    delta_v = (actual_venta - ema_v) * 0.35

    pred_c = round(actual_compra + delta_c, 2)
    pred_v = round(actual_venta + delta_v, 2)

    if (pred_v - pred_c) < 5.00:
        pred_v = round(pred_c + 7.00, 2)

    diff = pred_c - actual_compra

    if diff > 0.30:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif diff > 0.08:
        tendencia = "📈 ALCISTA (Moderada)"
    elif diff < -0.30:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif diff < -0.08:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": len(compras)
    }

# --- HANDLER TELEGRAM (SIN NOMBRES DE BANCOS) ---
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error al obtener datos de Binance P2P.")
        return

    pred = motor_quant_inteligente(tasa_compra, tasa_venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"**VENBOT PREDICCIONES**\n"
        f"⏰ {hora_ve} | BLOQUE 4\n\n"
        f"🟢 **COMPRA (10k):** `{tasa_compra:.2f} Bs`\n"
        f"🔴 **VENTA (300k):** `{tasa_venta:.2f} Bs`\n"
        f"⚡ **MARGEN:** `{spread:.2f} Bs` ({pct_bruto:.2f}%)\n\n"
        f"──────────────────\n"
        f"🔮 **PROYECCIÓN +7H (IA QUANT)**\n"
        f"• **Recompra Esperada:** `{pred['pred_compra_str']}`\n"
        f"• **Venta Esperada:** `{pred['pred_venta_str']}`\n"
        f"• **Dirección:** {pred['tendencia']}\n"
        f"──────────────────\n\n"
        f"📊 **Piso:** {pred['piso_str']} | **Techo:** {pred['techo_str']}\n"
        f"🧠 **Base de Datos:** {pred['muestras']} Muestras"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# --- SERVIDOR FASTAPI CON CORS ---
telegram_app = None

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    global telegram_app
    token_actual = os.getenv("TELEGRAM_TOKEN", "").strip()
    
    if token_actual and token_actual != "TU_TELEGRAM_TOKEN_AQUI":
        try:
            telegram_app = Application.builder().token(token_actual).build()
            telegram_app.add_handler(CommandHandler("prediccion", prediccion_cmd))
            await telegram_app.initialize()
            await telegram_app.start()
            await telegram_app.updater.start_polling()
            print("✅ Bot ejecutándose correctamente.")
        except Exception as e:
            print(f"❌ Error al iniciar Telegram Bot: {e}")
    yield
    if telegram_app:
        try:
            await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            print(f"Error al detener: {e}")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
def get_web():
    html_path = os.path.join(BASE_DIR, "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>Servidor Venbot Activo</h1>"

@app.get("/api/actual")
def get_actual_api():
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    if not tasa_compra:
        return {"error": "Sin datos"}
    pred = motor_quant_inteligente(tasa_compra, tasa_venta)
    return {
        "compra": tasa_compra,
        "venta": tasa_venta,
        "spread": spread,
        "pct_bruto": pct_bruto,
        "pred": pred
    }

@app.get("/api/historial")
def get_historial_api(rango: str = "1d"):
    limite = 480 if rango == "1d" else (3360 if rango == "7d" else 14400)
    data = obtener_historial(limite)
    data.reverse()

    paso = 1 if rango == "1d" else (7 if rango == "7d" else 30)
    data_filtrada = data[::paso]

    labels, compras, ventas = [], [], []

    for item in data_filtrada:
        fecha_obj = item[0]
        label = fecha_obj.strftime("%H:%M") if rango == "1d" else fecha_obj.strftime("%d/%m %H:%M")
        labels.append(label)
        compras.append(item[1])
        ventas.append(item[2])

    return {"labels": labels, "compras": compras, "ventas": ventas}
