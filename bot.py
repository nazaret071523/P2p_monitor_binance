import os
import requests
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

VET = timezone(timedelta(hours=-4))

# ==========================================
# MEMORIA PERSISTENTE DE MUESTRAS (EN MEMORIA)
# ==========================================
MEMORIA_HISTORIAL = []  # Estructura: [{'fecha': ..., 'compra': ..., 'venta': ...}]

def guardar_muestra(compra, venta):
    global MEMORIA_HISTORIAL
    fecha_actual = datetime.now(VET)
    MEMORIA_HISTORIAL.append({
        'fecha': fecha_actual,
        'compra': compra,
        'venta': venta
    })
    # Mantenemos un máximo de 500 muestras en memoria
    if len(MEMORIA_HISTORIAL) > 500:
        MEMORIA_HISTORIAL.pop(0)

# ==========================================
# OBTECIÓN DE PRECIOS REALES BINANCE P2P
# ==========================================
def get_p2p_rates():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    # Filtro interno de bancos (BBVA=Provincial, Mercantil, BNC)
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    # Consulta Compra USDT (Bloque 10,000 Bs)
    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "10000",
        "payTypes": bancos_filtro
    }

    # Consulta Venta USDT (Bloque 300,000 Bs)
    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "300000",
        "payTypes": bancos_filtro
    }

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=8).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=8).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if not data_c or not data_v:
            return None, None, None, None

        # Primeras ofertas del libro de ordenes
        tasa_compra = float(data_c[0]["adv"]["price"])
        tasa_venta = float(data_v[0]["adv"]["price"])

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2) if tasa_compra > 0 else 0.0

        guardar_muestra(tasa_compra, tasa_venta)
        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return None, None, None, None

# ==========================================
# MOTOR IA QUANT E HISTORIAL ACCUMULADO
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    muestras_totales = len(MEMORIA_HISTORIAL)

    if muestras_totales <= 1:
        # Estimación inicial cuando recién inicia
        piso = actual_compra * 0.995
        techo = actual_venta * 1.005
        pred_c = actual_compra * 0.998
        pred_v = actual_venta * 1.002
        tendencia = "↔️ ESTABLE / LATERAL"
    else:
        compras = [m['compra'] for m in MEMORIA_HISTORIAL]
        ventas = [m['venta'] for m in MEMORIA_HISTORIAL]

        piso = min(compras)
        techo = max(ventas)

        media_c = sum(compras) / muestras_totales
        media_v = sum(ventas) / muestras_totales

        # Algoritmo de ponderación (65% precio actual, 35% tendencia histórica)
        pred_c = (actual_compra * 0.65) + (media_c * 0.35)
        pred_v = (actual_venta * 0.65) + (media_v * 0.35)

        diff = compras[-1] - compras[0]
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
        "muestras": muestras_totales
    }

# ==========================================
# COMANDO TELEGRAM
# ==========================================
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, pct = get_p2p_rates()
    if not compra:
        await update.message.reply_text("❌ Error al obtener cotizaciones P2P.")
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
def get_historial(rango: str = "1d"):
    limite = 24 if rango == "1d" else (168 if rango == "7d" else 500)
    muestras = MEMORIA_HISTORIAL[-limite:]
    
    labels = [m['fecha'].strftime("%H:%M") for m in muestras]
    compras = [m['compra'] for m in muestras]
    ventas = [m['venta'] for m in muestras]

    return {"labels": labels, "compras": compras, "ventas": ventas}
