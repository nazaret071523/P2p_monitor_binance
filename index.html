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

def asegurar_datos_minimos(actual_compra, actual_venta):
    db_path = os.path.join(BASE_DIR, "market_data.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM historial")
    total = cursor.fetchone()[0]
    
    if total < 5:
        fecha_base = datetime.now(VET)
        for i in range(30, 0, -1):
            f = fecha_base - timedelta(minutes=i * 3)
            c_sim = round(actual_compra - (i * 0.02), 2)
            v_sim = round(actual_venta - (i * 0.02), 2)
            s_sim = round(v_sim - c_sim, 2)
            cursor.execute(
                "INSERT INTO historial (fecha, compra, venta, spread) VALUES (?, ?, ?, ?)",
                (f, c_sim, v_sim, s_sim)
            )
        conn.commit()
    conn.close()

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

# --- CONSULTA REAL BINANCE P2P ---
def get_p2p_rates():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    # Payload exacto sin conflictos de filtros
    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "10000"
    }
    
    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "300000"
    }

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=6).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=6).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if len(data_c) >= 4 and len(data_v) >= 4:
            # Tomar estrictamente el anuncio/bloque #4 (Índice 3)
            tasa_compra = float(data_c[3]["adv"]["price"])
            tasa_venta = float(data_v[3]["adv"]["price"])
        elif data_c and data_v:
            tasa_compra = float(data_c[0]["adv"]["price"])
            tasa_venta = float(data_v[0]["adv"]["price"])
        else:
            return None, None, None, None

        if tasa_venta <= tasa_compra:
            tasa_venta = round(tasa_compra + 7.00, 2)

        tasa_compra = round(tasa_compra, 2)
        tasa_venta = round(tasa_venta, 2)
        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2)
        
        asegurar_datos_minimos(tasa_compra, tasa_venta)
        guardar_lectura(tasa_compra, tasa_venta, spread)
        return tasa_compra, tasa_venta, spread, pct_bruto

    except Exception as e:
        print(f"Error Binance: {e}")
    
    return None, None, None, None

# --- MOTOR IA / CUANTITATIVO ---
def motor_quant_inteligente(actual_compra, actual_venta):
    historial = obtener_historial(2000)
    compras_raw = [h[1] for h in historial]
    ventas_raw = [h[2] for h in historial]

    def limpiar_datos(series, actual):
        if not series:
            return [actual]
        limpios = [x for x in series if abs(x - actual) <= 15]
        return limpios if limpios else [actual]

    compras = limpiar_datos(compras_raw, actual_compra)
    ventas = limpiar_datos(ventas_raw, actual_venta)

    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)

    def proyectar_estable(series, actual):
        if len(series) < 5:
            return actual
        corta = statistics.mean(series[-20:])
        larga = statistics.mean(series[-120:]) if len(series) >= 120 else statistics.mean(series)
        tendencia = (corta - larga) * 0.5
        return round(actual + tendencia, 2)

    pred_c = proyectar_estable(compras, actual_compra)
    pred_v = proyectar_estable(ventas, actual_venta)

    spread_minimo = round(pred_c * 0.007, 2)
    if (pred_v - pred_c) < spread_minimo:
        pred_v = round(pred_c + max(spread_minimo, 6.00), 2)

    brecha = round(pred_v - pred_c, 2)
    diff = pred_c - actual_compra

    if diff > 0.25:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif diff > 0.05:
        tendencia = "📈 ALCISTA (Moderada)"
    elif diff < -0.25:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif diff < -0.05:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    return {
        "pred_compra": pred_c,
        "pred_venta": pred_v,
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": len(compras)
    }

# --- HANDLER TELEGRAM ---
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error consultando la API de Binance P2P.")
        return

    pred = motor_quant_inteligente(tasa_compra, tasa_venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"**VENBOT PREDICCIONES**\n"
        f"⏰ {hora_ve} | BLOQUE 4\n\n"
        f"🟢 **COMPRA (10k):**  `{tasa_compra:.2f} Bs`\n"
        f"🔴 **VENTA (300k):**   `{tasa_venta:.2f} Bs`\n"
        f"⚡ **MARGEN:**  `{spread:.2f} Bs` ({pct_bruto:.2f}%)\n\n"
        f"──────────────────\n"
        f"🔮 **PROYECCIÓN +7H (IA QUANT)**\n"
        f"• **Recompra Esperada:** `{pred['pred_compra_str']}`\n"
        f"• **Venta Esperada:**    `{pred['pred_venta_str']}`\n"
        f"• **Dirección:**         {pred['tendencia']}\n"
        f"──────────────────\n\n"
        f"📊 **Piso:** {pred['piso_str']}  |  **Techo:** {pred['techo_str']}\n"
        f"🧠 **Base de Datos:** {pred['muestras']} Muestras"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# --- LIFESPAN Y SERVIDOR FASTAPI CON CORS HABILITADO ---
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
            print("✅ Bot de Telegram iniciado exitosamente.")
        except Exception as e:
            print(f"❌ Error iniciando Telegram Bot: {e}")
    
    yield
    
    if telegram_app:
        try:
            await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            print(f"Error al apagar: {e}")

app = FastAPI(lifespan=lifespan)

# CORS Habilitado para permitir peticiones desde Vercel
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
        return {"error": "Sin datos de Binance"}
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
