import os
import time
import requests
import threading
import psycopg2
import sqlite3
import numpy as np
import asyncio
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))
VET = timezone(timedelta(hours=-4))

SUSCRIPTORES = set()
ULTIMA_TENDENCIA = "NEUTRA"

# Servidor HTTP para cumplir el Health Check de Render
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Venbot P2P Monitor is Live!")

def run_web_server():
    server = HTTPServer(('0.0.0.0', PORT), SimpleHTTPRequestHandler)
    server.serve_forever()

def get_db_connection():
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception:
            pass
    return sqlite3.connect("p2p_data.db")

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS lecturas (
        id SERIAL PRIMARY KEY,
        timestamp DOUBLE PRECISION,
        fecha_hora TEXT,
        compra REAL,
        venta REAL,
        spread REAL
    );
    """ if DATABASE_URL else """
    CREATE TABLE IF NOT EXISTS lecturas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        fecha_hora TEXT,
        compra REAL,
        venta REAL,
        spread REAL
    );
    """
    cursor.execute(query)
    conn.commit()
    conn.close()

def guardar_lectura(compra, venta, spread):
    now_ve = datetime.now(VET)
    conn = get_db_connection()
    cursor = conn.cursor()
    q = "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (%s, %s, %s, %s, %s)" if DATABASE_URL else "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (?, ?, ?, ?, ?)"
    cursor.execute(q, (now_ve.timestamp(), now_ve.strftime("%Y-%m-%d %H:%M:%S"), compra, venta, spread))
    conn.commit()
    conn.close()

def obtener_historial(horas=24):
    conn = get_db_connection()
    cursor = conn.cursor()
    ts_limite = (datetime.now(VET) - timedelta(hours=horas)).timestamp()
    q = "SELECT timestamp, compra, venta, spread FROM lecturas WHERE timestamp >= %s ORDER BY timestamp ASC" if DATABASE_URL else "SELECT timestamp, compra, venta, spread FROM lecturas WHERE timestamp >= ? ORDER BY timestamp ASC"
    cursor.execute(q, (ts_limite,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def obtener_tasa_real_binance(trade_type, monto="10000"):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "publisherType": "user",
        "page": 1,
        "rows": 10,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }
    
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10).json()
        data = r.get('data', [])
        if data:
            return float(data[0]['adv']['price'])
        return None
    except Exception as e:
        print(f"Error Binance P2P ({trade_type}): {e}")
        return None

def get_p2p_rates():
    tasa_compra = obtener_tasa_real_binance("BUY", "10000")
    tasa_venta = obtener_tasa_real_binance("SELL", "10000")
    
    if not tasa_compra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_compra, 2)
    ganancia_neta = round(((tasa_venta * 0.9975) - (tasa_compra * 1.0025)) / tasa_compra * 100, 2)
    
    return tasa_compra, tasa_venta, spread, ganancia_neta

def motor_prediccion_7h():
    historial = obtener_historial(24)
    n = len(historial)
    
    if n < 5:
        return {
            "pred_compra": "Recolectando datos...",
            "pred_venta": "Recolectando datos...",
            "brecha_esperada": "N/A",
            "tendencia": "↔️ NEUTRA (Iniciando)",
            "piso": "N/A", "techo": "N/A", "muestras": n
        }
    
    compras = np.array([h[1] for h in historial])
    ventas = np.array([h[2] for h in historial])
    timestamps = np.array([h[0] for h in historial])
    
    piso_soporte = round(np.min(compras), 2)
    techo_resistencia = round(np.max(ventas), 2)
    
    weights = np.exp(np.linspace(-1., 0., n))
    weights /= weights.sum()
    
    ewma_compra = np.sum(compras * weights)
    ewma_venta = np.sum(ventas * weights)
    
    dx = timestamps[-1] - timestamps[0]
    if dx > 0:
        slope_c = (compras[-1] - ewma_compra) / (dx / 3600)
        slope_v = (ventas[-1] - ewma_venta) / (dx / 3600)
    else:
        slope_c, slope_v = 0, 0
        
    pred_c_7h = round(compras[-1] + (slope_c * 7), 2)
    pred_v_7h = round(ventas[-1] + (slope_v * 7), 2)
    
    if pred_v_7h <= pred_c_7h:
        pred_v_7h = round(pred_c_7h + (ventas[-1] - compras[-1]), 2)
        
    brecha = round(pred_v_7h - pred_c_7h, 2)
    
    var_7h = (slope_c + slope_v) / 2 * 7
    if var_7h > 0.8:
        tendencia = "📈 ALCISTA (Fuerte)"
    elif var_7h > 0.2:
        tendencia = "📈 ALCISTA (Moderada)"
    elif var_7h < -0.8:
        tendencia = "📉 BAJISTA (Fuerte)"
    elif var_7h < -0.2:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE"

    return {
        "pred_compra": f"{pred_c_7h:.2f} Bs",
        "pred_venta": f"{pred_v_7h:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso_soporte:.2f} Bs",
        "techo": f"{techo_resistencia:.2f} Bs",
        "muestras": n
    }

def background_monitor():
    while True:
        try:
            c, v, sp, gn = get_p2p_rates()
            if c and v:
                guardar_lectura(c, v, sp)
        except Exception as e:
            print(f"Error en monitor de fondo: {e}")
        time.sleep(180)

async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    SUSCRIPTORES.add(update.effective_chat.id)
    compra, venta, spread, ganancia = get_p2p_rates()
    pred = motor_prediccion_7h()
    
    if not compra:
        await update.message.reply_text("❌ Error consultando la API de Binance P2P.")
        return

    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P REAL (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_ve}\n"
        f"🎯 **Filtro:** 10K VES | **Comisión:** 0.50%\n\n"
        f"🟢 **Precio Real Compra:** {compra:.2f} Bs\n"
        f"🔴 **Precio Real Recompra/Venta:** {venta:.2f} Bs\n"
        f"⚡ **Spread Actual:** {spread:.2f} Bs | **Ganancia Neta:** {ganancia:.2f}%\n\n"
        f"🔮 **Proyección Compra (7h):** {pred['pred_compra']}\n"
        f"🔮 **Proyección Venta (7h):** {pred['pred_venta']}\n"
        f"📐 **Brecha Esperada (7h):** {pred['brecha_esperada']}\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n\n"
        f"🛡️ **Soporte (Piso 24h):** {pred['piso']}\n"
        f"🏰 **Resistencia (Techo 24h):** {pred['techo']}\n\n"
        f"🧠 **Lecturas Acumuladas:** {pred['muestras']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()
    
    # Iniciar Servidor HTTP Web para Render
    threading.Thread(target=run_web_server, daemon=True).start()
    
    # Iniciar Monitor de Fondo
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if TELEGRAM_TOKEN:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        print("Bot iniciado con éxito...")
        app.run_polling(drop_pending_updates=True)
