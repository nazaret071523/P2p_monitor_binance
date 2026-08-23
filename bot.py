import os
import json
import time
import math
import urllib.request
import threading
import psycopg2
import sqlite3
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))
VET = timezone(timedelta(hours=-4))

BANCOS_OBJETIVO = [
    "BankOfVenezuela",
    "Mercantil",
    "BBVAProvincial",
    "Banesco",
    "BNC",
    "Bancamiga"
]

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Venbot Quant Engine Active")

def run_web_server():
    try:
        server = HTTPServer(('0.0.0.0', PORT), SimpleHTTPRequestHandler)
        server.serve_forever()
    except Exception as e:
        print(f"Error Web Server: {e}")

# Base de Datos
def get_db_connection():
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception:
            pass
    return sqlite3.connect("p2p_data.db")

def init_db():
    try:
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
    except Exception as e:
        print(f"Error Init DB: {e}")

def guardar_lectura(compra, venta, spread):
    try:
        now_ve = datetime.now(VET)
        conn = get_db_connection()
        cursor = conn.cursor()
        q = "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (%s, %s, %s, %s, %s)" if DATABASE_URL else "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (?, ?, ?, ?, ?)"
        cursor.execute(q, (now_ve.timestamp(), now_ve.strftime("%Y-%m-%d %H:%M:%S"), compra, venta, spread))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error guardando lectura: {e}")

def obtener_historial(limite=20):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        q = "SELECT timestamp, compra, venta, spread FROM lecturas ORDER BY id DESC LIMIT %s" if DATABASE_URL else "SELECT timestamp, compra, venta, spread FROM lecturas ORDER BY id DESC LIMIT ?"
        cursor.execute(q, (limite,))
        rows = cursor.fetchall()
        conn.close()
        return list(reversed(rows))
    except Exception as e:
        print(f"Error obteniendo historial: {e}")
        return []

# Consulta P2P Top 1 No Verificados
def consultar_binance_top1(trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "rows": 30,
        "tradeType": trade_type,
        "transAmount": str(monto),
        "payTypes": BANCOS_OBJETIVO
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=8) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            
            for item in data:
                adv = item.get('adv', {})
                advertiser = item.get('advertiser', {})
                
                is_promoted = adv.get('isPromoted', False)
                user_type = advertiser.get('userType')
                
                if user_type == "user" and not is_promoted:
                    return round(float(adv['price']), 2)
                    
    except Exception as e:
        print(f"Error Binance API ({trade_type}): {e}")
    return None

def get_p2p_rates():
    tasa_recompra = consultar_binance_top1("SELL", "10000")
    tasa_venta = consultar_binance_top1("BUY", "300000")
    
    if not tasa_recompra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)
    return tasa_recompra, tasa_venta, spread, pct_bruto

# Motor Cuantitativo Ajustado (Filtra ruido e historia corrupta)
def motor_quant_top1_7h(actual_compra, actual_venta):
    historial = obtener_historial(15)
    
    if len(historial) < 3:
        return {
            "pred_compra": f"{actual_compra:.2f} Bs",
            "pred_venta": f"{actual_venta:.2f} Bs",
            "brecha_esperada": f"{round(actual_venta - actual_compra, 2):.2f} Bs",
            "tendencia": "↔️ ESTABLE / LATERAL",
            "piso": f"{actual_compra:.2f} Bs", 
            "techo": f"{actual_venta:.2f} Bs", 
            "volatilidad": "🛡️ BAJA", 
            "muestras": len(historial)
        }
    
    compras = [h[1] for h in historial if abs(h[1] - actual_compra) < 15]
    ventas = [h[2] for h in historial if abs(h[2] - actual_venta) < 15]

    if not compras: compras = [actual_compra]
    if not ventas: ventas = [actual_venta]

    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)

    # Suavizado adaptativo de corto plazo
    def calc_projection(series, actual):
        alpha = 0.4
        smooth = series[0]
        for val in series:
            smooth = alpha * val + (1 - alpha) * smooth
        momentum = (actual - smooth) * 0.5
        return round(actual + momentum, 2)

    pred_c = calc_projection(compras, actual_compra)
    pred_v = calc_projection(ventas, actual_venta)
    
    if pred_v <= pred_c:
        pred_v = round(pred_c + 0.50, 2)

    brecha = round(pred_v - pred_c, 2)
    diff = pred_c - actual_compra

    if diff > 0.30:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif diff > 0.05:
        tendencia = "📈 ALCISTA (Moderada)"
    elif diff < -0.30:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif diff < -0.05:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    return {
        "pred_compra": f"{pred_c:.2f} Bs",
        "pred_venta": f"{pred_v:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso:.2f} Bs",
        "techo": f"{techo:.2f} Bs",
        "volatilidad": "🛡️ BAJA",
        "muestras": len(historial)
    }

def background_monitor():
    while True:
        try:
            c, v, sp, pct = get_p2p_rates()
            if c and v:
                guardar_lectura(c, v, sp)
        except Exception as e:
            print(f"Error monitor de fondo: {e}")
        time.sleep(180)

# Telegram Handler
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error temporal consultando la API de Binance P2P.")
        return

    pred = motor_quant_top1_7h(tasa_compra, tasa_venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P TOP 1 (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_ve}\n"
        f"🎯 **Filtros:** Recompra (10K VES) | Venta (300K VES)\n\n"
        f"🟢 **Precio Real Recompra:** {tasa_compra:.2f} Bs\n"
        f"🔴 **Precio Real Venta:** {tasa_venta:.2f} Bs\n"
        f"⚡ **Spread Bruto:** {spread:.2f} Bs ({pct_bruto:.2f}%)\n\n"
        f"🔮 **Proyección Recompra (7h):** {pred['pred_compra']}\n"
        f"🔮 **Proyección Venta (7h):** {pred['pred_venta']}\n"
        f"📐 **Brecha Esperada (7h):** {pred['brecha_esperada']}\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n"
        f"🌊 **Volatilidad:** {pred['volatilidad']}\n\n"
        f"🛡️ **Soporte (Piso 24h):** {pred['piso']}\n"
        f"🏰 **Resistencia (Techo 24h):** {pred['techo']}\n\n"
        f"🧠 **Lecturas Acumuladas:** {pred['muestras']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()
    
    threading.Thread(target=run_web_server, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if TELEGRAM_TOKEN:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        app.add_handler(CommandHandler("p2p", prediccion_cmd))
        print("Bot en vivo e iniciado...")
        app.run_polling(drop_pending_updates=True)
