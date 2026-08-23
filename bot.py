import os
import json
import time
import math
import statistics
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

# Reducido a los 3 bancos de mayor estabilidad y rapidez
BANCOS_OBJETIVO = [
    "Mercantil",
    "BBVAProvincial",
    "BNC"
]

ULTIMA_LECTURA_VALIDA = {
    "compra": None,
    "venta": None,
    "spread": None,
    "pct": None,
    "timestamp": None
}

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

# Consulta a Binance P2P (Mercantil, Provincial, BNC)
def consultar_binance_top3_mediana(trade_type, monto, pay_types=None):
    if pay_types is None:
        pay_types = BANCOS_OBJETIVO

    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "rows": 20,
        "tradeType": trade_type,
        "transAmount": str(monto),
        "payTypes": pay_types
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            
            precios_validos = []
            for item in data:
                adv = item.get('adv', {})
                advertiser = item.get('advertiser', {})
                
                is_promoted = adv.get('isPromoted', False)
                user_type = advertiser.get('userType')
                
                if user_type == "user" and not is_promoted:
                    precios_validos.append(float(adv['price']))
                    if len(precios_validos) == 3:
                        break
            
            if precios_validos:
                return round(statistics.median(precios_validos), 2)
                    
    except Exception as e:
        print(f"Error Binance API ({trade_type}): {e}")
    return None

def get_p2p_rates():
    global ULTIMA_LECTURA_VALIDA

    # Intento 1: Consulta directa con los 3 bancos
    tasa_recompra = consultar_binance_top3_mediana("SELL", "10000", BANCOS_OBJETIVO)
    tasa_venta = consultar_binance_top3_mediana("BUY", "300000", BANCOS_OBJETIVO)
    
    # Intento 2: Consulta sin filtro si falla la especifica
    if not tasa_recompra:
        tasa_recompra = consultar_binance_top3_mediana("SELL", "10000", [])
    if not tasa_venta:
        tasa_venta = consultar_binance_top3_mediana("BUY", "300000", [])

    # Intento 3: Respaldo de memoria/BD
    if not tasa_recompra or not tasa_venta:
        historial = obtener_historial(1)
        if historial:
            tasa_recompra = historial[0][1]
            tasa_venta = historial[0][2]
        elif ULTIMA_LECTURA_VALIDA["compra"]:
            tasa_recompra = ULTIMA_LECTURA_VALIDA["compra"]
            tasa_venta = ULTIMA_LECTURA_VALIDA["venta"]
        else:
            return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)

    ULTIMA_LECTURA_VALIDA = {
        "compra": tasa_recompra,
        "venta": tasa_venta,
        "spread": spread,
        "pct": pct_bruto,
        "timestamp": time.time()
    }

    return tasa_recompra, tasa_venta, spread, pct_bruto

# Motor de Inteligencia Cuantitativa
def motor_quant_inteligente(actual_compra, actual_venta):
    historial = obtener_historial(15)
    
    compras_raw = [h[1] for h in historial]
    ventas_raw = [h[2] for h in historial]
    
    def limpiar_datos(series, actual):
        if not series:
            return [actual]
        limpios = [x for x in series if abs(x - actual) <= 10]
        return limpios if limpios else [actual]

    compras = limpiar_datos(compras_raw, actual_compra)
    ventas = limpiar_datos(ventas_raw, actual_venta)

    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)

    def proyectar(series, actual):
        alpha = 0.35
        smooth = series[0]
        for v in series:
            smooth = alpha * v + (1 - alpha) * smooth
        delta = actual - smooth
        return round(actual + (delta * 0.4), 2)

    pred_c = proyectar(compras, actual_compra)
    pred_v = proyectar(ventas, actual_venta)

    if pred_v <= pred_c:
        pred_v = round(pred_c + 0.50, 2)

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
        "pred_compra": f"{pred_c:.2f} Bs",
        "pred_venta": f"{pred_v:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso:.2f} Bs",
        "techo": f"{techo:.2f} Bs",
        "volatilidad": "🛡️ BAJA",
        "muestras": len(compras)
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

# Handler Telegram
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error consultando la API de Binance P2P.")
        return

    pred = motor_quant_inteligente(tasa_compra, tasa_venta)
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
        f"🧠 **Lecturas Limpias:** {pred['muestras']}"
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
