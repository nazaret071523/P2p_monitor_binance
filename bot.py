import os
import json
import time
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

# Servidor HTTP para Render
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Venbot Active")

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

def consultar_binance_native(trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "publisherType": "user",
        "page": 1,
        "rows": 5,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=6) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            if data:
                prices = [float(x['adv']['price']) for x in data[:3]]
                return round(sum(prices) / len(prices), 2)
    except Exception as e:
        print(f"Error consultando Binance Native: {e}")
    return None

def get_p2p_rates():
    # Tu anuncio de COMPRA (Recompra de USDT con filtro 10K VES) -> tradeType SELL
    tasa_recompra = consultar_binance_native("SELL", "10000")
    # Tu anuncio de VENTA (Liquidar USDT con filtro 300K VES) -> tradeType BUY
    tasa_venta = consultar_binance_native("BUY", "300000")
    
    if not tasa_recompra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)
    return tasa_recompra, tasa_venta, spread, pct_bruto

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
    
    compras = [h[1] for h in historial]
    ventas = [h[2] for h in historial]
    
    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)
    
    # Suavizado de tendencia lineal simple
    delta_c = compras[-1] - compras[0]
    delta_v = ventas[-1] - ventas[0]
    
    rate_c = delta_c / len(compras)
    rate_v = delta_v / len(ventas)
    
    # Proyección a 7 horas (20 lecturas por hora)
    pred_c = round(compras[-1] + (rate_c * 140), 2)
    pred_v = round(ventas[-1] + (rate_v * 140), 2)
    
    spread_mediano = (sum(ventas) - sum(compras)) / n
    if pred_v <= pred_c:
        pred_v = round(pred_c + spread_mediano, 2)
        
    brecha = round(pred_v - pred_c, 2)
    
    avg_rate = (rate_c + rate_v) / 2
    if avg_rate > 0.02:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif avg_rate > 0.005:
        tendencia = "📈 ALCISTA (Moderada)"
    elif avg_rate < -0.02:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif avg_rate < -0.005:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE"

    return {
        "pred_compra": f"{pred_c:.2f} Bs",
        "pred_venta": f"{pred_v:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso:.2f} Bs",
        "techo": f"{techo:.2f} Bs",
        "muestras": n
    }

def background_monitor():
    while True:
        try:
            c, v, sp, pct = get_p2p_rates()
            if c and v:
                guardar_lectura(c, v, sp)
        except Exception as e:
            print(f"Error monitor fondo: {e}")
        time.sleep(180)

async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    pred = motor_prediccion_7h()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error temporal conectando con Binance P2P. Intenta de nuevo.")
        return

    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P REAL (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_ve}\n"
        f"🎯 **Filtros:** Recompra (10K VES) | Venta (300K VES)\n\n"
        f"🟢 **Precio Real Recompra:** {tasa_compra:.2f} Bs\n"
        f"🔴 **Precio Real Venta:** {tasa_venta:.2f} Bs\n"
        f"⚡ **Spread Bruto:** {spread:.2f} Bs ({pct_bruto:.2f}%)\n\n"
        f"🔮 **Proyección Recompra (7h):** {pred['pred_compra']}\n"
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
    
    threading.Thread(target=run_web_server, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    
    if TELEGRAM_TOKEN:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        app.add_handler(CommandHandler("p2p", prediccion_cmd))
        print("Bot encendido correctamente...")
        app.run_polling(drop_pending_updates=True)
