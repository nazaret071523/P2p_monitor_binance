import os
import time
import json
import requests
import threading
import psycopg2
import sqlite3
import numpy as np
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ==========================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ==========================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))

# Zona horaria de Venezuela (UTC-4)
VET = timezone(timedelta(hours=-4))

# ==========================================
# GESTIÓN DE BASE DE DATOS
# ==========================================
def get_db_connection():
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            print(f"⚠️ Error conectando a PostgreSQL ({e}). Usando SQLite local...")
    return sqlite3.connect("p2p_data.db")

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if isinstance(conn, sqlite3.Connection):
        query = """
        CREATE TABLE IF NOT EXISTS lecturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            fecha_hora TEXT,
            compra REAL,
            venta REAL,
            spread REAL,
            ganancia_pct REAL
        );
        """
    else:
        query = """
        CREATE TABLE IF NOT EXISTS lecturas (
            id SERIAL PRIMARY KEY,
            timestamp DOUBLE PRECISION,
            fecha_hora TEXT,
            compra REAL,
            venta REAL,
            spread REAL,
            ganancia_pct REAL
        );
        """

    cursor.execute(query)
    conn.commit()
    conn.close()

def guardar_lectura(compra, venta, spread, ganancia_pct):
    now_ve = datetime.now(VET)
    ts = now_ve.timestamp()
    fecha_str = now_ve.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    if isinstance(conn, sqlite3.Connection):
        cursor.execute("""
            INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread, ganancia_pct)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ts, fecha_str, compra, venta, spread, ganancia_pct))
    else:
        cursor.execute("""
            INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread, ganancia_pct)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ts, fecha_str, compra, venta, spread, ganancia_pct))
        
    conn.commit()
    conn.close()

def obtener_historial_horas(horas=24):
    """Obtiene el historial de las últimas N horas almacenadas en DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    ts_limite = (datetime.now(VET) - timedelta(hours=horas)).timestamp()
    
    if isinstance(conn, sqlite3.Connection):
        cursor.execute("SELECT timestamp, fecha_hora, compra, venta, spread, ganancia_pct FROM lecturas WHERE timestamp >= ? ORDER BY id ASC", (ts_limite,))
    else:
        cursor.execute("SELECT timestamp, fecha_hora, compra, venta, spread, ganancia_pct FROM lecturas WHERE timestamp >= %s ORDER BY id ASC", (ts_limite,))
        
    rows = cursor.fetchall()
    conn.close()
    return rows

# ==========================================
# SCRAPING BINANCE P2P - EXCLUSIVO NO VERIFICADOS
# ==========================================
def get_binance_p2p_rates():
    """Consulta Binance P2P únicamente para Comerciantes NO Verificados ("publisherType": "user")."""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    # Anuncios de usuarios vendiendo USDT (Tu precio de COMPRA como comerciante)
    payload_compra = {
        "asset": "USDT", 
        "fiat": "VES", 
        "merchantCheck": False,
        "publisherType": "user",  # Exclusivo para usuarios NO verificados
        "page": 1, 
        "rows": 10, 
        "tradeType": "BUY", 
        "transAmount": "10000"
    }
    
    # Anuncios de usuarios comprando USDT (Tu precio de VENTA como comerciante)
    payload_venta = {
        "asset": "USDT", 
        "fiat": "VES", 
        "merchantCheck": False,
        "publisherType": "user",  # Exclusivo para usuarios NO verificados
        "page": 1, 
        "rows": 10, 
        "tradeType": "SELL", 
        "transAmount": "10000"
    }
    
    try:
        r_compra = requests.post(url, json=payload_compra, headers=headers, timeout=10).json()
        r_venta = requests.post(url, json=payload_venta, headers=headers, timeout=10).json()
        
        compra_list = [float(adv['adv']['price']) for adv in r_compra.get('data', [])]
        venta_list = [float(adv['adv']['price']) for adv in r_venta.get('data', [])]
        
        if not compra_list or not venta_list:
            return None, None, None, None

        # Primera oferta real del mercado de no verificados
        compra_raw = compra_list[0]
        venta_raw = venta_list[0]
        
        # Asignación correcta de puntas
        compra = min(compra_raw, venta_raw)
        venta = max(compra_raw, venta_raw)
        
        spread = round(venta - compra, 2)
        # Ganancia neta deduciendo 0.50% total de comisión
        ganancia_pct = round(((venta * 0.9975) - (compra * 1.0025)) / compra * 100, 2)
        
        return compra, venta, spread, ganancia_pct
    except Exception as e:
        print(f"⚠️ Error obteniendo datos de Binance: {e}")
        return None, None, None, None

# ==========================================
# MODELO PREDICTIVO (1h, 7h, Piso/Techo 24h)
# ==========================================
def calcular_prediccion_ml():
    historial_24h = obtener_historial_horas(24)
    n_muestras = len(historial_24h)
    
    if n_muestras < 3:
        return {
            "piso_24h": "N/A", "techo_24h": "N/A", 
            "pred_1h": "N/A", "pred_7h": "N/A",
            "tendencia": "↔️ ESTABLE (Recolectando Datos)",
            "precision": "Inicial (En entrenamiento)",
            "num_lecturas": n_muestras
        }
    
    precios_venta = [h[3] for h in historial_24h]
    timestamps = [h[0] for h in historial_24h]
    
    piso_24h = round(min(precios_venta), 2)
    techo_24h = round(max(precios_venta), 2)
    
    # Tendencia lineal según el tiempo transcurrido
    x = np.array(timestamps) - timestamps[0]
    y = np.array(precios_venta)
    
    if len(np.unique(x)) > 1:
        slope, intercept = np.polyfit(x, y, 1)
    else:
        slope, intercept = 0, y[-1]

    precio_actual = precios_venta[-1]
    
    # Proyección a 1 Hora (3600s) y 7 Horas (25200s)
    est_1h = round(precio_actual + (slope * 3600), 2)
    est_7h = round(precio_actual + (slope * 25200), 2)
    
    std_dev = np.std(precios_venta) if len(precios_venta) > 1 else 0.5
    
    pred_1h_str = f"{round(est_1h - (std_dev*0.5), 2)} - {round(est_1h + (std_dev*0.5), 2)} Bs"
    pred_7h_str = f"{round(est_7h - std_dev, 2)} - {round(est_7h + std_dev, 2)} Bs"
    
    cambio_hora = slope * 3600
    if cambio_hora > 0.50:
        tendencia = "📈 ALCISTA (Fuerte)"
    elif cambio_hora > 0.10:
        tendencia = "📈 ALCISTA (Moderada)"
    elif cambio_hora < -0.50:
        tendencia = "📉 BAJISTA (Fuerte)"
    elif cambio_hora < -0.10:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE"

    if n_muestras > 100:
        precision = "Alta (24h completas)"
    elif n_muestras > 20:
        precision = "Media"
    else:
        precision = "Inicial (En entrenamiento)"
        
    return {
        "piso_24h": piso_24h,
        "techo_24h": techo_24h,
        "pred_1h": pred_1h_str,
        "pred_7h": pred_7h_str,
        "tendencia": tendencia,
        "precision": precision,
        "num_lecturas": n_muestras
    }

# ==========================================
# MONITOR EN SEGUNDO PLANO
# ==========================================
def background_monitor():
    while True:
        compra, venta, spread, ganancia_pct = get_binance_p2p_rates()
        if compra and venta:
            guardar_lectura(compra, venta, spread, ganancia_pct)
            print(f"📊 [{datetime.now(VET).strftime('%H:%M:%S')}] Registrado - Compra: {compra} | Venta: {venta}")
        time.sleep(180)

# ==========================================
# SERVIDOR API WEB
# ==========================================
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        compra, venta, spread, ganancia = get_binance_p2p_rates()
        prediccion = calcular_prediccion_ml()
        historial = obtener_historial_horas(2)
        
        historial_formatted = [
            {
                "fecha_hora": h[1],
                "compra": h[2],
                "venta": h[3],
                "spread": h[4],
                "ganancia_pct": h[5]
            } for h in reversed(historial[-10:])
        ]

        response_data = {
            "hora_ve": datetime.now(VET).strftime("%I:%M %p"),
            "compra": compra,
            "venta": venta,
            "spread": spread,
            "ganancia_pct": ganancia,
            "filtro": "No Verificados | 10K VES | Comisión: 0.50% Total",
            "prediccion": prediccion,
            "historial_reciente": historial_formatted
        }

        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(response_data, indent=2, ensure_ascii=False).encode('utf-8'))

def run_api_server():
    server_address = ('0.0.0.0', PORT)
    httpd = HTTPServer(server_address, APIHandler)
    httpd.serve_forever()

# ==========================================
# BOT DE TELEGRAM
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 **Venbot Predicciones - Monitor P2P ML PRO**\n\n"
        "Usa `/prediccion` para obtener la tasa del mercado de comerciantes no verificados, proyecciones a 1h/7h y piso/techo de 24h."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, ganancia = get_binance_p2p_rates()
    pred = calcular_prediccion_ml()
    
    if not compra:
        await update.message.reply_text("❌ Error consultando Binance P2P. Reintentando...")
        return

    hora_actual = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P ML PRO (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_actual}\n"
        f"🎯 **Filtro:** 10K VES | **Comisión:** 0.50% Total\n\n"
        f"🟢 **Compra:** {compra:.2f} Bs\n"
        f"🔴 **Venta:** {venta:.2f} Bs\n"
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {ganancia:.2f}%\n\n"
        f"🔮 **Predicción (1 hora):** {pred['pred_1h']}\n"
        f"🔮 **Predicción (7 horas):** {pred['pred_7h']}\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n"
        f"🟢 **Piso (24h):** {pred['piso_24h']} Bs | 🔴 **Techo (24h):** {pred['techo_24h']} Bs\n\n"
        f"🧠 **Muestras (24h):** {pred['num_lecturas']} | **Precisión:** {pred['precision']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==========================================
# INICIO DE APLICACIÓN
# ==========================================
if __name__ == "__main__":
    init_db()

    t_monitor = threading.Thread(target=background_monitor, daemon=True)
    t_monitor.start()

    t_api = threading.Thread(target=run_api_server, daemon=True)
    t_api.start()

    if not TELEGRAM_TOKEN:
        print("❌ ERROR CRÍTICO: FALTA LA VARIABLE TELEGRAM_TOKEN")
    else:
        print("🤖 Bot de Telegram en ejecución...")
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("prediccion", prediccion))
        app.run_polling()
