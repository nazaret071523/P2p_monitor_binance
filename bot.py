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
# GESTIÓN DE BASE DE DATOS (Neon Postgres / SQLite Fallback)
# ==========================================
def get_db_connection():
    """Conecta a Neon.tech PostgreSQL si existe DATABASE_URL, de lo contrario a SQLite."""
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            print(f"⚠️ Error conectando a PostgreSQL ({e}). Usando SQLite local...")
    return sqlite3.connect("p2p_data.db")

def init_db():
    """Crea la tabla 'lecturas' si no existe."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if isinstance(conn, sqlite3.Connection):
        create_table_query = """
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
        create_table_query = """
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

    cursor.execute(create_table_query)
    conn.commit()
    conn.close()
    print("✅ Base de datos inicializada correctamente.")

def guardar_lectura(compra, venta, spread, ganancia_pct):
    """Guarda una nueva lectura de mercado."""
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

def obtener_historial(limite=100):
    """Obtiene los últimos registros guardados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if isinstance(conn, sqlite3.Connection):
        cursor.execute("SELECT timestamp, fecha_hora, compra, venta, spread, ganancia_pct FROM lecturas ORDER BY id DESC LIMIT ?", (limite,))
    else:
        cursor.execute("SELECT timestamp, fecha_hora, compra, venta, spread, ganancia_pct FROM lecturas ORDER BY id DESC LIMIT %s", (limite,))
        
    rows = cursor.fetchall()
    conn.close()
    return rows

# ==========================================
# SCRAPING Y MODELO ML DE PREDICCIÓN
# ==========================================
def get_binance_p2p_rates():
    """Consulta Binance P2P para USDT/VES sin verificar, filtro 5k-300k."""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    payload_buy = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 5, "tradeType": "BUY", "transAmount": "5000"
    }
    payload_sell = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 5, "tradeType": "SELL", "transAmount": "5000"
    }
    
    try:
        r_buy = requests.post(url, json=payload_buy, headers=headers, timeout=10).json()
        r_sell = requests.post(url, json=payload_sell, headers=headers, timeout=10).json()
        
        compra = float(r_buy['data'][0]['adv']['price'])
        venta = float(r_sell['data'][0]['adv']['price'])
        
        spread = round(venta - compra, 2)
        # Ganancia neta calculada con 0.50% de comisión total (0.25% compra + 0.25% venta)
        ganancia_pct = round(((venta * 0.9975) - (compra * 1.0025)) / compra * 100, 2)
        
        return compra, venta, spread, ganancia_pct
    except Exception as e:
        print(f"⚠️ Error obteniendo datos de Binance: {e}")
        return None, None, None, None

def calcular_prediccion_ml():
    """Analiza la tendencia e historial con regresión lineal."""
    historial = obtener_historial(limite=50)
    n_muestras = len(historial)
    
    if n_muestras < 3:
        return {
            "piso": "N/A", "techo": "N/A", "rango_min": "N/A", "rango_max": "N/A",
            "tendencia": "↔️ ESTABLE (Recolectando Datos)",
            "precision": "Inicial (En entrenamiento)",
            "num_lecturas": n_muestras
        }
    
    precios_venta = [h[3] for h in reversed(historial)]
    x = np.arange(len(precios_venta))
    y = np.array(precios_venta)
    
    slope, intercept = np.polyfit(x, y, 1)
    
    piso = round(min(precios_venta), 2)
    techo = round(max(precios_venta), 2)
    
    # Rango estimado próximo
    proxima_est = slope * len(precios_venta) + intercept
    std_dev = np.std(precios_venta) if len(precios_venta) > 1 else 0.5
    rango_min = round(proxima_est - std_dev, 2)
    rango_max = round(proxima_est + std_dev, 2)
    
    if slope > 0.08:
        tendencia = "📈 ALCISTA (Fuerte)"
    elif slope > 0.02:
        tendencia = "📈 ALCISTA (Moderada)"
    elif slope < -0.08:
        tendencia = "📉 BAJISTA (Fuerte)"
    elif slope < -0.02:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE"

    if n_muestras > 30:
        precision = "Alta"
    elif n_muestras > 10:
        precision = "Media"
    else:
        precision = "Inicial (En entrenamiento)"
        
    return {
        "piso": piso,
        "techo": techo,
        "rango_min": rango_min,
        "rango_max": rango_max,
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
        time.sleep(180) # Consulta cada 3 minutos

# ==========================================
# SERVIDOR API WEB
# ==========================================
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        compra, venta, spread, ganancia = get_binance_p2p_rates()
        prediccion = calcular_prediccion_ml()
        historial = obtener_historial(limite=10)
        
        historial_formatted = []
        for h in historial:
            historial_formatted.append({
                "fecha_hora": h[1],
                "compra": h[2],
                "venta": h[3],
                "spread": h[4],
                "ganancia_pct": h[5]
            })

        response_data = {
            "hora_ve": datetime.now(VET).strftime("%I:%M %p"),
            "compra": compra,
            "venta": venta,
            "spread": spread,
            "ganancia_pct": ganancia,
            "filtro": "5K - 300K VES | Comisión: 0.50% Total",
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
    print(f"🌐 Servidor API Web escuchando en el puerto {PORT}...")
    httpd.serve_forever()

# ==========================================
# BOT DE TELEGRAM
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 **Venbot Predicciones - Monitor P2P ML PRO**\n\n"
        "Usa `/prediccion` para obtener la tasa del mercado, ganancia neta y proyecciones de inteligencia artificial."
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
        f"🤖 **MONITOR P2P ML PRO**\n"
        f"⏰ **Hora VE:** {hora_actual}\n"
        f"🎯 **Filtro:** 5K - 300K VES | **Comisión:** 0.50% Total\n\n"
        f"🟢 **Compra:** {compra:.2f} Bs\n"
        f"🔴 **Venta:** {venta:.2f} Bs\n"
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {ganancia:.2f}%\n\n"
        f"↔️ **Rango Estimado:** {pred['rango_min']} - {pred['rango_max']} Bs\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n"
        f"🟢 **Piso:** {pred['piso']} Bs | 🔴 **Techo:** {pred['techo']} Bs\n\n"
        f"🧠 **Entrenado con {pred['num_lecturas']} muestras | Precisión:** {pred['precision']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==========================================
# INICIO DE APLICACIÓN
# ==========================================
if __name__ == "__main__":
    init_db()

    # Hilo para recolección continua de datos
    t_monitor = threading.Thread(target=background_monitor, daemon=True)
    t_monitor.start()

    # Hilo para el servidor API Web (evita suspensión en Render)
    t_api = threading.Thread(target=run_api_server, daemon=True)
    t_api.start()

    # Iniciar bot de Telegram
    if not TELEGRAM_TOKEN:
        print("❌ ERROR CRÍTICO: FALTA LA VARIABLE TELEGRAM_TOKEN")
    else:
        print("🤖 Bot de Telegram en ejecución...")
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("prediccion", prediccion))
        app.run_polling()
