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

# Lista global para usuarios suscritos a alertas
SUSCRIPTORES = set()
ULTIMA_TENDENCIA = "NEUTRA"

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
# SCRAPING BINANCE P2P - NO VERIFICADOS (10K y 300K)
# ==========================================
def get_binance_p2p_rates():
    """Consulta Binance P2P para no verificados evaluando rango amplio (10k a 300k)."""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    # Anuncios donde la gente VENDE USDT (Tu precio de COMPRA como comerciante)
    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "publisherType": "user", "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "10000"
    }
    # Anuncios donde la gente COMPRA USDT (Tu precio de VENTA/RECOMPRA como comerciante)
    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "publisherType": "user", "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000"
    }
    
    try:
        r_compra = requests.post(url, json=payload_compra, headers=headers, timeout=10).json()
        r_venta = requests.post(url, json=payload_venta, headers=headers, timeout=10).json()
        
        compra_list = [float(adv['adv']['price']) for adv in r_compra.get('data', [])]
        venta_list = [float(adv['adv']['price']) for adv in r_venta.get('data', [])]
        
        if not compra_list or not venta_list:
            return None, None, None, None

        compra_raw = compra_list[0]
        venta_raw = venta_list[0]
        
        compra = min(compra_raw, venta_raw)
        venta = max(compra_raw, venta_raw)
        
        spread = round(venta - compra, 2)
        ganancia_pct = round(((venta * 0.9975) - (compra * 1.0025)) / compra * 100, 2)
        
        return compra, venta, spread, ganancia_pct
    except Exception as e:
        print(f"⚠️ Error obteniendo datos de Binance: {e}")
        return None, None, None, None

# ==========================================
# PREDICCIÓN A 7 HORAS (COMPRA Y RECOMPRA) + SOPORTE Y RESISTENCIA
# ==========================================
def calcular_prediccion_ml():
    historial_24h = obtener_historial_horas(24)
    n_muestras = len(historial_24h)
    
    if n_muestras < 3:
        return {
            "soporte_piso": "N/A", "resistencia_techo": "N/A", 
            "pred_compra_7h": "N/A", "pred_venta_7h": "N/A",
            "tendencia": "↔️ NEUTRA (En entrenamiento)",
            "precision": "Inicial",
            "num_lecturas": n_muestras
        }
    
    compras = [h[2] for h in historial_24h]
    ventas = [h[3] for h in historial_24h]
    timestamps = [h[0] for h in historial_24h]
    
    # Cálculo de Resistencia (Techo) y Soporte (Piso) usando percentiles
    soporte_piso = round(np.percentile(compras, 10), 2)
    resistencia_techo = round(np.percentile(ventas, 90), 2)
    
    x = np.array(timestamps) - timestamps[0]
    
    # Tendencia de Compra
    slope_c, _ = np.polyfit(x, np.array(compras), 1) if len(np.unique(x)) > 1 else (0, 0)
    # Tendencia de Venta
    slope_v, _ = np.polyfit(x, np.array(ventas), 1) if len(np.unique(x)) > 1 else (0, 0)

    compra_act = compras[-1]
    venta_act = ventas[-1]
    
    # Proyección a 7 Horas (25,200 segundos)
    est_compra_7h = round(compra_act + (slope_c * 25200), 2)
    est_venta_7h = round(venta_act + (slope_v * 25200), 2)
    
    std_c = np.std(compras) if len(compras) > 1 else 0.5
    std_v = np.std(ventas) if len(ventas) > 1 else 0.5
    
    pred_compra_str = f"{round(est_compra_7h - std_c, 2)} - {round(est_compra_7h + std_c, 2)} Bs"
    pred_venta_str = f"{round(est_venta_7h - std_v, 2)} - {round(est_venta_7h + std_v, 2)} Bs"
    
    cambio_hora = slope_v * 3600
    if cambio_hora > 0.15:
        tendencia = "📈 ALCISTA"
    elif cambio_hora < -0.15:
        tendencia = "📉 BAJISTA"
    else:
        tendencia = "↔️ NEUTRA"

    precision = "Alta (24h)" if n_muestras > 100 else ("Media" if n_muestras > 20 else "Inicial")
        
    return {
        "soporte_piso": soporte_piso,
        "resistencia_techo": resistencia_techo,
        "pred_compra_7h": pred_compra_str,
        "pred_venta_7h": pred_venta_str,
        "tendencia": tendencia,
        "precision": precision,
        "num_lecturas": n_muestras
    }

# ==========================================
# MONITOR EN SEGUNDO PLANO Y ALERTAS AUTOMÁTICAS
# ==========================================
async def notificar_cambio_tendencia(app, nueva_tendencia):
    msg = f"🚨 **ALERTA DE MERCADO P2P**\n\nEl mercado de comerciantes no verificados ha cambiado de tendencia a: **{nueva_tendencia}**."
    for chat_id in list(SUSCRIPTORES):
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        except Exception:
            pass

def background_monitor(app_telegram):
    global ULTIMA_TENDENCIA
    while True:
        compra, venta, spread, ganancia_pct = get_binance_p2p_rates()
        if compra and venta:
            guardar_lectura(compra, venta, spread, ganancia_pct)
            pred = calcular_prediccion_ml()
            
            # Detección de cambio de tendencia para alertas automáticas
            if pred["tendencia"] in ["📈 ALCISTA", "📉 BAJISTA"] and pred["tendencia"] != ULTIMA_TENDENCIA:
                ULTIMA_TENDENCIA = pred["tendencia"]
                if app_telegram:
                    import asyncio
                    asyncio.run_coroutine_threadsafe(
                        notificar_cambio_tendencia(app_telegram, ULTIMA_TENDENCIA),
                        app_telegram.loop
                    )
            print(f"📊 [{datetime.now(VET).strftime('%H:%M:%S')}] Compra: {compra} | Venta: {venta} | Tendencia: {pred['tendencia']}")
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
            "filtro": "No Verificados | 10k-300k VES",
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
    SUSCRIPTORES.add(update.effective_chat.id)
    msg = (
        "🤖 **Venbot Predicciones - Monitor P2P PRO**\n\n"
        "Te has registrado para recibir **alertas automáticas de cambio de tendencia**.\n"
        "Usa `/prediccion` para obtener las proyecciones a 7 horas de compra y venta."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    SUSCRIPTORES.add(update.effective_chat.id)
    compra, venta, spread, ganancia = get_binance_p2p_rates()
    pred = calcular_prediccion_ml()
    
    if not compra:
        await update.message.reply_text("❌ Error consultando Binance P2P. Reintentando...")
        return

    hora_actual = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P ML PRO (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_actual}\n"
        f"🎯 **Filtro:** 10K - 300K VES | **Comisión:** 0.50%\n\n"
        f"🟢 **Tasa Compra Actual:** {compra:.2f} Bs\n"
        f"🔴 **Tasa Venta Actual:** {venta:.2f} Bs\n"
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {ganancia:.2f}%\n\n"
        f"🔮 **Proyección Compra (7h):** {pred['pred_compra_7h']}\n"
        f"🔮 **Proyección Recompra/Venta (7h):** {pred['pred_venta_7h']}\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n"
        f"🛡️ **Soporte / Piso:** {pred['soporte_piso']} Bs\n"
        f"🏰 **Resistencia / Techo:** {pred['resistencia_techo']} Bs\n\n"
        f"🧠 **Muestras 24h:** {pred['num_lecturas']} | **Precisión:** {pred['precision']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()

    t_api = threading.Thread(target=run_api_server, daemon=True)
    t_api.start()

    if not TELEGRAM_TOKEN:
        print("❌ ERROR CRÍTICO: FALTA LA VARIABLE TELEGRAM_TOKEN")
    else:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("prediccion", prediccion))
        
        t_monitor = threading.Thread(target=background_monitor, args=(app,), daemon=True)
        t_monitor.start()

        print("🤖 Bot de Telegram en ejecución...")
        app.run_polling()
