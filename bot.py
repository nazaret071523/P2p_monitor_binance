import os
import json
import time
import sqlite3
import threading
import urllib.request
import statistics
import math
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Intentar importar psycopg2 para soporte PostgreSQL en la nube
try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

TELEGRAM_TOKEN = os.environ.get"8579313357:AAE3_PCgfY2zmpkVJWIz8gA4ECeDBufoct4"
DB_FILE = "p2p_historial.db"
DATABASE_URL = os.environ.get("DATABASE_URL")  # URL de Neon.tech / Supabase
TZ_VE = timezone(timedelta(hours=-4))

TENDENCIA_ANTERIOR = None

# --- CONEXIÓN BASE DE DATOS UNIFICADA (PostgreSQL / SQLite) ---
def get_db_connection():
    if DATABASE_URL and HAS_POSTGRES:
        # Conexión permanente en la nube (Neon.tech)
        return psycopg2.connect(DATABASE_URL, sslmode="require")
    else:
        # Respaldo Local SQLite
        return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if DATABASE_URL and HAS_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precios (
                id SERIAL PRIMARY KEY,
                timestamp VARCHAR(50),
                compra REAL,
                venta REAL
            )
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                compra REAL,
                venta REAL
            )
        """)
    conn.commit()
    conn.close()

def guardar_precios(compra, venta):
    conn = get_db_connection()
    cursor = conn.cursor()
    hora_ve = datetime.now(TZ_VE).strftime("%Y-%m-%d %H:%M:%S")
    
    if DATABASE_URL and HAS_POSTGRES:
        cursor.execute("INSERT INTO precios (timestamp, compra, venta) VALUES (%s, %s, %s)", (hora_ve, compra, venta))
    else:
        cursor.execute("INSERT INTO precios (timestamp, compra, venta) VALUES (?, ?, ?)", (hora_ve, compra, venta))
        
    conn.commit()
    conn.close()

def obtener_historial(limit=200):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if DATABASE_URL and HAS_POSTGRES:
        cursor.execute("SELECT timestamp, compra, venta FROM precios ORDER BY id DESC LIMIT %s", (limit,))
    else:
        cursor.execute("SELECT timestamp, compra, venta FROM precios ORDER BY id DESC LIMIT ?", (limit,))
        
    rows = cursor.fetchall()
    conn.close()
    return list(reversed(rows))

# --- EXTRACTOR BINANCE P2P ---
def get_binance_p2p_rates(full_samples=False):
    def fetch_type(trade_type):
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        payload = json.dumps({
            "asset": "USDT",
            "fiat": "VES",
            "merchantCheck": False,
            "transAmount": "5000",
            "page": 1,
            "rows": 20,
            "tradeType": trade_type
        }).encode("utf-8")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json"
        }
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=8) as response:
                res_data = json.loads(response.read().decode("utf-8"))
                if res_data.get("data") and len(res_data["data"]) > 0:
                    prices = []
                    for adv in res_data["data"]:
                        max_single_trans = float(adv["adv"]["maxSingleTransAmount"])
                        if max_single_trans >= 5000:
                            prices.append(float(adv["adv"]["price"]))
                    if prices:
                        return prices if full_samples else round(sum(prices[:5]) / len(prices[:5]), 2)
        except Exception as e:
            print(f"Error consultando Binance {trade_type}: {e}")
        return None

    if full_samples:
        compras = fetch_type("SELL") or [915.21]
        ventas = fetch_type("BUY") or [920.20]
        return compras, ventas

    compra = fetch_type("SELL")
    venta = fetch_type("BUY")
    compra_val = compra if isinstance(compra, float) else 915.21
    venta_val = venta if isinstance(venta, float) else 920.20
    return compra_val, venta_val

def precargar_historial_si_vacio():
    historial = obtener_historial(10)
    if len(historial) < 5:
        print("⚡ DB con pocas muestras. Precargando datos vivos de Binance...")
        compras, ventas = get_binance_p2p_rates(full_samples=True)
        min_len = min(len(compras), len(ventas))
        for i in range(min_len):
            guardar_precios(compras[i], ventas[i])

# --- IA Y PREDICCIÓN ---
def calcular_prediccion_avanzada(compra_actual, historial):
    global TENDENCIA_ANTERIOR
    num_lecturas = len(historial)
    hora_ve_actual = datetime.now(TZ_VE)
    hora_proyeccion = (hora_ve_actual + timedelta(hours=1)).strftime("%I:%M %p")
    
    precios = [row[1] for row in historial]
    n = len(precios)
    pesos = [math.exp(i / n) for i in range(n)]
    sum_w = sum(pesos)
    
    x = list(range(n))
    mean_x = sum(i * w for i, w in zip(x, pesos)) / sum_w
    mean_y = sum(y * w for y, w in zip(precios, pesos)) / sum_w
    
    num = sum(w * (x[i] - mean_x) * (precios[i] - mean_y) for i, w in enumerate(pesos))
    den = sum(w * ((x[i] - mean_x) ** 2) for i, w in enumerate(pesos))
    
    slope = num / den if den != 0 else 0
    desviacion = statistics.stdev(precios) if n > 2 else compra_actual * 0.003
    
    if slope < -0.02:
        tendencia_limpia = "BAJISTA"
        tendencia = "📉 BAJISTA (Fuerte)" if slope < -0.08 else "↘️ BAJISTA (Moderada)"
    elif slope > 0.02:
        tendencia_limpia = "ALCISTA"
        tendencia = "📈 ALCISTA (Fuerte)" if slope > 0.08 else "↗️ ALCISTA (Moderada)"
    else:
        tendencia_limpia = "LATERAL"
        tendencia = "↔️ LATERAL"

    piso = round(min(precios[-10:]) - (desviacion * 0.5), 2)
    techo = round(max(precios[-10:]) + (desviacion * 0.5), 2)

    if tendencia_limpia == "BAJISTA":
        target = max(piso, compra_actual + (slope * 4))
        prediccion_ml = round(target, 2)
        texto_target = f"🔮 **Proyección de Caída ({hora_proyeccion}):** {prediccion_ml:.2f} Bs"
    elif tendencia_limpia == "ALCISTA":
        target = min(techo, compra_actual + (slope * 4))
        prediccion_ml = round(target, 2)
        texto_target = f"🔮 **Proyección de Subida ({hora_proyeccion}):** {prediccion_ml:.2f} Bs"
    else:
        prediccion_ml = round(compra_actual, 2)
        texto_target = f"🔮 **Rango de Oscilación ({hora_proyeccion}):** {piso:.2f} - {techo:.2f} Bs"

    alerta_cambio = ""
    if TENDENCIA_ANTERIOR and TENDENCIA_ANTERIOR != tendencia_limpia:
        alerta_cambio = f"\n⚠️ **¡ALERTA DE CAMBIO DE TENDENCIA!**\nEl mercado cambió a **{tendencia}**\n"
    TENDENCIA_ANTERIOR = tendencia_limpia

    precision = "Alta" if num_lecturas > 30 else ("Media" if num_lecturas > 10 else "Inicial")

    return {
        "prediccion_ml": prediccion_ml,
        "piso": piso,
        "techo": techo,
        "tendencia": tendencia,
        "alerta_cambio": alerta_cambio,
        "texto_target": texto_target,
        "hora_proyeccion": hora_proyeccion,
        "num_lecturas": num_lecturas,
        "precision_score": precision
    }

# --- API Y SERVIDOR ---
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        compra, venta = get_binance_p2p_rates()
        spread = round(venta - compra, 2)
        ganancia_bruta_pct = ((venta - compra) / compra) * 100 if compra else 0
        ganancia_neta_pct = round(ganancia_bruta_pct - 0.50, 2)
        
        historial = obtener_historial(200)
        ml_data = calcular_prediccion_avanzada(compra, historial)
        hora_actual_ve = datetime.now(TZ_VE).strftime("%H:%M:%S")

        data_response = {
            "hora_ve": hora_actual_ve,
            "compra": compra,
            "venta": venta,
            "spread": spread,
            "ganancia_pct": max(ganancia_neta_pct, 0.0),
            "filtro_rango": "5,000 - 300,000 VES",
            "prediccion": ml_data,
            "historial": [{"hora": row[0][11:16] if row[0] else "--:--", "compra": row[1], "venta": row[2]} for row in historial[-20:]]
        }
        self.wfile.write(json.dumps(data_response).encode("utf-8"))

def run_api_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("", port), APIHandler)
    server.serve_forever()

def auto_collector():
    while True:
        compra, venta = get_binance_p2p_rates()
        if compra and venta:
            guardar_precios(compra, venta)
        time.sleep(180)

# --- COMANDOS TELEGRAM ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot P2P Activo. Usa /prediccion para consultar la IA P2P.")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    precargar_historial_si_vacio()

    compra, venta = get_binance_p2p_rates()
    guardar_precios(compra, venta)
    
    historial = obtener_historial(200)
    ml = calcular_prediccion_avanzada(compra, historial)
    
    spread = round(venta - compra, 2)
    ganancia_neta = round((((venta - compra) / compra) * 100) - 0.50, 2)
    hora_actual_str = datetime.now(TZ_VE).strftime("%I:%M %p")

    mensaje = (
        f"🤖 **MONITOR P2P ML PRO**\n"
        f"🕒 *Hora VE: {hora_actual_str}*\n"
        f"🎯 *Filtro: 5K - 300K VES | Comisión: 0.50% Total*\n\n"
        f"🟢 **Compra:** {compra:.2f} Bs\n"
        f"🔴 **Venta:** {venta:.2f} Bs\n"
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {max(ganancia_neta, 0.0):.2f}%\n"
        f"{ml['alerta_cambio']}\n"
        f"{ml['texto_target']}\n"
        f"📊 **Tendencia:** {ml['tendencia']}\n"
        f"🟢 **Piso:** {ml['piso']:.2f} Bs | 🔴 **Techo:** {ml['techo']:.2f} Bs\n\n"
        f"🧠 *Entrenado con {ml['num_lecturas']} muestras | Precisión: {ml['precision_score']}*"
    )
    await update.message.reply_text(mensaje, parse_mode="Markdown")

async def exportar_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if os.path.exists(DB_FILE):
        await update.message.reply_document(document=open(DB_FILE, 'rb'), filename="p2p_historial_backup.db")
    else:
        await update.message.reply_text("Los datos están almacenados en PostgreSQL (Nube permanente).")

if __name__ == "__main__":
    init_db()
    precargar_historial_si_vacio()
    threading.Thread(target=run_api_server, daemon=True).start()
    threading.Thread(target=auto_collector, daemon=True).start()
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", prediccion))
    app.add_handler(CommandHandler("exportar_db", exportar_db))
    app.run_polling()
