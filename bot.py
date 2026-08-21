import os
import json
import time
import sqlite3
import threading
import http.server
import socketserver
import urllib.request
import statistics
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = "8579313357:AAE3_PCgfY2zmpkVJWIz8gA4ECeDBufoct4"
DB_FILE = "p2p_historial.db"

# 1. Base de datos SQLite para persistencia
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS precios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            precio REAL
        )
    """)
    conn.commit()
    conn.close()

def guardar_precio(precio):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO precios (precio) VALUES (?)", (precio,))
    conn.commit()
    conn.close()

def obtener_historial(limit=100):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT precio FROM precios ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in reversed(rows)]

# 2. Servidor web falso para evitar "Timed out" en Render Free
def run_dummy_server():
    port = int(os.environ.get("PORT", 8080))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        httpd.serve_forever()

# 3. Consulta de precio a Binance P2P
def get_binance_p2p_price():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "rows": 10,
        "tradeType": "BUY"
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
                prices = [float(adv["adv"]["price"]) for adv in res_data["data"][:5]]
                return round(sum(prices) / len(prices), 2)
    except Exception as e:
        print(f"Error consultando Binance: {e}")

    return None

# 4. Tarea en segundo plano: recolecta datos cada 15 minutos automáticamente
def auto_collector():
    while True:
        precio = get_binance_p2p_price()
        if precio:
            guardar_precio(precio)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Recolectado en DB: {precio} Bs")
        time.sleep(900)  # 15 minutos

# 5. Modelo Estadístico / Machine Learning
def calcular_modelo_ml(precio_actual):
    historial = obtener_historial(50)
    num_lecturas = len(historial)
    
    if num_lecturas < 3:
        volatilidad = precio_actual * 0.008
        prediccion = precio_actual + (volatilidad * 0.3)
    else:
        media_corta = statistics.mean(historial[-3:])
        media_larga = statistics.mean(historial)
        desviacion = statistics.stdev(historial) if num_lecturas > 2 else precio_actual * 0.005
        
        tendencia_inercial = media_corta - media_larga
        prediccion = precio_actual + tendencia_inercial + (desviacion * 0.2)
        volatilidad = max(desviacion * 1.5, precio_actual * 0.005)

    piso = round(precio_actual - volatilidad, 2)
    techo = round(precio_actual + volatilidad, 2)
    prediccion_final = round(prediccion, 2)

    return prediccion_final, piso, techo, num_lecturas

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot Activo. Usa /prediccion para consultar precios en vivo.")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    precio_actual = get_binance_p2p_price()
    
    if not precio_actual:
        await update.message.reply_text("⚠️ No se pudo obtener la tasa en vivo de Binance. Intenta en unos segundos.")
        return

    # Registrar también la consulta del usuario
    guardar_precio(precio_actual)
    
    prediccion_ml, piso, techo, lecturas = calcular_modelo_ml(precio_actual)
    hora_proyeccion = (datetime.now() + timedelta(hours=3)).strftime("%H:%M")
    
    if prediccion_ml > precio_actual:
        tendencia = "📈 ALCISTA"
    elif prediccion_ml < precio_actual:
        tendencia = "📉 BAJISTA"
    else:
        tendencia = "↔️ LATERAL / ESTABLE"

    mensaje = (
        f"🤖 **PREDICCIÓN MACHINE LEARNING**\n"
        f"⏰ Proyección para: {hora_proyeccion}\n\n"
        f"📌 **Precio Actual:** {precio_actual:.2f} Bs\n"
        f"🎯 **Predicción ML:** {prediccion_ml:.2f} Bs\n"
        f"📊 **Tendencia Estimada:** {tendencia}\n\n"
        f"🟢 **Piso Calculado:** {piso:.2f} Bs\n"
        f"🔴 **Techo Calculado:** {techo:.2f} Bs\n\n"
        f"🧠 *Modelado con {lecturas} lecturas guardadas en BD.*"
    )
    
    await update.message.reply_text(mensaje, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()
    # Inicia servidor web y recolector automático en hilos separados
    threading.Thread(target=run_dummy_server, daemon=True).start()
    threading.Thread(target=auto_collector, daemon=True).start()
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", prediccion))
    app.run_polling()
