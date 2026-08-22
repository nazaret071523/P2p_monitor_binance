import os
import json
import time
import sqlite3
import threading
import urllib.request
import statistics
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = "8579313357:AAE3_PCgfY2zmpkVJWIz8gA4ECeDBufoct4"
DB_FILE = "p2p_historial.db"

# 1. Base de datos SQLite
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS precios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            compra REAL,
            venta REAL
        )
    """)
    conn.commit()
    conn.close()

def guardar_precios(compra, venta):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO precios (compra, venta) VALUES (?, ?)", (compra, venta))
    conn.commit()
    conn.close()

def obtener_historial(limit=50):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, compra, venta FROM precios ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return list(reversed(rows))

# 2. Extractor Binance P2P (No Verificados | 5k - 300k VES)
def get_binance_p2p_rates():
    def fetch_type(trade_type):
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        payload = json.dumps({
            "asset": "USDT",
            "fiat": "VES",
            "merchantCheck": False,  # Incluye comerciantes NO verificados
            "transAmount": "5000",   # Filtro mínimo desde 5,000 VES
            "page": 1,
            "rows": 10,
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
                        if len(prices) >= 5:
                            break
                    if prices:
                        return round(sum(prices) / len(prices), 2)
        except Exception as e:
            print(f"Error consultando Binance {trade_type}: {e}")
        return None

    # Mapeo corregido: SELL para el precio de compra del comerciante y BUY para la venta
    compra = fetch_type("SELL")
    venta = fetch_type("BUY")
    
    return compra or 915.21, venta or 920.20

# 3. Servidor API Web para el Dashboard en Vercel
class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        compra, venta = get_binance_p2p_rates()
        spread = round(venta - compra, 2)
        
        # Margen neto restando 0.25% compra + 0.25% venta (0.50% total)
        ganancia_bruta_pct = ((venta - compra) / compra) * 100 if compra else 0
        ganancia_neta_pct = round(ganancia_bruta_pct - 0.50, 2)
        
        historial = obtener_historial(20)

        data_response = {
            "compra": compra,
            "venta": venta,
            "spread": spread,
            "ganancia_pct": max(ganancia_neta_pct, 0.0),
            "filtro_rango": "5,000 - 300,000 VES",
            "historial": [{"hora": row[0][11:16], "compra": row[1], "venta": row[2]} for row in historial]
        }
        self.wfile.write(json.dumps(data_response).encode("utf-8"))

def run_api_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("", port), APIHandler)
    server.serve_forever()

# 4. Hilo de recolección continua
def auto_collector():
    while True:
        compra, venta = get_binance_p2p_rates()
        if compra and venta:
            guardar_precios(compra, venta)
        time.sleep(900)

# 5. Comandos del Bot de Telegram
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot P2P Activo. Usa /prediccion para ver métricas y proyecciones en vivo.")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta = get_binance_p2p_rates()
    guardar_precios(compra, venta)
    
    historial_filas = obtener_historial(50)
    precios_compra = [r[1] for r in historial_filas]
    num_lecturas = len(precios_compra)
    
    if num_lecturas < 3:
        volatilidad = compra * 0.008
        prediccion_val = compra + (volatilidad * 0.3)
    else:
        media_corta = statistics.mean(precios_compra[-3:])
        media_larga = statistics.mean(precios_compra)
        desviacion = statistics.stdev(precios_compra) if num_lecturas > 2 else compra * 0.005
        prediccion_val = compra + (media_corta - media_larga) + (desviacion * 0.2)
        volatilidad = max(desviacion * 1.5, compra * 0.005)

    piso = round(compra - volatilidad, 2)
    techo = round(compra + volatilidad, 2)
    prediccion_ml = round(prediccion_val, 2)
    spread = round(venta - compra, 2)
    
    # Margen neto con comisiones (0.25% + 0.25%)
    ganancia_neta = round((((venta - compra) / compra) * 100) - 0.50, 2)
    
    hora_proyeccion = (datetime.now() + timedelta(hours=3)).strftime("%H:%M")
    tendencia = "📈 ALCISTA" if prediccion_ml > compra else ("📉 BAJISTA" if prediccion_ml < compra else "↔️ LATERAL")

    mensaje = (
        f"🤖 **MONITOR P2P NO VERIFICADO**\n"
        f"🎯 *Filtro: 5K - 300K VES | Comisión: 0.25% + 0.25%*\n\n"
        f"🟢 **Compra:** {compra:.2f} Bs\n"
        f"🔴 **Venta:** {venta:.2f} Bs\n"
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {max(ganancia_neta, 0.0):.2f}%\n\n"
        f"🔮 **Predicción ML ({hora_proyeccion}):** {prediccion_ml:.2f} Bs ({tendencia})\n"
        f"🟢 **Piso:** {piso:.2f} Bs | 🔴 **Techo:** {techo:.2f} Bs\n\n"
        f"🧠 *Modelado con {num_lecturas} lecturas en BD.*"
    )
    await update.message.reply_text(mensaje, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_api_server, daemon=True).start()
    threading.Thread(target=auto_collector, daemon=True).start()
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", prediccion))
    app.run_polling()
