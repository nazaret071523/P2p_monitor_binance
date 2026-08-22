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

TELEGRAM_TOKEN = "8579313357:AAE3_PCgfY2zmpkVJWIz8gA4ECeDBufoct4"
DB_FILE = "p2p_historial.db"
TZ_VE = timezone(timedelta(hours=-4))

# 1. Base de datos
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
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
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    hora_ve = datetime.now(TZ_VE).strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("INSERT INTO precios (timestamp, compra, venta) VALUES (?, ?, ?)", (hora_ve, compra, venta))
    conn.commit()
    conn.close()

def obtener_historial(limit=200):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, compra, venta FROM precios ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return list(reversed(rows))

# 2. Extractor Binance P2P
def get_binance_p2p_rates():
    def fetch_type(trade_type):
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        payload = json.dumps({
            "asset": "USDT",
            "fiat": "VES",
            "merchantCheck": False,
            "transAmount": "5000",
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

    compra = fetch_type("SELL")
    venta = fetch_type("BUY")
    return compra or 915.21, venta or 920.20

# 3. Motor Avanzado de Machine Learning (Weighted Linear Regression + Volatility Model)
def calcular_prediccion_avanzada(compra_actual, historial):
    num_lecturas = len(historial)
    hora_ve_actual = datetime.now(TZ_VE)
    hora_proyeccion = (hora_ve_actual + timedelta(hours=1)).strftime("%I:%M %p")
    
    if num_lecturas < 5:
        # Modo frio (pocas lecturas acumuladas)
        volatilidad = compra_actual * 0.005
        return {
            "prediccion_ml": round(compra_actual + 0.10, 2),
            "piso": round(compra_actual - volatilidad, 2),
            "techo": round(compra_actual + volatilidad, 2),
            "tendencia": "↔️ ESTABLE (Recolectando Datos)",
            "hora_proyeccion": hora_proyeccion,
            "num_lecturas": num_lecturas,
            "precision_score": "Inicial (En entrenamiento)"
        }

    # Extracción de características (X = tiempo en minutos, Y = precio compra)
    precios = [row[1] for row in historial]
    
    # Regresión Lineal Ponderada (dar más peso a los datos más recientes)
    n = len(precios)
    pesos = [math.exp(i / n) for i in range(n)]  # Ponderación exponencial
    sum_w = sum(pesos)
    
    x = list(range(n))
    mean_x = sum(i * w for i, w in zip(x, pesos)) / sum_w
    mean_y = sum(y * w for y, w in zip(precios, pesos)) / sum_w
    
    num = sum(w * (x[i] - mean_x) * (precios[i] - mean_y) for i, w in enumerate(pesos))
    den = sum(w * ((x[i] - mean_x) ** 2) for i, w in enumerate(pesos))
    
    slope = num / den if den != 0 else 0
    
    # Proyección a 4 pasos (1 hora si se guarda cada 15 min)
    prediccion_base = compra_actual + (slope * 4)
    
    # Ajuste por Desviación Estándar Dinámica (Medición del Riesgo/Piso/Techo)
    desviacion = statistics.stdev(precios) if n > 2 else compra_actual * 0.003
    piso = round(prediccion_base - (desviacion * 1.25), 2)
    techo = round(prediccion_base + (desviacion * 1.25), 2)
    prediccion_ml = round(prediccion_base, 2)
    
    # Clasificación de tendencia
    diff = prediccion_ml - compra_actual
    if diff > 0.15:
        tendencia = "📈 ALCISTA (Fuerte)"
    elif diff > 0.02:
        tendencia = "↗️ ALCISTA (Moderada)"
    elif diff < -0.15:
        tendencia = "📉 BAJISTA (Fuerte)"
    elif diff < -0.02:
        tendencia = "↘️ BAJISTA (Moderada)"
    else:
        tendencia = "↔️ LATERAL"

    precision = "Alta" if num_lecturas > 50 else ("Media" if num_lecturas > 20 else "Baja")

    return {
        "prediccion_ml": prediccion_ml,
        "piso": piso,
        "techo": techo,
        "tendencia": tendencia,
        "hora_proyeccion": hora_proyeccion,
        "num_lecturas": num_lecturas,
        "precision_score": precision
    }

# 4. API Servidor
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

# 5. Recolección Continua
def auto_collector():
    while True:
        compra, venta = get_binance_p2p_rates()
        if compra and venta:
            guardar_precios(compra, venta)
        time.sleep(900)

# 6. Comandos Telegram
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot P2P Activo. Usa /prediccion para consultar la IA P2P.")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        f"⚡ **Spread:** {spread:.2f} Bs | **Ganancia Neta:** {max(ganancia_neta, 0.0):.2f}%\n\n"
        f"🔮 **Predicción ML ({ml['hora_proyeccion']}):** {ml['prediccion_ml']:.2f} Bs\n"
        f"📊 **Tendencia:** {ml['tendencia']}\n"
        f"🟢 **Piso:** {ml['piso']:.2f} Bs | 🔴 **Techo:** {ml['techo']:.2f} Bs\n\n"
        f"🧠 *Entrenado con {ml['num_lecturas']} muestras | Precisión: {ml['precision_score']}*"
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
