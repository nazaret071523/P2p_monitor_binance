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

# Servidor HTTP para Render
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Venbot Quant Engine Live")

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
                # Ponderación por volumen de las 3 mejores ofertas
                prices = [float(x['adv']['price']) for x in data[:3]]
                return round(sum(prices) / len(prices), 2)
    except Exception as e:
        print(f"Error consultando Binance Native: {e}")
    return None

def get_p2p_rates():
    # Anuncio COMPRA (Recompra @ 10K VES) -> tradeType SELL
    tasa_recompra = consultar_binance_native("SELL", "10000")
    # Anuncio VENTA (Vender @ 300K VES) -> tradeType BUY
    tasa_venta = consultar_binance_native("BUY", "300000")
    
    if not tasa_recompra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)
    return tasa_recompra, tasa_venta, spread, pct_bruto

def calcular_mediana(lista):
    sorted_lst = sorted(lista)
    lst_len = len(sorted_lst)
    index = (lst_len - 1) // 2
    if lst_len % 2:
        return sorted_lst[index]
    else:
        return (sorted_lst[index] + sorted_lst[index + 1]) / 2.0

def motor_prediccion_avanzado_7h():
    historial = obtener_historial(24)
    n = len(historial)
    
    if n < 6:
        return {
            "pred_compra": "Recolectando datos...",
            "pred_venta": "Recolectando datos...",
            "brecha_esperada": "N/A",
            "tendencia": "↔️ NEUTRA (Calibrando)",
            "piso": "N/A", "techo": "N/A", "volatilidad": "Baja", "muestras": n
        }
    
    compras = [h[1] for h in historial]
    ventas = [h[2] for h in historial]
    
    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)
    
    # 1. Filtro MAD (Median Absolute Deviation) para limpiar anomalías
    mediana_c = calcular_mediana(compras)
    difs_c = [abs(x - mediana_c) for x in compras]
    mad_c = calcular_mediana(difs_c) or 0.01
    c_clean = [x for x in compras if abs(x - mediana_c) / mad_c <= 3.5]
    if not c_clean: c_clean = compras

    mediana_v = calcular_mediana(ventas)
    difs_v = [abs(x - mediana_v) for x in ventas]
    mad_v = calcular_mediana(difs_v) or 0.01
    v_clean = [x for x in ventas if abs(x - mediana_v) / mad_v <= 3.5]
    if not v_clean: v_clean = ventas

    # 2. Modelo Holt's Exponential Smoothing (Tendencia Doble)
    def holt_predict(series, alpha=0.35, beta=0.15, steps=140):
        level = series[0]
        trend = series[1] - series[0] if len(series) > 1 else 0
        for i in range(1, len(series)):
            last_level = level
            level = alpha * series[i] + (1 - alpha) * (level + trend)
            trend = beta * (level - last_level) + (1 - beta) * trend
        return level + (trend * steps), trend

    # 140 lecturas equivalen a 7 horas (1 lectura cada 3 minutos)
    pred_c_raw, trend_c = holt_predict(c_clean, steps=140)
    pred_v_raw, trend_v = holt_predict(v_clean, steps=140)

    # Mediana del spread histórico para asegurar coherencia
    spreads_clean = [v - c for c, v in zip(compras, ventas)]
    mediana_spread = max(0.5, calcular_mediana(spreads_clean))

    pred_c_7h = round(pred_c_raw, 2)
    pred_v_7h = round(max(pred_v_raw, pred_c_7h + mediana_spread), 2)
    brecha = round(pred_v_7h - pred_c_7h, 2)

    # 3. Cálculo de Volatilidad
    promedio_c = sum(c_clean) / len(c_clean)
    varianza = sum((x - promedio_c) ** 2 for x in c_clean) / len(c_clean)
    desviacion = math.sqrt(varianza)
    volatilidad_pct = (desviacion / promedio_c) * 100

    avg_trend = (trend_c + trend_v) / 2
    if avg_trend > 0.015:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif avg_trend > 0.003:
        tendencia = "📈 ALCISTA (Moderada)"
    elif avg_trend < -0.015:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif avg_trend < -0.003:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    if volatilidad_pct > 0.8:
        estado_vol = "⚠️ ALTA"
    elif volatilidad_pct > 0.3:
        estado_vol = "⚡ MODERADA"
    else:
        estado_vol = "🛡️ BAJA"

    return {
        "pred_compra": f"{pred_c_7h:.2f} Bs",
        "pred_venta": f"{pred_v_7h:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso:.2f} Bs",
        "techo": f"{techo:.2f} Bs",
        "volatilidad": estado_vol,
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
    pred = motor_prediccion_avanzado_7h()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error temporal consultando Binance P2P.")
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
        print("Bot en vivo...")
        app.run_polling(drop_pending_updates=True)
