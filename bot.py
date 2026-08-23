import os
import time
import asyncio
import httpx
import threading
import psycopg2
import sqlite3
import numpy as np
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))
VET = timezone(timedelta(hours=-4))

# Servidor HTTP para Render Health Check
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

async def consultar_ordenes_binance_async(client, trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "publisherType": "user",
        "page": 1,
        "rows": 5,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }
    
    try:
        response = await client.post(url, json=payload, timeout=5.0)
        if response.status_code == 200:
            data = response.json().get('data', [])
            if data:
                precios = [float(item['adv']['price']) for item in data[:3]]
                return round(float(np.mean(precios)), 2)
    except Exception as e:
        print(f"Error consultando Binance ({trade_type} - {monto}): {e}")
    return None

async def get_p2p_rates_async():
    async with httpx.AsyncClient(headers={"Content-Type": "application/json"}) as client:
        # Recompra a 10k VES -> tradeType "SELL"
        # Venta a 300k VES -> tradeType "BUY"
        task_recompra = consultar_ordenes_binance_async(client, "SELL", "10000")
        task_venta = consultar_ordenes_binance_async(client, "BUY", "300000")
        
        tasa_recompra, tasa_venta = await asyncio.gather(task_recompra, task_venta)
        
        if not tasa_recompra or not tasa_venta:
            return None, None, None, None

        spread = round(tasa_venta - tasa_recompra, 2)
        porcentaje_bruto = round((spread / tasa_recompra) * 100, 2)
        
        return tasa_recompra, tasa_venta, spread, porcentaje_bruto

def motor_prediccion_cuantitativo_7h():
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
    
    compras = np.array([h[1] for h in historial])
    ventas = np.array([h[2] for h in historial])
    
    def limpiar_outliers(data):
        q25, q75 = np.percentile(data, 25), np.percentile(data, 75)
        iqr = q75 - q25
        lim_inf, lim_sup = q25 - (1.5 * iqr), q75 + (1.5 * iqr)
        filtrados = data[(data >= lim_inf) & (data <= lim_sup)]
        return filtrados if len(filtrados) > 3 else data

    c_clean = limpiar_outliers(compras)
    v_clean = limpiar_outliers(ventas)

    piso_soporte = round(float(np.min(c_clean)), 2)
    techo_resistencia = round(float(np.max(v_clean)), 2)

    def holt_linear(series, alpha=0.4, beta=0.2, h=7):
        level = series[0]
        trend = series[1] - series[0]
        for i in range(1, len(series)):
            last_level = level
            level = alpha * series[i] + (1 - alpha) * (level + trend)
            trend = beta * (level - last_level) + (1 - beta) * trend
        return level + (trend * h), trend

    steps_7h = 7 * 20
    pred_c_raw, trend_c = holt_linear(c_clean, h=steps_7h)
    pred_v_raw, trend_v = holt_linear(v_clean, h=steps_7h)

    spread_historico_mediana = float(np.median(v_clean - c_clean))
    pred_c_7h = round(float(pred_c_raw), 2)
    pred_v_7h = round(float(max(pred_v_raw, pred_c_7h + spread_historico_mediana)), 2)
    brecha = round(pred_v_7h - pred_c_7h, 2)

    std_dev = float(np.std(c_clean))
    volatilidad_pct = (std_dev / float(np.mean(c_clean))) * 100
    
    trend_score = (trend_c + trend_v) / 2
    if trend_score > 0.05:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif trend_score > 0.01:
        tendencia = "📈 ALCISTA (Moderada)"
    elif trend_score < -0.05:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif trend_score < -0.01:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    if volatilidad_pct > 0.8:
        estado_vol = "⚠️ ALTA (Inestable)"
    elif volatilidad_pct > 0.3:
        estado_vol = "⚡ MODERADA"
    else:
        estado_vol = "🛡️ BAJA (Seguro)"

    return {
        "pred_compra": f"{pred_c_7h:.2f} Bs",
        "pred_venta": f"{pred_v_7h:.2f} Bs",
        "brecha_esperada": f"{brecha:.2f} Bs",
        "tendencia": tendencia,
        "piso": f"{piso_soporte:.2f} Bs",
        "techo": f"{techo_resistencia:.2f} Bs",
        "volatilidad": estado_vol,
        "muestras": n
    }

def background_monitor_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def loop_func():
        while True:
            try:
                c, v, sp, pct = await get_p2p_rates_async()
                if c and v:
                    guardar_lectura(c, v, sp)
            except Exception as e:
                print(f"Error monitor fondo: {e}")
            await asyncio.sleep(180)
            
    loop.run_until_complete(loop_func())

async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = await get_p2p_rates_async()
    pred = motor_prediccion_cuantitativo_7h()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Ocurrió un timeout al consultar Binance P2P. Intenta de nuevo en unos segundos.")
        return

    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🧠 **MONITOR QUANT P2P (No Verificados)**\n"
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
        f"🧠 **Lecturas Validadas:** {pred['muestras']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

if __name__ == "__main__":
    init_db()
    
    threading.Thread(target=run_web_server, daemon=True).start()
    threading.Thread(target=background_monitor_worker, daemon=True).start()
    
    if TELEGRAM_TOKEN:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        app.add_handler(CommandHandler("p2p", prediccion_cmd))
        print("Bot iniciado correctamente...")
        app.run_polling(drop_pending_updates=True)
