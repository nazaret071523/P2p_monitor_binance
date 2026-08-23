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

# Servidor Web para mantener Render activo
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Venbot Quant Engine Active")

def run_web_server():
    try:
        server = HTTPServer(('0.0.0.0', PORT), SimpleHTTPRequestHandler)
        server.serve_forever()
    except Exception as e:
        print(f"Error Web Server: {e}")

# Base de Datos
def get_db_connection():
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception:
            pass
    return sqlite3.connect("p2p_data.db")

def init_db():
    try:
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
    except Exception as e:
        print(f"Error Init DB: {e}")

def guardar_lectura(compra, venta, spread):
    try:
        now_ve = datetime.now(VET)
        conn = get_db_connection()
        cursor = conn.cursor()
        q = "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (%s, %s, %s, %s, %s)" if DATABASE_URL else "INSERT INTO lecturas (timestamp, fecha_hora, compra, venta, spread) VALUES (?, ?, ?, ?, ?)"
        cursor.execute(q, (now_ve.timestamp(), now_ve.strftime("%Y-%m-%d %H:%M:%S"), compra, venta, spread))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error guardando lectura: {e}")

def obtener_historial(horas=24):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        ts_limite = (datetime.now(VET) - timedelta(hours=horas)).timestamp()
        q = "SELECT timestamp, compra, venta, spread FROM lecturas WHERE timestamp >= %s ORDER BY timestamp ASC" if DATABASE_URL else "SELECT timestamp, compra, venta, spread FROM lecturas WHERE timestamp >= ? ORDER BY timestamp ASC"
        cursor.execute(q, (ts_limite,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"Error obteniendo historial: {e}")
        return []

# Consulta Directa a API Binance (Filtro Estricto No Verificados + Banco Mercantil)
def consultar_binance_top1(trade_type, monto, banco="SpecificBank"):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "rows": 20,
        "tradeType": trade_type,
        "transAmount": str(monto),
        "payTypes": [banco] if banco != "ALL" else []
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=6) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            
            for item in data:
                adv = item.get('adv', {})
                advertiser = item.get('advertiser', {})
                
                # Ignorar Anuncios Promocionados o de comerciantes verificados
                is_promoted = adv.get('isPromoted', False)
                user_type = advertiser.get('userType')
                user_type_badge = advertiser.get('userStatsRet', {}).get('userType')
                
                # 'user' es el rol único de usuario NO verificado
                if user_type == "user" and not is_promoted:
                    return round(float(adv['price']), 2)
                    
    except Exception as e:
        print(f"Error consultando Binance Top1 ({trade_type}): {e}")
    return None

def get_p2p_rates():
    # VENDER USDT en Binance -> Tasa Recompra (10K VES)
    tasa_recompra = consultar_binance_top1("SELL", "10000", banco="SpecificBank")
    
    # COMPRAR USDT en Binance -> Tasa Venta (300K VES)
    tasa_venta = consultar_binance_top1("BUY", "300000", banco="SpecificBank")
    
    if not tasa_recompra or not tasa_venta:
        return None, None, None, None

    spread = round(tasa_venta - tasa_recompra, 2)
    pct_bruto = round((spread / tasa_recompra) * 100, 2)
    return tasa_recompra, tasa_venta, spread, pct_bruto

# Motor Cuantitativo Holt Suavizado
def motor_quant_top1_7h():
    historial = obtener_historial(24)
    n = len(historial)
    
    if n < 5:
        return {
            "pred_compra": "Calibrando...",
            "pred_venta": "Calibrando...",
            "brecha_esperada": "N/A",
            "tendencia": "↔️ NEUTRA (Recolectando)",
            "piso": "N/A", "techo": "N/A", "volatilidad": "Baja", "muestras": n
        }
    
    compras = [h[1] for h in historial]
    ventas = [h[2] for h in historial]
    
    piso = round(min(compras), 2)
    techo = round(max(ventas), 2)

    def damped_holt(series, alpha=0.35, beta=0.1, phi=0.8, steps=140):
        level = series[0]
        trend = series[1] - series[0] if len(series) > 1 else 0
        for i in range(1, len(series)):
            last_level = level
            level = alpha * series[i] + (1 - alpha) * (level + phi * trend)
            trend = beta * (level - last_level) + (1 - beta) * phi * trend
            
        damped_sum = sum(phi ** k for k in range(1, steps + 1)) * trend
        return level + damped_sum, trend

    pred_c_raw, trend_c = damped_holt(compras, steps=140)
    pred_v_raw, trend_v = damped_holt(ventas, steps=140)
    
    pred_c_7h = round(max(piso * 0.98, min(techo * 1.02, pred_c_raw)), 2)
    pred_v_7h = round(max(pred_c_7h + 0.5, pred_v_raw), 2)
    brecha = round(pred_v_7h - pred_c_7h, 2)

    prom_c = sum(compras) / n
    var_c = sum((x - prom_c) ** 2 for x in compras) / n
    std_c = math.sqrt(var_c)
    vol_pct = (std_c / prom_c) * 100

    avg_trend = (trend_c + trend_v) / 2
    if avg_trend > 0.005:
        tendencia = "🚀 ALCISTA (Fuerte)"
    elif avg_trend > 0.001:
        tendencia = "📈 ALCISTA (Moderada)"
    elif avg_trend < -0.005:
        tendencia = "🔻 BAJISTA (Fuerte)"
    elif avg_trend < -0.001:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE / LATERAL"

    estado_vol = "⚠️ ALTA" if vol_pct > 0.8 else ("⚡ MODERADA" if vol_pct > 0.3 else "🛡️ BAJA")

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
            print(f"Error monitor de fondo: {e}")
        time.sleep(180)

# Handler Telegram
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasa_compra, tasa_venta, spread, pct_bruto = get_p2p_rates()
    pred = motor_quant_top1_7h()
    
    if not tasa_compra or not tasa_venta:
        await update.message.reply_text("❌ Error temporal consultando la API de Binance P2P.")
        return

    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P TOP 1 (No Verificados - Mercantil)**\n"
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
        print("Bot en vivo e iniciado...")
        app.run_polling(drop_pending_updates=True)
