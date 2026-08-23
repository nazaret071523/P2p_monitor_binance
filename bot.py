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
# SCRAPING BINANCE P2P - NO VERIFICADOS (FILTRO REVOLVENTE 10K - 300K)
# ==========================================
def obtener_mejor_precio_filtrado(trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "publisherType": "user",  # Exclusivo No Verificados
        "page": 1,
        "rows": 20,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10).json()
        data = r.get('data', [])
        precios = []
        for item in data:
            adv = item.get('adv', {})
            min_single = float(adv.get('minSingleTransAmount', 0))
            max_single = float(adv.get('maxSingleTransAmount', 0))
            price = float(adv.get('price', 0))
            
            # Verificar que el anuncio cubra el espectro comercial real
            if max_single >= 10000 and min_single <= 300000 and price > 0:
                precios.append(price)
                
        if precios:
            return precios[0]  # El primer anuncio más competitivo válido
        elif data:
            return float(data[0]['adv']['price'])
        return None
    except Exception as e:
        print(f"⚠️ Error filtrando Binance P2P ({trade_type}): {e}")
        return None

def get_binance_p2p_rates():
    # Evaluamos con volumen operativo de $50,000 VES para filtrar anomalías de micro-montos
    raw_compra = obtener_mejor_precio_filtrado("BUY", 50000)
    raw_venta = obtener_mejor_precio_filtrado("SELL", 50000)
    
    if not raw_compra or not raw_venta:
        # Fallback a 10k si no hay órdenes de 50k
        raw_compra = obtener_mejor_precio_filtrado("BUY", 10000)
        raw_venta = obtener_mejor_precio_filtrado("SELL", 10000)
        
    if not raw_compra or not raw_venta:
        return None, None, None, None

    # Asignación correcta: Compra (Punta de adquisición comerciante) < Venta (Punta de colocación comerciante)
    compra = min(raw_compra, raw_venta)
    venta = max(raw_compra, raw_venta)
    
    spread = round(venta - compra, 2)
    # Ganancia Neta restando la comisión estándar del 0.50%
    ganancia_pct = round(((venta * 0.9975) - (compra * 1.0025)) / compra * 100, 2)
    
    return compra, venta, spread, ganancia_pct

# ==========================================
# CÁLCULO PREDICTIVO A 7 HORAS Y NIVELES CLAVE
# ==========================================
def calcular_prediccion_ml():
    historial = obtener_historial_horas(24)
    n_muestras = len(historial)
    
    if n_muestras < 3:
        return {
            "soporte_piso": "N/A", "resistencia_techo": "N/A", 
            "pred_compra_7h": "N/A", "pred_venta_7h": "N/A",
            "brecha_proyectada": "N/A",
            "tendencia": "↔️ NEUTRA (Recolectando Datos)",
            "precision": "Inicial",
            "num_lecturas": n_muestras
        }
    
    compras = np.array([h[2] for h in historial])
    ventas = np.array([h[3] for h in historial])
    timestamps = np.array([h[0] for h in historial])
    
    # Soporte (Piso) y Resistencia (Techo) basados en la liquidez acumulada del día
    soporte_piso = round(np.percentile(compras, 15), 2)
    resistencia_techo = round(np.percentile(ventas, 85), 2)
    
    x = timestamps - timestamps[0]
    
    # Pendiente general del mercado usando el promedio del spread
    precios_medios = (compras + ventas) / 2
    if len(np.unique(x)) > 1:
        slope, _ = np.polyfit(x, precios_medios, 1)
    else:
        slope = 0

    compra_act = compras[-1]
    venta_act = ventas[-1]
    spread_actual = venta_act - compra_act
    
    # Proyección a 7 Horas (25,200 segundos) manteniendo la estructura del spread
    centro_7h = ((compra_act + venta_act) / 2) + (slope * 25200)
    
    est_compra_7h = round(centro_7h - (spread_actual / 2), 2)
    est_venta_7h = round(centro_7h + (spread_actual / 2), 2)
    brecha_futura = round(est_venta_7h - est_compra_7h, 2)
    
    pred_compra_str = f"{est_compra_7h:.2f} Bs"
    pred_venta_str = f"{est_venta_7h:.2f} Bs"
    
    cambio_7h = slope * 25200
    if cambio_7h > 1.5:
        tendencia = "📈 ALCISTA (Fuerte)"
    elif cambio_7h > 0.3:
        tendencia = "📈 ALCISTA (Moderada)"
    elif cambio_7h < -1.5:
        tendencia = "📉 BAJISTA (Fuerte)"
    elif cambio_7h < -0.3:
        tendencia = "📉 BAJISTA (Moderada)"
    else:
        tendencia = "↔️ ESTABLE"

    precision = "Alta (24h)" if n_muestras > 100 else ("Media" if n_muestras > 20 else "Inicial")
        
    return {
        "soporte_piso": soporte_piso,
        "resistencia_techo": resistencia_techo,
        "pred_compra_7h": pred_compra_str,
        "pred_venta_7h": pred_venta_str,
        "brecha_proyectada": brecha_futura,
        "tendencia": tendencia,
        "precision": precision,
        "num_lecturas": n_muestras
    }

# ==========================================
# MONITOR EN SEGUNDO PLANO Y ALERTAS
# ==========================================
async def notificar_cambio_tendencia(app, nueva_tendencia):
    msg = f"🚨 **ALERTA DE CAMBIO DE TENDENCIA**\n\nEl mercado P2P (No Verificados) ahora está: **{nueva_tendencia}**."
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
            
            # Enviar alerta automática solo si cambia la tendencia (y no es estable/neutra)
            if "ALCISTA" in pred["tendencia"] or "BAJISTA" in pred["tendencia"]:
                if pred["tendencia"] != ULTIMA_TENDENCIA:
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
            "filtro": "No Verificados | 10K - 300K VES",
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
        "Te has suscrito correctamente a las **alertas automáticas de tendencia**.\n"
        "Usa `/prediccion` para consultar los datos del mercado en tiempo real."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    SUSCRIPTORES.add(update.effective_chat.id)
    compra, venta, spread, ganancia = get_binance_p2p_rates()
    pred = calcular_prediccion_ml()
    
    if not compra:
        await update.message.reply_text("❌ Error conectando con Binance P2P. Intenta de nuevo.")
        return

    hora_actual = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🤖 **MONITOR P2P ML PRO (No Verificados)**\n"
        f"⏰ **Hora VE:** {hora_actual}\n"
        f"🎯 **Filtro:** 10K - 300K VES | **Comisión:** 0.50%\n\n"
        f"🟢 **Tasa Compra Actual:** {compra:.2f} Bs\n"
        f"🔴 **Tasa Venta Actual:** {venta:.2f} Bs\n"
        f"⚡ **Spread Actual:** {spread:.2f} Bs | **Ganancia Neta:** {ganancia:.2f}%\n\n"
        f"🔮 **Proyección Compra (7h):** {pred['pred_compra_7h']}\n"
        f"🔮 **Proyección Venta (7h):** {pred['pred_venta_7h']}\n"
        f"📐 **Brecha Esperada (7h):** {pred['brecha_proyectada']} Bs\n"
        f"📊 **Tendencia:** {pred['tendencia']}\n\n"
        f"🛡️ **Soporte (Piso 24h):** {pred['soporte_piso']} Bs\n"
        f"🏰 **Resistencia (Techo 24h):** {pred['resistencia_techo']} Bs\n\n"
        f"🧠 **Muestras (24h):** {pred['num_lecturas']} | **Precisión:** {pred['precision']}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==========================================
# INICIALIZACIÓN
# ==========================================
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
