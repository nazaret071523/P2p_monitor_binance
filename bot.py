import os
import json
import threading
import http.server
import socketserver
import urllib.request
import statistics
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = "8579313357:AAE3_PCgfY2zmpkVJWIz8gA4ECeDBufoct4"

# Memoria temporal para almacenar el historial de precios en vivo
HISTORIAL_PRECIOS = []

def run_dummy_server():
    port = int(os.environ.get("PORT", 8080))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        httpd.serve_forever()

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
                precio_promedio = round(sum(prices) / len(prices), 2)
                
                # Guardar en la base de datos histórica
                HISTORIAL_PRECIOS.append(precio_promedio)
                if len(HISTORIAL_PRECIOS) > 50:
                    HISTORIAL_PRECIOS.pop(0)
                    
                return precio_promedio
    except Exception as e:
        print(f"Error consultando Binance: {e}")

    return None

def calcular_modelo_ml(precio_actual):
    """
    Simulador de Machine Learning basado en Promedios Móviles y Volatilidad Histórica
    """
    if len(HISTORIAL_PRECIOS) < 3:
        # Volatilidad base si aún hay pocas lecturas registradas
        volatilidad = precio_actual * 0.008  # ~0.8% de margen
        prediccion = precio_actual + (volatilidad * 0.5)
    else:
        # Análisis de tendencia con medias móviles
        media_corta = statistics.mean(HISTORIAL_PRECIOS[-3:])
        media_larga = statistics.mean(HISTORIAL_PRECIOS)
        desviacion = statistics.stdev(HISTORIAL_PRECIOS) if len(HISTORIAL_PRECIOS) > 2 else precio_actual * 0.005
        
        # Inercia de tendencia
        tendencia_inercial = media_corta - media_larga
        prediccion = precio_actual + tendencia_inercial + (desviacion * 0.2)
        volatilidad = max(desviacion * 1.5, precio_actual * 0.005)

    piso = round(precio_actual - volatilidad, 2)
    techo = round(precio_actual + volatilidad, 2)
    prediccion_final = round(prediccion, 2)

    return prediccion_final, piso, techo

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot Activo. Usa /prediccion para consultar precios en vivo.")

async def prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    precio_actual = get_binance_p2p_price()
    
    if not precio_actual:
        await update.message.reply_text("⚠️ No se pudo obtener la tasa en vivo de Binance. Intenta en unos segundos.")
        return

    prediccion_ml, piso, techo = calcular_modelo_ml(precio_actual)
    hora_proyeccion = (datetime.now() + timedelta(hours=3)).strftime("%H:%M")
    
    if prediccion_ml > precio_actual:
        tendencia = "📈 ALCISTA"
    elif prediccion_ml < precio_actual:
        tendencia = "📉 BAJISTA"
    else:
        tendencia = "↔️ LATERAL / ESTABLE"

    lecturas = len(HISTORIAL_PRECIOS)

    mensaje = (
        f"🤖 **PREDICCIÓN MACHINE LEARNING**\n"
        f"⏰ Proyección para: {hora_proyeccion}\n\n"
        f"📌 **Precio Actual:** {precio_actual:.2f} Bs\n"
        f"🎯 **Predicción ML:** {prediccion_ml:.2f} Bs\n"
        f"📊 **Tendencia Estimada:** {tendencia}\n\n"
        f"🟢 **Piso Calculado:** {piso:.2f} Bs\n"
        f"🔴 **Techo Calculado:** {techo:.2f} Bs\n\n"
        f"🧠 *Modelado con indicadores en vivo ({lecturas} lecturas acumuladas).* "
    )
    
    await update.message.reply_text(mensaje, parse_mode="Markdown")

if __name__ == "__main__":
    threading.Thread(target=run_dummy_server, daemon=True).start()
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", prediccion))
    app.run_polling()
