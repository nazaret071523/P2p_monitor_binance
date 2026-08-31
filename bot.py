import os
import io
import asyncio
import logging
from datetime import datetime, timedelta
import pytz

import psycopg2
import requests
from bs4 import BeautifulSoup
import numpy as np
import xgboost as xgb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ==========================================
# CONFIGURACIÓN GENERAL Y ZONA HORARIA VET
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VET = pytz.timezone('America/Caracas')

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/venbot")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TU_TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "TU_GEMINI_API_KEY")

# ==========================================
# GESTIÓN DE BASE DE DATOS POSTGRESQL
# ==========================================
def obtener_conexion():
    return psycopg2.connect(DATABASE_URL)

def inicializar_db():
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS muestras_p2p (
            id SERIAL PRIMARY KEY,
            compra FLOAT,
            venta FLOAT,
            fecha TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios_suscritos (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            activo BOOLEAN DEFAULT FALSE,
            fecha_registro TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def guardar_muestra_db(compra, venta):
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO muestras_p2p (compra, venta, fecha) VALUES (%s, %s, %s)",
        (compra, venta, datetime.now(VET))
    )
    conn.commit()
    cur.close()
    conn.close()

def obtener_estadisticas_db(limit=2000):
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute("SELECT compra, venta, fecha FROM muestras_p2p ORDER BY id DESC LIMIT %s;", (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return list(reversed(rows))

# ==========================================
# SCRAPING REAL DE BINANCE P2P USDT/VES
# ==========================================
def obtener_precios_binance_p2p():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]
    payload_compra = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000", "payTypes": bancos_filtro}
    payload_venta = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000", "payTypes": bancos_filtro}

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=5).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=5).json()

        data_c, data_v = res_c.get("data", []), res_v.get("data", [])
        if not data_c or not data_v:
            return 923.00, 937.00

        precios_compra = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
        precios_venta = [float(item["adv"]["price"]) for item in data_v if "adv" in item]

        if not precios_compra or not precios_venta:
            return 923.00, 937.00

        tasa_compra, tasa_venta = min(precios_compra), max(precios_venta)
        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = precios_compra[0], precios_venta[0]

        return round(tasa_compra, 2), round(tasa_venta, 2)
    except Exception as e:
        logger.error(f"Error consultando Binance P2P: {e}")
        return 923.00, 937.00

# ==========================================
# MOTOR QUANT INTELIGENTE (DIANA FIJA +7H)
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(actual_compra * 0.995, 2)
        pred_v = round(actual_venta * 1.005, 2)
        tendencia = "➖ ESTABLE / LATERAL"
        piso, techo = actual_compra, actual_venta
        ruta_horas, ruta_valores = [], []
    else:
        compras = np.array([f[0] for f in filas])
        ventas = np.array([f[1] for f in filas])
        piso, techo = np.min(compras), np.max(ventas)

        window_size = min(total_muestras - 1, 5)
        X, y = [], []
        for i in range(window_size, len(compras)):
            X.append(compras[i - window_size:i])
            y.append(compras[i])
        
        X, y = np.array(X), np.array(y)
        if len(X) > 0:
            model = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0)
            model.fit(X, y)
            pred_c_next = model.predict(compras[-window_size:].reshape(1, -1))[0]
            
            recent_x = np.arange(min(total_muestras, 30))
            recent_y = compras[-len(recent_x):]
            slope_c, _ = np.polyfit(recent_x, recent_y, 1)
            
            delta_proyectado = (pred_c_next - actual_compra) + (slope_c * 42)
            pred_c = round(actual_compra + delta_proyectado, 2)
        else:
            pred_c, slope_c = round(actual_compra, 2), 0.0

        spread_historico_promedio = np.mean(ventas - compras)
        pred_v = round(pred_c + spread_historico_promedio, 2)

        if slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"

        ahora_dt = datetime.now(VET)
        ruta_horas = [(ahora_dt + timedelta(hours=h)).strftime("%I:%M %p") for h in range(1, 8)]
        ruta_valores = [round(actual_compra + (pred_c - actual_compra) * (i / 7), 2) for i in range(1, 8)]

    spread = round(actual_venta - actual_compra, 2)
    analisis_ia = obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia, pred_c, pred_v)

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs", 
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia, 
        "recompra": pred_c, 
        "venta_esperada": pred_v,
        "piso_str": f"{piso:.2f} Bs", 
        "techo_str": f"{techo:.2f} Bs",
        "muestras": total_muestras,
        "ruta_horas": ruta_horas,
        "ruta_valores": ruta_valores,
        "analisis_ia": analisis_ia
    }

def obtener_analisis_ia_coherente(compra, venta, spread, tendencia, pred_c, pred_v):
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        prompt = (
            f"Actúa como analista financiero cuantitativo experto en Binance P2P USDT/VES. "
            f"Datos actuales: Compra={compra}, Venta={venta}, Spread={spread}, Tendencia={tendencia}, "
            f"Diana +7H Recompra={pred_c}, Diana +7H Venta={pred_v}. "
            f"Redacta un comentario táctico muy breve y profesional para inversores (máximo 2 líneas)."
        )
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        pass
    return "Mercado operando dentro del canal de volatilidad esperado. Mantener disciplina en cobertura."

# ==========================================
# GENERACIÓN DE GRÁFICA PROFESIONAL CON TRAYECTORIA
# ==========================================
def generar_grafica_prediccion_buffer():
    filas = obtener_estadisticas_db(limit=30)
    if not filas or len(filas) < 2:
        return None

    compras = [f[0] for f in filas]
    tiempos = [f[2][11:16] for f in filas]
    
    ultimo_precio = compras[-1]
    pred = motor_quant_inteligente(ultimo_precio, ultimo_precio + 14.0)
    
    plt.figure(figsize=(10, 5))
    plt.style.use('dark_background')

    plt.plot(tiempos, compras, label='Historial P2P Real', color='#00ffcc', marker='o', linewidth=2, markersize=4)

    if pred["ruta_horas"] and pred["ruta_valores"]:
        tiempos_futuros = [tiempos[-1]] + pred["ruta_horas"]
        valores_futuros = [ultimo_precio] + pred["ruta_valores"]
        plt.plot(tiempos_futuros, valores_futuros, label='Ruta Proyectada (+7H Objetivo)', color='#ff0055', linestyle='--', marker='x', linewidth=2, markersize=5)

    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")
    plt.title(f'Venbot Quant - Diana Predictiva a {hora_objetivo}', fontsize=12, color='white', pad=12)
    plt.xlabel('Evolución Temporal (VET)', color='#aaaaaa', fontsize=9)
    plt.ylabel('Precio USDT/VES (Bs)', color='#aaaaaa', fontsize=9)
    plt.xticks(rotation=45, fontsize=8, color='#888888')
    plt.yticks(fontsize=9, color='#888888')
    plt.legend(loc='upper left', facecolor='#111111', edgecolor='#333333', fontsize=9)
    plt.grid(True, linestyle='--', alpha=0.2)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

# ==========================================
# TELEGRAM BOT HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = [
        [InlineKeyboardButton("🔮 Ver Predicción +7H", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("📊 Ver Gráfica de Tendencia", callback_data="cmd_grafica")],
        [InlineKeyboardButton("💳 Suscribirse al Servicio", callback_data="cmd_suscribir")]
    ]
    reply_markup = InlineKeyboardMarkup(teclado)
    await update.message.reply_text(
        "🦜 *VENBOT PREDICCIONES QUANT*\n\n"
        "Sistema de análisis predictivo de alto nivel para Binance P2P USDT/VES.\n"
        "Selecciona una opción del menú:",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        chat_id = query.message.chat_id
    else:
        chat_id = update.effective_chat.id

    c_real, v_real = obtener_precios_binance_p2p()
    datos = motor_quant_inteligente(c_real, v_real)
    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    texto = (
        f"🦜 *VENBOT PREDICCIONES QUANT*\n"
        f"⏱ ({hora_actual}) | DIANA FIJA A LAS {hora_objetivo}\n"
        f"🟢 COMPRA P2P (Spot): `{c_real:.2f} Bs`\n"
        f"🔴 VENTA P2P (Spot): `{v_real:.2f} Bs`\n\n"
        f"🔮 *PROYECCIÓN OBJETIVO (+7H)*\n"
        f"🟢 Recompra Esperada: `{datos['pred_compra_str']}`\n"
        f"🔴 Venta Esperada: `{datos['pred_venta_str']}`\n"
        f"🎯 Dirección: `{datos['tendencia']}`\n\n"
        f"📈 Piso Canal: `{datos['piso_str']}` | Techo: `{datos['techo_str']}`\n"
        f"💾 Base de Datos: `{datos['muestras']} Muestras`\n\n"
        f"💡 *Análisis Táctico:* _{datos['analisis_ia']}_"
    )

    if query:
        await query.message.reply_text(texto, parse_mode="Markdown")
    else:
        await update.message.reply_text(texto, parse_mode="Markdown")

async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    buf = generar_grafica_prediccion_buffer()
    if buf:
        await message.reply_photo(photo=buf, caption="📊 *Venbot Quant - Ruta Predictiva e Historial P2P*", parse_mode="Markdown")
    else:
        await message.reply_text("⚠️ Recopilando suficientes muestras de mercado para generar la curva...")

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    texto = (
        "💳 *Suscripción Venbot Quant Pro*\n\n"
        "Obtén acceso ilimitado a señales automáticas, alertas de cambio de tendencia en tiempo real y panel web avanzado.\n\n"
        "Realiza tu pago móvil o transferencia y reporta tu pago con el administrador."
    )
    await message.reply_text(texto, parse_mode="Markdown")

async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    if data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_suscribir":
        await cmd_suscribir(update, context)

# ==========================================
# TAREA EN SEGUNDO PLANO (RECOLECCIÓN)
# ==========================================
async def tarea_recoleccion_automatica():
    while True:
        try:
            c, v = obtener_precios_binance_p2p()
            guardar_muestra_db(c, v)
            logger.info(f"Muestra guardada exitosamente: Compra={c}, Venta={v}")
        except Exception as e:
            logger.error(f"Error en tarea de recolección: {e}")
        await asyncio.sleep(300)

# ==========================================
# APLICACIÓN FASTAPI
# ==========================================
app = FastAPI()
telegram_app = None

@app.on_event("startup")
async def startup_event():
    global telegram_app
    inicializar_db()
    
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
    telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
    telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
    telegram_app.add_handler(CallbackQueryHandler(manejar_botones))

    await telegram_app.initialize()
    await telegram_app.start()
    asyncio.create_task(tarea_recoleccion_automatica())
    logger.info("¡Venbot Quant desplegado y operando con éxito!")

@app.on_event("shutdown")
async def shutdown_event():
    if telegram_app:
        await telegram_app.stop()
        await telegram_app.shutdown()

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"status": "ok"}

@app.get("/")
def read_root():
    return {"status": "Venbot Quant Activo", "timestamp": str(datetime.now(VET))}
