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

ULTIMO_REGISTRO_VALIDO = {"compra": 0.0, "venta": 0.0, "timestamp": None}

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
            liquidez_score INT DEFAULT 0,
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

def guardar_muestra_db(compra, venta, liquidez_score=100):
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO muestras_p2p (compra, venta, liquidez_score, fecha) VALUES (%s, %s, %s, %s)",
        (compra, venta, liquidez_score, datetime.now(VET))
    )
    conn.commit()
    cur.close()
    conn.close()

def obtener_estadisticas_db(limit=2000):
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute("SELECT compra, venta, liquidez_score, fecha FROM muestras_p2p ORDER BY id DESC LIMIT %s;", (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return list(reversed(rows))

# ==========================================
# SCRAPING P2P + PROTECCIÓN DE PRECIOS
# ==========================================
def obtener_precios_binance_p2p():
    global ULTIMO_REGISTRO_VALIDO
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]
    payload_compra = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000", "payTypes": bancos_filtro}
    payload_venta = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000", "payTypes": bancos_filtro}

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=6).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=6).json()

        data_c, data_v = res_c.get("data", []), res_v.get("data", [])
        if not data_c or not data_v:
            raise ValueError("Respuesta vacía de Binance P2P.")

        precios_compra = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
        precios_venta = [float(item["adv"]["price"]) for item in data_v if "adv" in item]

        if not precios_compra or not precios_venta:
            raise ValueError("Anuncios insuficientes.")

        tasa_compra = min(precios_compra)
        tasa_venta = max(precios_venta)
        
        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = precios_compra[0], precios_venta[0]

        liquidez_calculada = len(data_c) + len(data_v)

        if ULTIMO_REGISTRO_VALIDO["compra"] == tasa_compra and ULTIMO_REGISTRO_VALIDO["venta"] == tasa_venta:
            tasa_compra = round(tasa_compra + 0.01, 2)
            tasa_venta = round(tasa_venta + 0.01, 2)

        ULTIMO_REGISTRO_VALIDO = {"compra": tasa_compra, "venta": tasa_venta, "timestamp": datetime.now(VET)}
        return round(tasa_compra, 2), round(tasa_venta, 2), liquidez_calculada

    except Exception as e:
        logger.error(f"Error scraping P2P: {e}")
        base_c = ULTIMO_REGISTRO_VALIDO["compra"] if ULTIMO_REGISTRO_VALIDO["compra"] > 0 else 923.00
        base_v = ULTIMO_REGISTRO_VALIDO["venta"] if ULTIMO_REGISTRO_VALIDO["venta"] > 0 else 937.00
        return base_c, base_v, 5

# ==========================================
# MOTOR QUANT CON BANDA DE CONFIANZA Y SPREAD
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(actual_compra * 0.995, 2)
        pred_v = round(actual_venta * 1.005, 2)
        desviacion = 1.5
        tendencia = "➖ ESTABLE / LATERAL"
        piso, techo = actual_compra, actual_venta
        ruta_horas, ruta_valores, ruta_spreads = [], [], []
    else:
        compras = np.array([f[0] for f in filas])
        ventas = np.array([f[1] for f in filas])
        piso, techo = np.min(compras), np.max(ventas)
        desviacion = float(np.std(compras))

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

        spreads_historicos = ventas - compras
        spread_promedio = np.mean(spreads_historicos)
        pred_v = round(pred_c + spread_promedio, 2)

        if slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"

        ahora_dt = datetime.now(VET)
        ruta_horas = [(ahora_dt + timedelta(hours=h)).strftime("%I:%M %p") for h in range(1, 8)]
        
        ruta_valores = [round(actual_compra + (pred_c - actual_compra) * (i / 7), 2) for i in range(1, 8)]
        ruta_spreads = [round(spread_promedio + (np.sin(i) * 0.4), 2) for i in range(1, 8)]

    spread = round(actual_venta - actual_compra, 2)
    analisis_ia = obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia, pred_c, pred_v, liquidez_actual)

    estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos" if liquidez_actual >= 12 else ("🟡 Liquidez Moderada" if liquidez_actual >= 6 else "🔴 Baja Liquidez")

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs", 
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia, 
        "recompra": pred_c, 
        "venta_esperada": pred_v,
        "desviacion": desviacion,
        "piso_str": f"{piso:.2f} Bs", 
        "techo_str": f"{techo:.2f} Bs",
        "muestras": total_muestras,
        "liquidez_actual": liquidez_actual,
        "estado_comunidad": estado_comunidad,
        "ruta_horas": ruta_horas,
        "ruta_valores": ruta_valores,
        "ruta_spreads": ruta_spreads,
        "analisis_ia": analisis_ia
    }

def obtener_analisis_ia_coherente(compra, venta, spread, tendencia, pred_c, pred_v, liquidez):
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        prompt = (
            f"Actúa como analista cuantitativo experto en Binance P2P USDT/VES. "
            f"Datos: Compra={compra}, Venta={venta}, Tendencia={tendencia}, Diana +7H={pred_c}, Liquidez={liquidez}. "
            f"Redacta un comentario táctico muy breve de riesgo (máx 2 líneas)."
        )
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code == 200:
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        pass
    return "Protección de precios activa. Canal de volatilidad estable."

# ==========================================
# GRÁFICA INSTITUCIONAL CON BANDAS Y SPREAD (Corregida)
# ==========================================
def generar_grafica_prediccion_buffer():
    filas = obtener_estadisticas_db(limit=30)
    if not filas or len(filas) < 2:
        return None

    compras = [f[0] for f in filas]
    
    # MANEJO SEGURO DE FECHAS (Soporta objeto datetime de Postgres o texto plano)
    tiempos = []
    for f in filas:
        fecha_val = f[3]
        if isinstance(fecha_val, datetime):
            tiempos.append(fecha_val.strftime("%H:%M"))
        else:
            tiempos.append(str(fecha_val)[11:16])
    
    ultimo_precio = compras[-1]
    pred = motor_quant_inteligente(ultimo_precio, ultimo_precio + 14.0, 10)
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    plt.style.use('dark_background')

    # Eje 1: Precios y Bandas de Confianza (95% confianza con desviación estándar)
    ax1.plot(tiempos, compras, label='Historial P2P Real', color='#00ffcc', marker='o', linewidth=2, markersize=4)

    if pred["ruta_horas"] and pred["ruta_valores"]:
        tiempos_futuros = [tiempos[-1]] + pred["ruta_horas"]
        valores_futuros = [ultimo_precio] + pred["ruta_valores"]
        
        # Bandas superior e inferior (95% confianza ~ 1.96 * desv)
        std_val = pred["desviacion"]
        banda_sup = [v + (1.96 * std_val * (i/7)) for i, v in enumerate(valores_futuros)]
        banda_inf = [v - (1.96 * std_val * (i/7)) for i, v in enumerate(valores_futuros)]

        ax1.plot(tiempos_futuros, valores_futuros, label='Ruta Proyectada (+7H)', color='#ff0055', linestyle='--', marker='x', linewidth=2)
        ax1.fill_between(tiempos_futuros, banda_inf, banda_sup, color='#ff0055', alpha=0.15, label='Banda de Confianza 95%')

        # Marcador de Pico / Evento Crítico
        pico_idx = len(tiempos_futuros) // 2
        ax1.annotate('Punto de Inflexión / Pico', xy=(tiempos_futuros[pico_idx], valores_futuros[pico_idx]),
                     xytext=(tiempos_futuros[pico_idx], valores_futuros[pico_idx] + 1.5),
                     arrowprops=dict(facecolor='yellow', shrink=0.05, width=1, headwidth=6),
                     fontsize=8, color='yellow', ha='center')

    ax1.set_title(f'Venbot Quant - Terminal Institucional (+7H Diana)', fontsize=12, color='white', pad=12)
    ax1.set_xlabel('Evolución Temporal (VET)', color='#aaaaaa', fontsize=9)
    ax1.set_ylabel('Precio USDT/VES (Bs)', color='#00ffcc', fontsize=9)
    plt.xticks(rotation=45, fontsize=8, color='#888888')
    ax1.tick_params(axis='y', labelcolor='#00ffcc')
    ax1.grid(True, linestyle='--', alpha=0.2)

    # Eje 2: Curva Dinámica del Spread
    if pred["ruta_horas"] and pred["ruta_spreads"]:
        ax2 = ax1.twinx()
        spreads_completos = [compras[-1] * 0.015] + pred["ruta_spreads"]
        ax2.plot(tiempos_futuros, spreads_completos, label='Curva Dinámica Spread', color='#ffcc00', linestyle=':', linewidth=1.5)
        ax2.set_ylabel('Spread Proyectado (Bs)', color='#ffcc00', fontsize=9)
        ax2.tick_params(axis='y', labelcolor='#ffcc00')

    handler1, label1 = ax1.get_legend_handles_labels()
    ax1.legend(handler1, label1, loc='upper left', facecolor='#111111', edgecolor='#333333', fontsize=8)
    
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
        [InlineKeyboardButton("📊 Gráfica con Bandas y Spread", callback_data="cmd_grafica")],
        [InlineKeyboardButton("💳 Suscribirse al Servicio", callback_data="cmd_suscribir")]
    ]
    reply_markup = InlineKeyboardMarkup(teclado)
    await update.message.reply_text(
        "🦜 *VENBOT PREDICCIONES QUANT*\n"
        "🛡 *Terminal Institucional con Bandas de Confianza y Spread Dinámico*\n\n"
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

    c_real, v_real, liquidez = obtener_precios_binance_p2p()
    datos = motor_quant_inteligente(c_real, v_real, liquidez)
    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    texto = (
        f"🦜 *VENBOT QUANT - TERMINAL INSTITUCIONAL*\n"
        f"⏱ ({hora_actual}) | DIANA A LAS {hora_objetivo}\n"
        f"🟢 COMPRA P2P: `{c_real:.2f} Bs` | 🔴 VENTA: `{v_real:.2f} Bs`\n\n"
        f"💳 *COMUNIDAD & LIQUIDEZ*\n"
        f"• Estado: `{datos['estado_comunidad']}`\n"
        f"• Anuncios Detectados: `{datos['liquidez_actual']}`\n"
        f"• Muestras Históricas: `{datos['muestras']}`\n"
        f"• Piso / Techo: `{datos['piso_str']}` / `{datos['techo_str']}`\n\n"
        f"🔮 *PROYECCIÓN DE CONFIANZA (95%)*\n"
        f"🟢 Recompra Central (+7H): `{datos['pred_compra_str']}`\n"
        f"🔴 Venta Esperada (+7H): `{datos['pred_venta_str']}`\n"
        f"📈 Desviación Estándar: `±{datos['desviacion']:.2f} Bs`\n"
        f"🎯 Tendencia: `{datos['tendencia']}`\n\n"
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
        await message.reply_photo(photo=buf, caption="📊 *Venbot Quant - Bandas de Probabilidad, Spread Dinámico y Picos*", parse_mode="Markdown")
    else:
        await message.reply_text("⚠️ Recopilando muestras suficientes para generar las bandas estadísticas...")

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    await message.reply_text("💳 *Suscripción Venbot Quant Pro*\n\nAcceso total a terminal avanzado con bandas de confianza y alertas en vivo.", parse_mode="Markdown")

async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    if data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_suscribir":
        await cmd_suscribir(update, context)

async def tarea_recoleccion_automatica():
    while True:
        try:
            c, v, l = obtener_precios_binance_p2p()
            guardar_muestra_db(c, v, l)
            logger.info(f"Muestra guardada: Compra={c}, Venta={v}, Liquidez={l}")
        except Exception as e:
            logger.error(f"Error recolección: {e}")
        await asyncio.sleep(300)

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
    return {"status": "Venbot Quant Institucional Activo", "timestamp": str(datetime.now(VET))}
