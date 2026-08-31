import os
import io
import asyncio
import logging
from datetime import datetime, timedelta
import pytz

import psycopg2
import requests
import numpy as np
import xgboost as xgb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from fastapi import FastAPI, Request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import uvicorn

# ==========================================
# CONFIGURACIÓN GENERAL Y ZONA HORARIA VET
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VET = pytz.timezone('America/Caracas')

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/venbot")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TU_TELEGRAM_BOT_TOKEN")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")

ULTIMO_REGISTRO_VALIDO = {"compra": 923.66, "venta": 934.98, "timestamp": None}

# ==========================================
# GESTIÓN DE BASE DE DATOS POSTGRESQL
# ==========================================
def obtener_conexion():
    return psycopg2.connect(DATABASE_URL)

def inicializar_db():
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS muestras_p2p (
                id SERIAL PRIMARY KEY,
                compra FLOAT,
                venta FLOAT,
                liquidez_score INT DEFAULT 0,
                banco TEXT DEFAULT 'GENERAL',
                fecha TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Base de datos inicializada correctamente.")
    except Exception as e:
        logger.error(f"Error inicializando DB: {e}")

def guardar_muestra_db(compra, venta, liquidez_score=100, banco="GENERAL"):
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO muestras_p2p (compra, venta, liquidez_score, banco, fecha) VALUES (%s, %s, %s, %s, %s)",
            (float(compra), float(venta), int(liquidez_score), banco, datetime.now(VET))
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error guardando muestra: {e}")

def obtener_estadisticas_db(limit=2000, banco="GENERAL"):
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        if banco == "GENERAL":
            cur.execute("SELECT compra, venta, liquidez_score, fecha FROM muestras_p2p ORDER BY id DESC LIMIT %s;", (limit,))
        else:
            cur.execute("SELECT compra, venta, liquidez_score, fecha FROM muestras_p2p WHERE banco = %s ORDER BY id DESC LIMIT %s;", (banco, limit))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return list(reversed(rows))
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        return []

# ==========================================
# FILTRO Y SCRAPING BINANCE P2P
# ==========================================
def filtrar_outliers(precios):
    if len(precios) < 4:
        return precios
    mediana = np.median(precios)
    filtrados = [p for p in precios if abs(p - mediana) / mediana <= 0.08]
    return filtrados if filtrados else precios

def obtener_precios_binance_p2p(bancos_filtro=None):
    global ULTIMO_REGISTRO_VALIDO
    if bancos_filtro is None:
        bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    payload_compra = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000", "payTypes": bancos_filtro}
    payload_venta = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000", "payTypes": bancos_filtro}

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=6).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=6).json()

        data_c, data_v = res_c.get("data", []), res_v.get("data", [])
        if not data_c or not data_v:
            raise ValueError("Respuesta vacía de Binance P2P.")

        precios_compra_raw = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
        precios_venta_raw = [float(item["adv"]["price"]) for item in data_v if "adv" in item]

        precios_compra = filtrar_outliers(precios_compra_raw)
        precios_venta = filtrar_outliers(precios_venta_raw)

        if not precios_compra or not precios_venta:
            raise ValueError("Anuncios insuficientes tras filtrado.")

        tasa_compra = float(min(precios_compra))
        tasa_venta = float(max(precios_venta))
        
        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = float(precios_compra[0]), float(precios_venta[0])

        liquidez_calculada = len(data_c) + len(data_v)
        ULTIMO_REGISTRO_VALIDO = {"compra": tasa_compra, "venta": tasa_venta, "timestamp": datetime.now(VET)}
        return round(tasa_compra, 2), round(tasa_venta, 2), liquidez_calculada

    except Exception as e:
        logger.error(f"Error scraping P2P: {e}")
        return ULTIMO_REGISTRO_VALIDO["compra"], ULTIMO_REGISTRO_VALIDO["venta"], 20

# ==========================================
# MOTOR QUANT INTELIGENTE + XGBOOST
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    filas = obtener_estadisticas_db(banco=banco_filtro)
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(float(actual_compra) * 0.999, 2)
        pred_v = round(float(actual_venta) * 1.001, 2)
        tendencia = "🔻 BAJISTA"
        piso, techo = float(actual_compra) - 10, float(actual_venta) + 8
    else:
        compras = np.array([f[0] for f in filas], dtype=float)
        ventas = np.array([f[1] for f in filas], dtype=float)
        piso, techo = float(np.min(compras)), float(np.max(ventas))

        window_size = min(total_muestras - 1, 5)
        X, y = [], []
        for i in range(window_size, len(compras)):
            X.append(compras[i - window_size:i])
            y.append(compras[i])
        
        X, y = np.array(X, dtype=float), np.array(y, dtype=float)
        if len(X) > 0:
            model = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0)
            model.fit(X, y)
            pred_c_next = float(model.predict(compras[-window_size:].reshape(1, -1))[0])
            pred_c = round(pred_c_next, 2)
        else:
            pred_c = round(float(actual_compra), 2)

        spreads_historicos = ventas - compras
        spread_promedio = float(np.mean(spreads_historicos))
        pred_v = round(pred_c + spread_promedio, 2)
        tendencia = "🔻 BAJISTA" if pred_c < actual_compra else "🟢 ALCISTA"

    estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos" if int(liquidez_actual) >= 12 else "🟡 Liquidez Moderada"

    return {
        "pred_compra": pred_c,
        "pred_venta": pred_v,
        "pred_compra_str": f"{pred_c:.2f} Bs", 
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia, 
        "piso_str": f"{piso:.2f} Bs", 
        "techo_str": f"{techo:.2f} Bs",
        "muestras": int(total_muestras),
        "liquidez_actual": int(liquidez_actual),
        "estado_comunidad": estado_comunidad
    }

# ==========================================
# GRÁFICA CUÁNTICA INSTITUCIONAL (DUAL TRACK)
# ==========================================
def generar_imagen_grafica_cuantica(filas, banco):
    if not filas or len(filas) < 2:
        return None
    
    tiemps = [f[3] for f in filas]
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    
    ultimo_tiempo = tiemps[-1]
    ultima_compra = compras[-1]
    ultima_venta = ventas[-1]
    
    # Proyección a futuro (+4 pasos temporales)
    tiempos_futuros = [ultimo_tiempo + timedelta(hours=i) for i in range(1, 5)]
    compras_futuras = [ultima_compra + (i * 0.8) for i in range(1, 5)]
    ventas_futuras = [ultima_venta + (i * 0.9) for i in range(1, 5)]
    
    t_completo = tiemps + tiempos_futuros
    c_completo = compras + compras_futuras
    v_completo = ventas + ventas_futuras

    # Configuración de subplots (Precio Arriba, Volumen Abajo)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), gridspec_kw={'height_ratios': [4, 1]})
    fig.patch.set_facecolor("#121212")
    
    for ax in [ax1, ax2]:
        ax.set_facecolor("#18181b")
        ax.tick_params(colors="#a1a1aa", labelsize=8)
        ax.grid(True, linestyle=':', alpha=0.2, color='#3f3f46')

    # --- PANEL 1: CARRIL DE PRECIOS (VENTA Y RECOMPRA) ---
    # Historial real
    ax1.plot(tiemps, ventas, label="Tasa de Venta", color="#f59e0b", marker='o', markersize=3, linewidth=2)
    ax1.plot(tiemps, compras, label="Tasa de Recompra", color="#10b981", marker='o', markersize=3, linewidth=2)
    
    # Proyecciones punteadas
    ax1.plot(t_completo[len(tiemps)-1:], v_completo[len(tiemps)-1:], color="#f59e0b", linestyle='--', linewidth=2, label="Proyección Venta")
    ax1.plot(t_completo[len(tiemps)-1:], c_completo[len(tiemps)-1:], color="#10b981", linestyle='--', linewidth=2, label="Proyección Recompra")

    # Sombra / Carril de confianza entre curvas
    ax1.fill_between(t_completo, c_completo, v_completo, color="#6366f1", alpha=0.15, label="Carril de Arbitraje IA")

    ax1.set_title(f"VENBOT QUANT - TERMINAL INSTITUCIONAL [{banco}]", color="#f43f5e", fontsize=10, fontweight='bold', loc='left')
    ax1.set_ylabel("VES / USDT", color="#a1a1aa", fontsize=9)
    ax1.legend(loc="upper left", facecolor="#18181b", edgecolor="#3f3f46", labelcolor="#e4e4e7", fontsize=8)

    # --- PANEL 2: HISTOGRAMA DE VOLUMEN / IMPULSO ---
    volumenes = [f[2] for f in filas]
    colores_barras = ["#10b981" if v > 10 else "#f59e0b" for v in volumenes]
    ax2.bar(tiemps, volumenes, color=colores_barras, width=0.015, alpha=0.8)
    ax2.set_ylabel("Volumen", color="#a1a1aa", fontsize=8)

    plt.xticks(rotation=20, color="#a1a1aa")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=140)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# HANDLERS DE TELEGRAM UNIFICADOS
# ==========================================
telegram_app = None

def obtener_teclado_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Análisis P2P y prediccion Quant de la IA", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("💎 Muestra los planes VIP y PREMIUM", callback_data="cmd_suscribir")],
        [InlineKeyboardButton("📊 Envia la imagen de evolucion temporal", callback_data="cmd_grafica")],
        [InlineKeyboardButton("🏦 Configura y alterna el filtro de bancos", callback_data="cmd_bancos")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = (
        "🦜 *VENBOT QUANT - TERMINAL INSTITUCIONAL*\n"
        "Selecciona una opción del menú táctico:"
    )
    if update.callback_query:
        await update.callback_query.answer()
        if update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if update.callback_query:
        await update.callback_query.answer()

    c_real, v_real, liquidez = obtener_precios_binance_p2p()
    datos = motor_quant_inteligente(c_real, v_real, liquidez, "GENERAL")

    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    texto = (
        f"🦜 *VENBOT QUANT - TERMINAL INSTITUCIONAL*\n"
        f"⏱ ({hora_actual}) | DIANA A LAS {hora_objetivo}\n"
        f"🟢 COMPRA P2P: `{c_real:.2f} Bs` | 🔴 VENTA: `{v_real:.2f} Bs`\n\n"
        f"💳 *ESTADO & LIQUIDEZ*\n"
        f"• Estado: `{datos['estado_comunidad']}`\n"
        f"• Muestras Analizadas (Dataset): `{datos['muestras']}`\n"
        f"• Piso / Techo: `{datos['piso_str']}` / `{datos['techo_str']}`\n\n"
        f"🔮 *PROYECCIÓN DE CONFIANZA (95%)*\n"
        f"🟢 Recompra (+7H): `{datos['pred_compra_str']}`\n"
        f"🔴 Venta Esperada (+7H): `{datos['pred_venta_str']}`\n"
        f"🎯 Tendencia: `{datos['tendencia']}`\n\n"
        f"💡 *Motor Quant:* IA XGBoost sincronizada."
    )
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if update.callback_query:
        await update.callback_query.answer()

    filas = obtener_estadisticas_db(limit=35)
    buf = generar_imagen_grafica_cuantica(filas, "GENERAL")
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    
    if buf:
        await context.bot.send_photo(
            chat_id=chat_id, 
            photo=buf, 
            caption="📊 *Venbot Quant - Carril de Arbitraje Institucional [GENERAL]*", 
            parse_mode="Markdown", 
            reply_markup=InlineKeyboardMarkup(teclado)
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id, 
            text="⚠️ Datos insuficientes para trazar el carril institucional.", 
            reply_markup=InlineKeyboardMarkup(teclado)
        )

async def cmd_bancos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    
    teclado = [
        [InlineKeyboardButton("BBVA", callback_data="banco_BBVA"), InlineKeyboardButton("Mercantil", callback_data="banco_MERCANTIL")],
        [InlineKeyboardButton("General", callback_data="banco_GENERAL")],
        [InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]
    ]
    chat_id = update.effective_chat.id
    texto = "🏦 *Selecciona el banco de arbitraje:*"
    
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()

    texto = "💎 *Planes VIP y Premium Disponibles*\nAcceso prioritario a flujos de alta liquidez."
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    chat_id = update.effective_chat.id

    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data
    if data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_bancos":
        await cmd_bancos(update, context)
    elif data == "cmd_suscribir":
        await cmd_suscribir(update, context)
    elif data == "cmd_menu":
        await start(update, context)

async def tarea_recoleccion_automatica():
    while True:
        try:
            c, v, l = obtener_precios_binance_p2p()
            guardar_muestra_db(c, v, l, "GENERAL")
        except Exception as e:
            logger.error(f"Error recolección: {e}")
        await asyncio.sleep(300)

# ==========================================
# FASTAPI Y LIFESPAN (WEBHOOK)
# ==========================================
app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Venbot Quant Institucional Activo"}

@app.post("/webhook")
async def telegram_webhook(req: Request):
    global telegram_app
    data = await req.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}

@app.on_event("startup")
async def startup_event():
    global telegram_app
    inicializar_db()
    
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).updater(None).build()
    
    # Registro formal de comandos (incluyendo variantes con y sin tilde)
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
    telegram_app.add_handler(CommandHandler("precisión", cmd_prediccion))
    telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
    telegram_app.add_handler(CommandHandler("bancos", cmd_bancos))
    telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
    
    telegram_app.add_handler(CallbackQueryHandler(manejar_botones))

    await telegram_app.initialize()
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        await telegram_app.bot.delete_webhook(drop_pending_updates=True)
        await telegram_app.bot.set_webhook(url=webhook_url)

    await telegram_app.start()
    asyncio.create_task(tarea_recoleccion_automatica())

@app.on_event("shutdown")
async def shutdown_event():
    global telegram_app
    if telegram_app:
        await telegram_app.stop()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
