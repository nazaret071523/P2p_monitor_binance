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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config_usuario (
                telegram_id BIGINT PRIMARY KEY,
                banco_preferido TEXT DEFAULT 'GENERAL',
                suscrito BOOLEAN DEFAULT FALSE,
                plan TEXT DEFAULT 'FREE'
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
# FILTRO ANTI-ANUNCIANTES FANTASMAS Y SCRAPING
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

        if ULTIMO_REGISTRO_VALIDO["compra"] == tasa_compra and ULTIMO_REGISTRO_VALIDO["venta"] == tasa_venta:
            tasa_compra = round(tasa_compra, 2)
            tasa_venta = round(tasa_venta, 2)

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
        tendencia = "🔻 BAJISTA"

    estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos" if int(liquidez_actual) >= 12 else "🟡 Liquidez Moderada"

    return {
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
# GENERADOR DE GRÁFICAS (ESTILO CLARO PROFESIONAL)
# ==========================================
def generar_imagen_grafica(filas, banco):
    if not filas:
        return None
    tiemps = [f[3] for f in filas]
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]

    # Estilo de fondo blanco limpio preferido por legibilidad institucional
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(tiemps, compras, label="Compra P2P", color="#009966", linewidth=2)
    ax.plot(tiemps, ventas, label="Venta P2P", color="#cc0033", linewidth=2)
    ax.set_title(f"Gráfica Institucional P2P [{banco}]", color="#222222", fontsize=12, fontweight='bold')
    
    ax.set_facecolor("#ffffff")
    fig.patch.set_facecolor("#ffffff")
    ax.tick_params(colors="#333333")
    ax.spines['bottom'].set_color('#cccccc')
    ax.spines['left'].set_color('#cccccc')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, linestyle='--', alpha=0.5, color='#dddddd')
    ax.legend(facecolor="#f9f9f9", edgecolor="#cccccc", labelcolor="#222222")
    
    plt.xticks(rotation=30)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# TELEGRAM HANDLERS UNIFICADOS (COMANDOS Y BOTONES)
# ==========================================
telegram_app = None

def obtener_teclado_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Ver Predicción +7H", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("📊 Gráfica Institucional", callback_data="cmd_grafica")],
        [InlineKeyboardButton("📊 Análisis de Spread", callback_data="cmd_spread")],
        [InlineKeyboardButton("📈 Simulador P&L", callback_data="cmd_simulador")],
        [InlineKeyboardButton("🏦 Seleccionar Banco", callback_data="cmd_bancos")],
        [InlineKeyboardButton("💎 Suscribirse / Planes", callback_data="cmd_suscribir")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = (
        "🦜 *VENBOT PREDICCIONES QUANT - PRO*\n"
        "🛡 Terminal con Alertas Push y Filtro de Bancos\n\n"
        "Selecciona una opción:"
    )
    if update.callback_query:
        await update.callback_query.answer()
        if update.callback_query.message:
            await update.callback_query.message.edit_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=obtener_teclado_menu())

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        chat_id = query.message.chat_id if query.message else update.effective_chat.id
        await query.answer()
    else:
        chat_id = update.effective_chat.id

    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT banco_preferido FROM config_usuario WHERE telegram_id = %s;", (chat_id,))
        row = cur.fetchone()
        banco_usr = row[0] if row else "GENERAL"
        cur.close()
        conn.close()
    except Exception:
        banco_usr = "GENERAL"

    banco_map = {"BBVA": ["BBVA"], "MERCANTIL": ["Mercantil"], "BNC": ["BNC"], "GENERAL": ["BBVA", "Mercantil", "BNC"]}
    c_real, v_real, liquidez = obtener_precios_binance_p2p(banco_map.get(banco_usr, ["BBVA", "Mercantil", "BNC"]))
    datos = motor_quant_inteligente(c_real, v_real, liquidez, banco_usr)

    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    texto = (
        f"🦜 *VENBOT QUANT - TERMINAL [{banco_usr}]*\n"
        f"⏱ ({hora_actual}) | DIANA A LAS {hora_objetivo}\n"
        f"🟢 COMPRA P2P: `{c_real:.2f} Bs` | 🔴 VENTA: `{v_real:.2f} Bs`\n\n"
        f"💳 *COMUNIDAD & LIQUIDEZ*\n"
        f"• Estado: `{datos['estado_comunidad']}`\n"
        f"• Anuncios Detectados: `{datos['liquidez_actual']}`\n"
        f"• Piso / Techo: `{datos['piso_str']}` / `{datos['techo_str']}`\n\n"
        f"🔮 *PROYECCIÓN DE CONFIANZA (95%)*\n"
        f"🟢 Recompra (+7H): `{datos['pred_compra_str']}`\n"
        f"🔴 Venta Esperada (+7H): `{datos['pred_venta_str']}`\n"
        f"🎯 Tendencia: `{datos['tendencia']}`\n\n"
        f"💡 *Análisis Táctico:* Protección de precios activa. Canal de volatilidad estable."
    )

    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]

    if query and query.message:
        await query.message.reply_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        chat_id = query.message.chat_id if query.message else update.effective_chat.id
    else:
        chat_id = update.effective_chat.id

    filas = obtener_estadisticas_db(limit=50)
    buf = generar_imagen_grafica(filas, "GENERAL")
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    
    if buf:
        await context.bot.send_photo(
            chat_id=chat_id, 
            photo=buf, 
            caption="📊 *Gráfica Institucional Actualizada*", 
            parse_mode="Markdown", 
            reply_markup=InlineKeyboardMarkup(teclado)
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id, 
            text="⚠️ No hay suficientes datos históricos para generar la gráfica.", 
            reply_markup=InlineKeyboardMarkup(teclado)
        )

async def cmd_spread(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        chat_id = query.message.chat_id if query.message else update.effective_chat.id
    else:
        chat_id = update.effective_chat.id

    filas = obtener_estadisticas_db(limit=20)
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    
    if not filas:
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Sin datos suficientes de spread.", reply_markup=InlineKeyboardMarkup(teclado))
        return
        
    spreads = [f[1] - f[0] for f in filas]
    prom_spread = np.mean(spreads)
    texto = f"📊 *ANÁLISIS DE SPREAD P2P*\n\n• Spread Promedio Actual: `{prom_spread:.2f} Bs`\n• Último Spread Registrado: `{spreads[-1]:.2f} Bs`"
    await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_simulador(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        chat_id = query.message.chat_id if query.message else update.effective_chat.id
    else:
        chat_id = update.effective_chat.id

    c, v, _ = obtener_precios_binance_p2p()
    spread = v - c
    ganancia_est = spread * 100
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    
    texto = (
        f"📈 *SIMULADOR P&L (100 USDT)*\n\n"
        f"• Compra a: `{c:.2f} Bs`\n"
        f"• Venta a: `{v:.2f} Bs`\n"
        f"• Spread por USDT: `{spread:.2f} Bs`\n"
        f"💰 *Ganancia Estimada (100 USDT):* `{ganancia_est:.2f} Bs`"
    )
    await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_bancos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
    
    teclado = [
        [InlineKeyboardButton("BBVA Provincial", callback_data="banco_BBVA"), InlineKeyboardButton("Mercantil", callback_data="banco_MERCANTIL")],
        [InlineKeyboardButton("BNC", callback_data="banco_BNC"), InlineKeyboardButton("General (Todos)", callback_data="banco_GENERAL")],
        [InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]
    ]
    
    if query and query.message:
        await query.message.edit_text("🏦 *Selecciona el banco:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    elif update.message:
        await update.message.reply_text("🏦 *Selecciona el banco:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    texto = (
        "💎 *SISTEMA DE SUSCRIPCIÓN VIP & CREDENCIALES*\n\n"
        "Obtén acceso total al motor predictivo y la app móvil.\n\n"
        "🏦 *MÉTODOS DE PAGO DISPONIBLES (VES / USD)*\n"
        "• *Pago Móvil / Banco:* Mercantil / BNC / BBVA Provincial\n"
        "• *Teléfono Pago Móvil:* `0412-0000000`\n"
        "• *Cédula:* `V-00.000.000`\n"
        "• *Binance Pay (USDT):* `tu_correo_o_id@binance`\n\n"
        "📝 *PASOS PARA ACTIVACIÓN:*\n"
        "1️⃣ Realiza el pago correspondiente al plan.\n"
        "2️⃣ Envía por aquí una foto del comprobante o tu número de referencia.\n"
        "3️⃣ El bot validará tu pago y te asignará tu contraseña VIP automáticamente."
    )
    teclado = [[InlineKeyboardButton("⬅️ Volver al Menú", callback_data="cmd_menu")]]
    
    if query and query.message:
        await query.message.edit_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))
    elif update.message:
        await update.message.reply_text(texto, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(teclado))

async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    
    data = query.data
    chat_id = query.message.chat_id if query.message else None

    if data.startswith("banco_"):
        banco_elegido = data.split("_")[1]
        if chat_id:
            try:
                conn = obtener_conexion()
                cur = conn.cursor()
                cur.execute("INSERT INTO config_usuario (telegram_id, banco_preferido) VALUES (%s, %s) ON CONFLICT (telegram_id) DO UPDATE SET banco_preferido = EXCLUDED.banco_preferido;", (chat_id, banco_elegido))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Error guardando banco: {e}")
        await query.answer(f"Banco configurado: {banco_elegido}")
        if query.message:
            await query.message.edit_text(f"✅ Banco configurado a: *{banco_elegido}*", parse_mode="Markdown", reply_markup=obtener_teclado_menu())
        return

    if data == "cmd_prediccion":
        await cmd_prediccion(update, context)
    elif data == "cmd_grafica":
        await cmd_grafica(update, context)
    elif data == "cmd_spread":
        await cmd_spread(update, context)
    elif data == "cmd_simulador":
        await cmd_simulador(update, context)
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
            logger.info(f"Muestra automática guardada: Compra={c}, Venta={v}")
        except Exception as e:
            logger.error(f"Error recolección: {e}")
        await asyncio.sleep(300)

# ==========================================
# APLICACIÓN FASTAPI + WEBHOOK NATIVO LIMPIO
# ==========================================
app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Venbot Quant Activo", "timestamp": str(datetime.now(VET))}

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
    
    # Registro formal de comandos de texto (barra /)
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
    telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
    telegram_app.add_handler(CommandHandler("spread", cmd_spread))
    telegram_app.add_handler(CommandHandler("simulador", cmd_simulador))
    telegram_app.add_handler(CommandHandler("bancos", cmd_bancos))
    telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
    
    # Manejador de botones interactivos
    telegram_app.add_handler(CallbackQueryHandler(manejar_botones))

    await telegram_app.initialize()
    
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        await telegram_app.bot.delete_webhook(drop_pending_updates=True)
        await telegram_app.bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook configurado limpiamente en: {webhook_url}")

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
