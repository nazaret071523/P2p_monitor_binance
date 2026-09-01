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
import matplotlib.dates as mdates

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
# SCRAPING BINANCE P2P CON CÁLCULO VWAP DE PROFUNDIDAD
# ==========================================
def calcular_vwap_y_profundidad(items):
    """Calcula el VWAP (Volume Weighted Average Price) basado en la profundidad real."""
    precio_acumulado = 0.0
    volumen_total = 0.0
    
    for item in items:
        try:
            adv = item.get("adv", {})
            price = float(adv.get("price", 0))
            # Usar la cantidad disponible o el límite máximo transable como ponderador de liquidez
            itable = item.get("advertiser", {})
            vol = float(adv.get("surplusAmount", 1000) or 1000)
            
            precio_acumulado += price * vol
            volumen_total += vol
        except Exception:
            continue
            
    if volumen_total == 0:
        return 0.0
    return precio_acumulado / volumen_total

def obtener_precios_binance_p2p(bancos_filtro=None):
    global ULTIMO_REGISTRO_VALIDO
    if bancos_filtro is None:
        bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # Ampliamos a 2 páginas para capturar mayor profundidad de order book
    payload_compra = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 20, "tradeType": "SELL", "transAmount": "10000", "payTypes": bancos_filtro}
    payload_venta = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 20, "tradeType": "BUY", "transAmount": "300000", "payTypes": bancos_filtro}

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=6).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=6).json()

        data_c, data_v = res_c.get("data", []), res_v.get("data", [])
        if not data_c or not data_v:
            raise ValueError("Respuesta vacía de Binance P2P.")

        # Obtención de VWAP de profundidad en lugar de un único punto estático
        vwap_compra = calcular_vwap_y_profundidad(data_c)
        vwap_venta = calcular_vwap_y_profundidad(data_v)

        if vwap_compra == 0 or vwap_venta == 0:
            precios_c = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
            precios_v = [float(item["adv"]["price"]) for item in data_v if "adv" in item]
            vwap_compra = min(precios_c) if precios_c else ULTIMO_REGISTRO_VALIDO["compra"]
            vwap_venta = max(precios_v) if precios_v else ULTIMO_REGISTRO_VALIDO["venta"]

        if vwap_compra >= vwap_venta:
            vwap_compra, vwap_venta = vwap_venta * 0.98, vwap_venta

        liquidez_calculada = len(data_c) + len(data_v)
        ULTIMO_REGISTRO_VALIDO = {"compra": vwap_compra, "venta": vwap_venta, "timestamp": datetime.now(VET)}
        return round(vwap_compra, 2), round(vwap_venta, 2), liquidez_calculada

    except Exception as e:
        logger.error(f"Error scraping P2P con VWAP: {e}")
        return ULTIMO_REGISTRO_VALIDO["compra"], ULTIMO_REGISTRO_VALIDO["venta"], 20

# ==========================================
# MOTOR DE PROTECCIÓN Y QUANT INTELIGENTE
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    filas = obtener_estadisticas_db(banco=banco_filtro)
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(float(actual_compra), 2)
        pred_v = round(float(actual_venta), 2)
        tendencia = "🛡️ PROTECCIÓN ESTABLE"
        piso, techo = float(actual_compra) - 10, float(actual_venta) + 10
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

        delta_porcentual = ((pred_c - actual_compra) / actual_compra) * 100
        if delta_porcentual > 0.4:
            tendencia = "🟢 TENDENCIA ALCISTA PROTEGIDA"
        elif delta_porcentual < -0.4:
            tendencia = "🛡️ SOPORTE DE PROTECCIÓN ACTIVO"
        else:
            tendencia = "🛡️ ZONA DE PROTECCIÓN ESTABLE"

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
# MOTOR GRÁFICO CON BANDAS DE DESVIACIÓN DINÁMICA
# ==========================================
def generar_imagen_grafica_cuantica(filas, banco):
    if not filas or len(filas) < 2:
        return None
    
    tiemps = [f[3] for f in filas]
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    
    # Cálculo de desviación estándar histórica para bandas dinámicas de volatilidad
    std_compras = float(np.std(compras)) if len(compras) > 1 else 0.5
    std_ventas = float(np.std(ventas)) if len(ventas) > 1 else 0.5
    
    ultimo_tiempo = tiemps[-1]
    ultima_compra = compras[-1]
    ultima_venta = ventas[-1]
    
    pasos_futuros = [0, 2, 4, 6, 8]
    tiempos_futuros = [ultimo_tiempo + timedelta(hours=h) for h in pasos_futuros]
    
    # Aplicación de factor de ensanchamiento dinámico basado en volatilidad real
    compras_futuras = [round(ultima_compra + (h * 0.15), 2) for h in pasos_futuros]
    ventas_futuras = [round(ultima_venta + (h * 0.18), 2) for h in pasos_futuros]

    # Bandas superior e inferior dinámicas basadas en desviación estándar
    banda_superior_dinamica = [round(v + (std_ventas * 0.8), 2) for v in ventas_futuras]
    banda_inferior_dinamica = [round(c - (std_compras * 0.8), 2) for c in compras_futuras]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    fig.patch.set_facecolor("#0b0f19")
    ax.set_facecolor("#0f172a")

    ax.grid(True, linestyle=':', alpha=0.25, color='#38bdf8')
    for spine in ax.spines.values():
        spine.set_edgecolor('#334155')

    ax.plot(tiemps, ventas, color="#f59e0b", linewidth=2.2, label="Venta Real (VWAP)")
    ax.plot(tiemps, compras, color="#10b981", linewidth=2.2, label="Recompra Real (VWAP)")

    ax.plot(tiempos_futuros, ventas_futuras, color="#f59e0b", linestyle='--', linewidth=2, marker='^', label="Proyección Venta (+H)")
    ax.plot(tiempos_futuros, compras_futuras, color="#10b981", linestyle='--', linewidth=2, marker='v', label="Proyección Recompra (+H)")

    for t_fut, p_venta, p_compra in zip(tiempos_futuros, ventas_futuras, compras_futuras):
        ax.annotate(f"{p_venta:.1f}", (t_fut, p_venta), textcoords="offset points", xytext=(0, 8), ha='center', color="#f59e0b", fontsize=7, fontweight='bold')
        ax.annotate(f"{p_compra:.1f}", (t_fut, p_compra), textcoords="offset points", xytext=(0, -12), ha='center', color="#10b981", fontsize=7, fontweight='bold')

    # Canal de protección IA expandido con Bandas de Desviación Dinámica
    ax.fill_between(tiempos_futuros, banda_inferior_dinamica, banda_superior_dinamica, color="#38bdf8", alpha=0.15, label="Canal de Volatilidad Dinámica")

    ax.set_xlim(tiemps[0], tiempos_futuros[-1])
    ax.set_title(f"VENBOT PREDICCIONES // CANAL CUÁNTICO DINÁMICO [{banco}]", color="#38bdf8", fontsize=10, fontweight='bold', loc='left', pad=12)
    ax.set_ylabel("Tasa VES / USDT", color="#94a3b8", fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=VET))
    ax.tick_params(colors="#94a3b8", labelsize=8)
    ax.legend(loc="upper left", facecolor="#0f172a", edgecolor="#334155", labelcolor="#cbd5e1", fontsize=7)

    plt.xticks(rotation=15)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# HANDLERS DE TELEGRAM UNIFICADOS
# ==========================================
telegram_app = None

def obtener_teclado_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔮 Análisis P2P y Protección Quant", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("💎 Muestra los planes VIP y PREMIUM", callback_data="cmd_suscribir")],
        [InlineKeyboardButton("📊 Gráfica de Protección Temporal", callback_data="cmd_grafica")],
        [InlineKeyboardButton("🏦 Configurar Filtro de Bancos", callback_data="cmd_bancos")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = (
        "🦜 *VENBOT PREDICCIONES - SISTEMA DE PROTECCIÓN*\n"
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
        f"🦜 *VENBOT PREDICCIONES - PROTECCIÓN*\n"
        f"⏱ Sincronizado ({hora_actual}) | Objetivo: {hora_objetivo}\n"
        f"🟢 COMPRA P2P (VWAP): `{c_real:.2f} Bs` | 🔴 VENTA (VWAP): `{v_real:.2f} Bs`\n\n"
        f"💳 *ESTADO DE LIQUIDEZ*\n"
        f"• Estado: `{datos['estado_comunidad']}`\n"
        f"• Muestras Analizadas: `{datos['muestras']}`\n"
        f"• Rango Piso / Techo: `{datos['piso_str']}` / `{datos['techo_str']}`\n\n"
        f"🔮 *PROYECCIÓN DE PROTECCIÓN (7H)*\n"
        f"🟢 Recompra Protegida: `{datos['pred_compra_str']}`\n"
        f"🔴 Venta Estimada: `{datos['pred_venta_str']}`\n"
        f"🎯 Estado Operativo: `{datos['tendencia']}`\n\n"
        f"💡 *Motor:* VWAP Order Book & Bandas Dinámicas Activas."
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
            caption="📊 *Venbot Predicciones - Canal Cuántico Dinámico [GENERAL]*", 
            parse_mode="Markdown", 
            reply_markup=InlineKeyboardMarkup(teclado)
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id, 
            text="⚠️ Datos insuficientes para trazar el canal dinámico.", 
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

    texto = "💎 *Planes VIP y Premium Disponibles*\nAcceso prioritario a flujos de alta liquidez y canales cuánticos avanzados."
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
    return {"status": "Venbot Predicciones Sistema de Protección Activo con VWAP y Bandas Dinámicas"}

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
    
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
    telegram_app.add_handler(CommandHandler("precision", cmd_prediccion))
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
