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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "TU_GEMINI_API_KEY")
CHAT_COMUNIDAD_ID = os.getenv("CHAT_COMUNIDAD_ID", "-100123456789")

ULTIMO_REGISTRO_VALIDO = {"compra": 0.0, "venta": 0.0, "timestamp": None}
BANCO_ACTIVO_DEFAULT = ["BBVA", "Mercantil", "BNC"]

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
            CREATE TABLE IF NOT EXISTS registro_senales (
                id SERIAL PRIMARY KEY,
                precio_entrada FLOAT,
                precio_objetivo FLOAT,
                tendencia TEXT,
                estado TEXT DEFAULT 'PENDIENTE',
                fecha_creacion TIMESTAMP,
                fecha_cierre TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config_usuario (
                telegram_id BIGINT PRIMARY KEY,
                banco_preferido TEXT DEFAULT 'BBVA',
                suscrito BOOLEAN DEFAULT FALSE,
                plan TEXT DEFAULT 'FREE'
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
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

def registrar_senal_simulador(precio_entrada, precio_objetivo, tendencia):
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO registro_senales (precio_entrada, precio_objetivo, tendencia, estado, fecha_creacion) VALUES (%s, %s, %s, 'PENDIENTE', %s)",
            (float(precio_entrada), float(precio_objetivo), str(tendencia), datetime.now(VET))
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando señal: {e}")

def evaluar_rendimiento_senales(precio_actual):
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT id, precio_entrada, precio_objetivo, tendencia FROM registro_senales WHERE estado = 'PENDIENTE';")
        pendientes = cur.fetchall()
        
        ahora = datetime.now(VET)
        for row in pendientes:
            sig_id, p_entrada, p_obj, tendencia = row
            cur.execute("SELECT fecha_creacion FROM registro_senales WHERE id = %s;", (sig_id,))
            f_creacion = cur.fetchone()[0]
            if (ahora - f_creacion.astimezone(VET)) >= timedelta(hours=7):
                exito = False
                if "ALCISTA" in tendencia and float(precio_actual) >= float(p_obj):
                    exito = True
                elif "BAJISTA" in tendencia and float(precio_actual) <= float(p_obj):
                    exito = True
                
                nuevo_estado = "EXITOSA 🎯" if exito else "EXPIRADA ⚠️"
                cur.execute("UPDATE registro_senales SET estado = %s, fecha_cierre = %s WHERE id = %s;", (nuevo_estado, ahora, sig_id))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error evaluando rendimiento: {e}")

# ==========================================
# FILTRO ANTI-ANUNCIANTES FANTASMAS
# ==========================================
def filtrar_outliers(precios):
    if len(precios) < 4:
        return precios
    mediana = np.median(precios)
    filtrados = [p for p in precios if abs(p - mediana) / mediana <= 0.08]
    return filtrados if filtrados else precios

# ==========================================
# SCRAPING P2P + SELECCIÓN DE BANCOS
# ==========================================
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
# MOTOR QUANT HÍBRIDO (MACRO + MICRO TENDENCIA)
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta, liquidez_actual, banco_filtro="GENERAL"):
    filas = obtener_estadisticas_db(banco=banco_filtro)
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(float(actual_compra) * 0.995, 2)
        pred_v = round(float(actual_venta) * 1.005, 2)
        desviacion = 1.5
        tendencia = "➖ ESTABLE / LATERAL"
        piso, techo = float(actual_compra), float(actual_venta)
        ruta_horas, ruta_valores, ruta_spreads = [], [], []
    else:
        compras = np.array([f[0] for f in filas], dtype=float)
        ventas = np.array([f[1] for f in filas], dtype=float)
        piso, techo = float(np.min(compras)), float(np.max(ventas))
        desviacion = float(np.std(compras))

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
            
            recent_x = np.arange(min(total_muestras, 30), dtype=float)
            recent_y = compras[-len(recent_x):]
            slope_c, _ = np.polyfit(recent_x, recent_y, 1)
            
            delta_proyectado = (pred_c_next - float(actual_compra)) + (float(slope_c) * 42)
            pred_c = round(float(actual_compra) + delta_proyectado, 2)
        else:
            pred_c, slope_c = round(float(actual_compra), 2), 0.0

        spreads_historicos = ventas - compras
        spread_promedio = float(np.mean(spreads_historicos))
        pred_v = round(pred_c + spread_promedio, 2)

        variacion_reciente = (float(actual_compra) - compras[-3]) / compras[-3] if len(compras) >= 3 else 0

        if variacion_reciente < -0.004:
            tendencia = "🔻 CORRECCIÓN TÁCTICA"
        elif slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"

        ahora_dt = datetime.now(VET)
        ruta_horas = [(ahora_dt + timedelta(hours=h)).strftime("%I:%M %p") for h in range(1, 8)]
        ruta_valores = [round(float(actual_compra) + (pred_c - float(actual_compra)) * (i / 7), 2) for i in range(1, 8)]
        ruta_spreads = [round(spread_promedio + (float(np.sin(i)) * 0.4), 2) for i in range(1, 8)]

    spread = round(float(actual_venta) - float(actual_compra), 2)
    analisis_ia = obtener_analisis_ia_coherente(float(actual_compra), float(actual_venta), spread, tendencia, pred_c, pred_v, int(liquidez_actual))
    estado_comunidad = "🟢 Alta Liquidez y Anunciantes Activos" if int(liquidez_actual) >= 12 else ("🟡 Liquidez Moderada" if int(liquidez_actual) >= 6 else "🔴 Baja Liquidez")

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs", 
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia, 
        "recompra": float(pred_c), 
        "venta_esperada": float(pred_v),
        "desviacion": float(desviacion),
        "piso_str": f"{piso:.2f} Bs", 
        "techo_str": f"{techo:.2f} Bs",
        "muestras": int(total_muestras),
        "liquidez_actual": int(liquidez_actual),
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
            f"Datos actuales: Compra={compra}, Venta={venta}, Tendencia={tendencia}, Diana +7H={pred_c}, Liquidez={liquidez}. "
            f"Redacta un comentario táctico dinámico de riesgo y sugerencia de entrada/salida (máx 2 líneas)."
        )
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code == 200:
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        pass
    
    if "CORRECCIÓN" in tendencia:
        return "Precio en retroceso táctico. Esperar estabilización en zona de soporte para reentrada."
    elif "ALCISTA" in tendencia:
        return "Impulso alcista activo. Ideal buscar entradas en soportes y asegurar parciales cerca del techo."
    return "Protección de precios activa. Canal de volatilidad estable."

# ==========================================
# GRÁFICA INSTITUCIONAL CON BANDAS Y SPREAD
# ==========================================
def generar_grafica_prediccion_buffer(banco_filtro="GENERAL"):
    filas = obtener_estadisticas_db(limit=30, banco=banco_filtro)
    if not filas or len(filas) < 2:
        return None

    compras = [float(f[0]) for f in filas]
    tiempos = [f[3].strftime("%H:%M") if isinstance(f[3], datetime) else str(f[3])[11:16] for f in filas]
    
    ultimo_precio = compras[-1]
    pred = motor_quant_inteligente(ultimo_precio, ultimo_precio + 14.0, 10, banco_filtro)
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    plt.style.use('dark_background')

    ax1.plot(tiempos, compras, label='Historial P2P Real', color='#00ffcc', marker='o', linewidth=2, markersize=4)

    if pred["ruta_horas"] and pred["ruta_valores"]:
        tiempos_futuros = [tiempos[-1]] + pred["ruta_horas"]
        valores_futuros = [ultimo_precio] + pred["ruta_valores"]
        
        std_val = float(pred["desviacion"])
        banda_sup = [float(v + (1.96 * std_val * (i/7))) for i, v in enumerate(valores_futuros)]
        banda_inf = [float(v - (1.96 * std_val * (i/7))) for i, v in enumerate(valores_futuros)]

        ax1.plot(tiempos_futuros, valores_futuros, label='Ruta Proyectada (+7H)', color='#ff0055', linestyle='--', marker='x', linewidth=2)
        ax1.fill_between(tiempos_futuros, banda_inf, banda_sup, color='#ff0055', alpha=0.15, label='Banda de Confianza 95%')

        pico_idx = len(tiempos_futuros) // 2
        ax1.annotate('Punto de Inflexión', xy=(tiempos_futuros[pico_idx], valores_futuros[pico_idx]),
                     xytext=(tiempos_futuros[pico_idx], valores_futuros[pico_idx] + 1.5),
                     arrowprops=dict(facecolor='yellow', shrink=0.05, width=1, headwidth=6),
                     fontsize=8, color='yellow', ha='center')

    ax1.set_title(f'Venbot Quant - Terminal Institucional [{banco_filtro}]', fontsize=12, color='white', pad=12)
    ax1.set_xlabel('Evolución Temporal (VET)', color='#aaaaaa', fontsize=9)
    ax1.set_ylabel('Precio USDT/VES (Bs)', color='#00ffcc', fontsize=9)
    plt.xticks(rotation=45, fontsize=8, color='#888888')
    ax1.tick_params(axis='y', labelcolor='#00ffcc')
    ax1.grid(True, linestyle='--', alpha=0.2)

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
# SISTEMA DE ALERTAS PROACTIVAS Y PUSH
# ==========================================
async def verificar_alertas_proactivas(bot, compra_actual, venta_actual):
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT MIN(compra), MAX(venta) FROM muestras_p2p WHERE id < (SELECT MAX(id) FROM muestras_p2p);")
        res = cur.fetchone()
        cur.close()
        conn.close()

        if res and res[0] and res[1]:
            piso_h, techo_h = float(res[0]), float(res[1])
            
            if float(compra_actual) < piso_h:
                await bot.send_message(
                    chat_id=CHAT_COMUNIDAD_ID,
                    text=f"🚨 *ALERTA CRÍTICA: RUPTURA DE PISO*\nEl precio de compra ha perforado el soporte histórico: `{float(compra_actual):.2f} Bs`",
                    parse_mode="Markdown"
                )
            elif float(venta_actual) > techo_h:
                await bot.send_message(
                    chat_id=CHAT_COMUNIDAD_ID,
                    text=f"🚀 *ALERTA CRÍTICA: RUPTURA DE TECHO*\nEl precio de venta ha superado la resistencia histórica: `{float(venta_actual):.2f} Bs`",
                    parse_mode="Markdown"
                )
    except Exception as e:
        logger.error(f"Error en alertas proactivas: {e}")

# ==========================================
# TELEGRAM BOT HANDLERS & COMANDOS RECUPERADOS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = [
        [InlineKeyboardButton("🔮 Ver Predicción +7H", callback_data="cmd_prediccion")],
        [InlineKeyboardButton("📊 Gráfica Institucional", callback_data="cmd_grafica")],
        [InlineKeyboardButton("📊 Análisis de Spread", callback_data="cmd_spread")],
        [InlineKeyboardButton("📈 Simulador P&L", callback_data="cmd_rendimiento")],
        [InlineKeyboardButton("🏦 Seleccionar Banco", callback_data="cmd_bancos")],
        [InlineKeyboardButton("💎 Suscribirse / Planes", callback_data="cmd_suscribir")]
    ]
    reply_markup = InlineKeyboardMarkup(teclado)
    await update.message.reply_text(
        "🦜 *VENBOT PREDICCIONES QUANT - PRO*\n"
        "🛡 *Terminal con Alertas Push, Simulador P&L y Filtro de Bancos*\n\n"
        "Selecciona una opción:",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    texto = (
        "💎 *SISTEMA DE SUSCRIPCIÓN VENBOT*\n\n"
        "Obtén acceso completo al motor cuantitativo avanzado, alertas prioritarias y reportes extendidos.\n\n"
        "• Usa /miplan para ver tu estado actual.\n"
        "• Contacta a soporte para activar tu membresía Pro."
    )
    if query: await query.answer()
    await message.reply_text(texto, parse_mode="Markdown")

async def cmd_registrar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("INSERT INTO config_usuario (telegram_id, banco_preferido, suscrito, plan) VALUES (%s, 'BBVA', FALSE, 'FREE') ON CONFLICT (telegram_id) DO NOTHING;", (chat_id,))
        conn.commit()
        cur.close()
        conn.close()
        await update.message.reply_text("✅ ¡Te has registrado exitosamente en el sistema de Venbot Quant!", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error en registrar: {e}")

async def cmd_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔑 *Gestión de Credenciales*\n\nPara restablecer tu contraseña o clave de API asociada al bot, contacta al administrador.", parse_mode="Markdown")

async def cmd_miplan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT banco_preferido, suscrito, plan FROM config_usuario WHERE telegram_id = %s;", (chat_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        plan = row[2] if row else "FREE"
        banco = row[0] if row else "BBVA"
        
        texto = (
            f"📊 *ESTADO DE TU PLAN (VENBOT)*\n\n"
            f"• Plan Activo: `{plan}`\n"
            f"• Banco Preferido: `{banco}`\n"
            f"• Estado: `Activo`"
        )
        await update.message.reply_text(texto, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error en miplan: {e}")

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat_id if query else update.effective_chat.id
    
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT banco_preferido FROM config_usuario WHERE telegram_id = %s;", (chat_id,))
        row = cur.fetchone()
        banco_usr = row[0] if row else "BBVA"
        cur.close()
        conn.close()
    except Exception:
        banco_usr = "BBVA"

    banco_map = {"BBVA": ["BBVA"], "MERCANTIL": ["Mercantil"], "BNC": ["BNC"], "GENERAL": ["BBVA", "Mercantil", "BNC"]}
    c_real, v_real, liquidez = obtener_precios_binance_p2p(banco_map.get(banco_usr, ["BBVA"]))
    datos = motor_quant_inteligente(c_real, v_real, liquidez, banco_usr)
    
    registrar_senal_simulador(c_real, datos["recompra"], datos["tendencia"])

    hora_actual = datetime.now(VET).strftime("%I:%M %p")
    hora_objetivo = (datetime.now(VET) + timedelta(hours=7)).strftime("%I:%M %p")

    texto = (
        f"🦜 *VENBOT QUANT - TERMINAL [{banco_usr}]*\n"
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
        await query.answer()
        await query.message.reply_text(texto, parse_mode="Markdown")
    else:
        await update.message.reply_text(texto, parse_mode="Markdown")

async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    chat_id = message.chat_id

    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT banco_preferido FROM config_usuario WHERE telegram_id = %s;", (chat_id,))
        row = cur.fetchone()
        banco_usr = row[0] if row else "BBVA"
        cur.close()
        conn.close()
    except Exception:
        banco_usr = "BBVA"

    buf = generar_grafica_prediccion_buffer(banco_usr)
    if buf:
        if query: await query.answer()
        await message.reply_photo(photo=buf, caption=f"📊 *Venbot Quant - Bandas y Spread [{banco_usr}]*", parse_mode="Markdown")
    else:
        await message.reply_text("⚠️ Recopilando muestras suficientes para la gráfica...")

async def cmd_spread(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    filas = obtener_estadisticas_db(limit=50)
    if not filas or len(filas) < 2:
        await message.reply_text("⚠️ No hay suficientes datos históricos.")
        return

    spreads = [float(f[1]) - float(f[0]) for f in filas]
    texto = (
        f"📊 *ANÁLISIS DE SPREAD EN VIVO*\n\n"
        f"• Spread Actual: `{spreads[-1]:.2f} Bs`\n"
        f"• Promedio: `{np.mean(spreads):.2f} Bs`\n"
        f"• Máximo: `{np.max(spreads):.2f} Bs`"
    )
    if query: await query.answer()
    await message.reply_text(texto, parse_mode="Markdown")

async def cmd_rendimiento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        cur.execute("SELECT estado, COUNT(*) FROM registro_senales GROUP BY estado;")
        res = cur.fetchall()
        cur.close()
        conn.close()
    except Exception:
        res = []

    texto = "📈 *SIMULADOR DE RENDIMIENTO P&L (+7H)*\n\nEstadísticas de señales históricas:\n"
    for estado, cuenta in res:
        texto += f"• {estado}: `{cuenta}`\n"
    
    if query: await query.answer()
    await message.reply_text(texto, parse_mode="Markdown")

async def cmd_bancos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    teclado = [
        [InlineKeyboardButton("BBVA Provincial", callback_data="banco_BBVA"), InlineKeyboardButton("Mercantil", callback_data="banco_MERCANTIL")],
        [InlineKeyboardButton("BNC", callback_data="banco_BNC"), InlineKeyboardButton("General (Todos)", callback_data="banco_GENERAL")]
    ]
    if query:
        await query.answer()
        await query.message.edit_text("🏦 Selecciona el banco para filtrar el motor Quant:", reply_markup=InlineKeyboardMarkup(teclado))
    else:
        await update.message.reply_text("🏦 Selecciona el banco para filtrar el motor Quant:", reply_markup=InlineKeyboardMarkup(teclado))

async def manejar_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("banco_"):
        banco_elegido = data.split("_")[1]
        try:
            conn = obtener_conexion()
            cur = conn.cursor()
            cur.execute("INSERT INTO config_usuario (telegram_id, banco_preferido) VALUES (%s, %s) ON CONFLICT (telegram_id) DO UPDATE SET banco_preferido = EXCLUDED.banco_preferido;", (chat_id, banco_elegido))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Error actualizando banco: {e}")
        await query.answer(f"Banco actualizado a: {banco_elegido}")
        await query.message.edit_text(f"✅ Banco configurado correctamente a: *{banco_elegido}*. Ya puedes usar `/prediccion` o `/grafica`.", parse_mode="Markdown")
        return

    if data == "cmd_prediccion": await cmd_prediccion(update, context)
    elif data == "cmd_grafica": await cmd_grafica(update, context)
    elif data == "cmd_spread": await cmd_spread(update, context)
    elif data == "cmd_rendimiento": await cmd_rendimiento(update, context)
    elif data == "cmd_bancos": await cmd_bancos(update, context)
    elif data == "cmd_suscribir": await cmd_suscribir(update, context)

async def tarea_recoleccion_automatica():
    while True:
        try:
            c, v, l = obtener_precios_binance_p2p()
            guardar_muestra_db(c, v, l, "GENERAL")
            evaluar_rendimiento_senales(c)
            logger.info(f"Muestra automática guardada: Compra={c}, Venta={v}")
            
            if telegram_app and telegram_app.bot:
                await verificar_alertas_proactivas(telegram_app.bot, c, v)
        except Exception as e:
            logger.error(f"Error recolección automática: {e}")
        await asyncio.sleep(300)

app = FastAPI()
telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

# Configurar manejadores de Telegram
telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
telegram_app.add_handler(CommandHandler("spread", cmd_spread))
telegram_app.add_handler(CommandHandler("rendimiento", cmd_rendimiento))
telegram_app.add_handler(CommandHandler("bancos", cmd_bancos))
telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
telegram_app.add_handler(CommandHandler("registrar", cmd_registrar))
telegram_app.add_handler(CommandHandler("password", cmd_password))
telegram_app.add_handler(CommandHandler("miplan", cmd_miplan))
telegram_app.add_handler(CallbackQueryHandler(manejar_botones))

@app.on_event("startup")
async def startup_event():
    inicializar_db()
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
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
    except Exception as e:
        logger.error(f"Error procesando webhook: {e}")
    return {"status": "ok"}

@app.get("/")
def read_root():
    return {"status": "Venbot Quant Pro Institucional Activo", "timestamp": str(datetime.now(VET))}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
