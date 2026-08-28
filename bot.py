import os
import asyncio
import psycopg2
import requests
import json
import numpy as np
import urllib3
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, Request
from fastapi.responses import Response, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# Importaciones de Machine Learning (XGBoost Quant)
from sklearn.linear_model import Ridge
import xgboost as xgb

# Importaciones de Telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    CallbackQueryHandler, 
    MessageHandler, 
    filters, 
    ConversationHandler
)

from google import genai
from google.genai import types

# Deshabilitar advertencias SSL en caso de fluctuaciones en el certificado del BCV
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de zona horaria Venezuela
VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Configura aquí tu ID personal de Telegram para que solo tú puedas ver y aprobar pagos desde el bot
ADMIN_TELEGRAM_ID = 123456789  # <--- REEMPLAZA ESTE NÚMERO CON TU TELEGRAM ID REAL

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
telegram_application = None

# Estados para la Conversación de Pagos en Telegram (Fase 3)
SELECCIONANDO_PLAN, ESPERANDO_REFERENCIA = range(2)

# ==========================================
# SISTEMA DE CACHÉ DE IA (15 MINUTOS)
# ==========================================
_gemini_cache = {
    "resultado": None,
    "ultima_actualizacion": 0
}
CACHE_EXPIRATION_TIME = 900  # 900 segundos = 15 minutos

# ==========================================
# AUTODESCUBRIMIENTO AUTÓNOMO DE MODELOS IA
# ==========================================
def obtener_modelo_gemini_activo() -> str:
    modelo_por_defecto = "gemini-2.5-flash"
    if not gemini_client:
        return modelo_por_defecto
    try:
        models_pager = gemini_client.models.list()
        candidatos = []
        for m in models_pager:
            nombre = getattr(m, "name", "")
            if nombre.startswith("models/"):
                nombre = nombre.replace("models/", "", 1)
            if "flash" in nombre.lower():
                candidatos.append(nombre)
        if candidatos:
            candidatos.sort(reverse=True)
            return candidatos[0]
    except Exception:
        pass
    return modelo_por_defecto

# ==========================================
# SCRAPING / OBTENCIÓN TASAS BCV Y EURO EN VIVO
# ==========================================
def obtener_tasas_oficiales_bcv():
    usd_bcv = 898.50
    eur_bcv = 1050.00
    try:
        url = "https://www.bcv.org.ve/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers, verify=False, timeout=3)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            usd_elem = soup.find('div', {'id': 'dolar'})
            if usd_elem:
                val = usd_elem.find('strong').text.strip().replace('.', '').replace(',', '.')
                usd_bcv = float(val)
            eur_elem = soup.find('div', {'id': 'euro'})
            if eur_elem:
                val = eur_elem.find('strong').text.strip().replace('.', '').replace(',', '.')
                eur_bcv = float(val)
            return usd_bcv, eur_bcv
    except Exception:
        pass

    try:
        res_backup = requests.get("https://rates.dolarvzla.com/bcv/current.json", timeout=2).json()
        usd_bcv = float(res_backup.get("current", {}).get("usd", usd_bcv))
        eur_bcv = float(res_backup.get("current", {}).get("eur", eur_bcv))
    except Exception:
        pass

    return usd_bcv, eur_bcv

# ==========================================
# BASE DE DATOS POSTGRESQL (SUPABASE)
# ==========================================
def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Error conectando a DB: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS historial (
                        id SERIAL PRIMARY KEY,
                        timestamp TEXT,
                        compra REAL,
                        venta REAL
                    );
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS usuarios (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE NOT NULL,
                        username TEXT,
                        nombre TEXT,
                        rol TEXT DEFAULT 'gratuito',
                        suscripcion_hasta TIMESTAMP WITH TIME ZONE,
                        creado_en TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pagos (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT REFERENCES public.usuarios(telegram_id),
                        plan TEXT NOT NULL,
                        monto REAL NOT NULL,
                        referencia TEXT UNIQUE,
                        metodo TEXT,
                        estado TEXT DEFAULT 'pendiente',
                        creado_en TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                ''')
                conn.commit()
        finally:
            conn.close()

init_db()

def guardar_muestra_db(compra, venta):
    conn = get_db_connection()
    if conn:
        try:
            hora_str = datetime.now(VET).isoformat()
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO historial (timestamp, compra, venta) VALUES (%s, %s, %s);",
                    (hora_str, compra, venta)
                )
                cursor.execute('''
                    DELETE FROM historial 
                    WHERE id NOT IN (
                        SELECT id FROM historial ORDER BY id DESC LIMIT 2000
                    );
                ''')
                conn.commit()
        except Exception as e:
            print(f"Error guardando en DB: {e}")
        finally:
            conn.close()

def obtener_estadisticas_db(limit=2000):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT compra, venta, timestamp FROM historial ORDER BY id ASC LIMIT %s;", (limit,))
                filas = cursor.fetchall()
            return filas
        except Exception as e:
            print(f"Error leyendo DB: {e}")
            return []
        finally:
            conn.close()
    return []

# ==========================================
# LECTURA P2P BINANCE (Filtros internos discretos)
# ==========================================
def fetch_binance_p2p():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    bancos_filtro = ["Mercantil", "Provincial", "BNC"]

    payload_compra = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000",
        "payTypes": bancos_filtro
    }
    payload_venta = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000",
        "payTypes": bancos_filtro
    }

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=4).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=4).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if not data_c or not data_v:
            return None, None, None, None

        precios_compra = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
        precios_venta = [float(item["adv"]["price"]) for item in data_v if "adv" in item]

        if not precios_compra or not precios_venta:
            return None, None, None, None

        tasa_compra = min(precios_compra)
        tasa_venta = max(precios_venta)

        if tasa_compra >= tasa_venta:
            tasa_compra = precios_compra[0]
            tasa_venta = precios_venta[0]

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2) if tasa_compra > 0 else 0.0

        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception:
        return None, None, None, None

# ==========================================
# GEMINI IA CON CACHÉ
# ==========================================
def obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia_quant, pred_compra, pred_venta):
    global _gemini_cache
    tiempo_actual = time.time()

    if _gemini_cache["resultado"] is not None and (tiempo_actual - _gemini_cache["ultima_actualizacion"] < CACHE_EXPIRATION_TIME):
        return _gemini_cache["resultado"]

    fallback_response = {
        "estado_actual": f"El spread P2P actual se ubica en {spread:.2f} Bs con órdenes activas en compra ({actual_compra:.2f} Bs) y venta ({actual_venta:.2f} Bs).",
        "proyeccion_7_12h": f"Tendencia {tendencia_quant}. Nivel óptimo de recompra estimado en {pred_compra:.2f} Bs.",
        "recomendacion_tactica": "Mantener margen dinámico en los anuncios de compra para acelerar la rotación de capital.",
        "tactica": {
            "texto": f"El spread P2P de {spread:.2f} Bs permite colocación rápida de órdenes en la punta competitiva.",
            "senal": "COMPRA MODERADA", "velocidad": "ALTA (< 5 min)", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} - {pred_venta:.2f} Bs"
        },
        "flujo": {
            "texto": "Absorción constante de volumen P2P orientada a comerciantes.",
            "dominio": "COMPRADORES ACTIVOS", "spread_status": f"{spread:.2f} Bs", "riesgo": "BAJO", "proyeccion_12h": f"{pred_venta:.2f} Bs"
        },
        "niveles": {
            "texto": "Comportamiento del libro de órdenes ajustado al canal actual de USDT/VES.",
            "momentum": "MEDIO (65%)", "liquidez": "ESTABLE", "quiebre": f"{actual_compra:.2f} Bs", "techo": f"{pred_venta:.2f} Bs"
        }
    }

    if not gemini_client:
        _gemini_cache["resultado"] = fallback_response
        _gemini_cache["ultima_actualizacion"] = tiempo_actual
        return fallback_response

    try:
        system_instruction = (
            "Eres Venbot AI, el analista experto de mercado P2P para VENBOT en Binance Venezuela (USDT/VES). "
            "Prohibido mencionar nombres de bancos específicos, BCV, tasa oficial o entes gubernamentales. "
            "Tus respuestas deben tratar exclusivamente sobre libro de órdenes P2P, spread y estrategia de anuncios."
        )

        prompt = f"""
        Datos Binance P2P Tiempo Real:
        - Compra: {actual_compra:.2f} Bs | Venta: {actual_venta:.2f} Bs | Spread: {spread:.2f} Bs
        - Tendencia: {tendencia_quant} | Recompra Proyectada: {pred_compra:.2f} Bs | Venta Proyectada: {pred_venta:.2f} Bs
        Genera este formato JSON estricto:
        {{
         "estado_actual": "Análisis exclusivo de las puntas P2P y spread actual (1 frase).",
         "proyeccion_7_12h": "Proyección de rotación P2P (1 frase).",
         "recomendacion_tactica": "Recomendación de colocación de anuncios (1 frase).",
         "tactica": {{"texto": "Lectura operativa P2P.", "senal": "COMPRA FUERTE", "velocidad": "ALTA", "sombra": "NORMAL", "rango": "{actual_compra:.2f} - {pred_venta:.2f} Bs"}},
         "flujo": {{"texto": "Análisis de flujo.", "dominio": "COMPRADORES", "spread_status": "{spread:.2f} Bs", "riesgo": "BAJO", "proyeccion_12h": "{pred_venta:.2f} Bs"}},
         "niveles": {{"texto": "Evaluación de soporte.", "momentum": "MEDIO", "liquidez": "ESTABLE", "quiebre": "{pred_compra:.2f} Bs", "techo": "{pred_venta:.2f} Bs"}}
        }}
        """

        modelo_activo = obtener_modelo_gemini_activo()
        response = gemini_client.models.generate_content(
            model=modelo_activo,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                temperature=0.3
            ),
        )
        resultado_json = json.loads(response.text)
        _gemini_cache["resultado"] = resultado_json
        _gemini_cache["ultima_actualizacion"] = tiempo_actual
        return resultado_json
    except Exception:
        _gemini_cache["resultado"] = fallback_response
        _gemini_cache["ultima_actualizacion"] = tiempo_actual + 900
        return fallback_response

# ==========================================
# MOTOR QUANT (XGBOOST)
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(actual_compra * 0.999, 2)
        pred_v = round(actual_venta * 1.001, 2)
        tendencia = "➖ ESTABLE / LATERAL"
        direction = "LATERAL"
        piso = actual_compra
        techo = actual_venta
    else:
        compras = np.array([f[0] for f in filas])
        ventas = np.array([f[1] for f in filas])
        piso = np.min(compras)
        techo = np.max(ventas)

        window_size = min(total_muestras - 1, 5)
        X, y = [], []
        for i in range(window_size, len(compras)):
            X.append(compras[i - window_size:i])
            y.append(compras[i])
        X = np.array(X)
        y = np.array(y)

        if len(X) > 0:
            model = xgb.XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1, verbosity=0)
            model.fit(X, y)
            last_window = compras[-window_size:].reshape(1, -1)
            pred_c_next = model.predict(last_window)[0]
            
            recent_x = np.arange(min(total_muestras, 30))
            recent_y = compras[-len(recent_x):]
            slope_c, _ = np.polyfit(recent_x, recent_y, 1)
            
            delta_estimado = (pred_c_next - actual_compra) + (slope_c * 10)
            pred_c = round(actual_compra + delta_estimado, 2)
        else:
            pred_c = round(actual_compra, 2)
            slope_c = 0.0

        spread_historico_promedio = np.mean(ventas - compras)
        pred_v = round(pred_c + spread_historico_promedio, 2)

        if slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
            direction = "ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
            direction = "BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"
            direction = "LATERAL"

    spread = round(actual_venta - actual_compra, 2)
    analisis_ia = obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia, pred_c, pred_v)

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "direccion": direction,
        "recompra": pred_c,
        "venta_esperada": pred_v,
        "piso_str": f"{piso:.2f} Bs",
        "techo_str": f"{techo:.2f} Bs",
        "muestras": total_muestras,
        "analisis_ia": analisis_ia
    }

# ==========================================
# TAREA EN SEGUNDO PLANO
# ==========================================
async def tarea_recoleccion_automatica():
    while True:
        try:
            compra, venta, _, _ = await asyncio.to_thread(fetch_binance_p2p)
            if compra and venta:
                await asyncio.to_thread(guardar_muestra_db, compra, venta)
        except Exception as e:
            print(f"Error en recolección: {e}")
        await asyncio.sleep(300)

# ==========================================
# BOT DE TELEGRAM (COMANDOS Y FLUJOS FASE 3)
# ==========================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO usuarios (telegram_id, username, nombre, rol) VALUES (%s, %s, %s, 'gratuito') ON CONFLICT (telegram_id) DO NOTHING;",
                    (user.id, user.username, user.first_name)
                )
                conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

    keyboard = [
        [InlineKeyboardButton("📊 Ver Predicción P2P", callback_data="menu_prediccion")],
        [InlineKeyboardButton("🧮 Calculadora Rápida", callback_data="menu_calcular")],
        [InlineKeyboardButton("👤 Mi Plan & Membresía", callback_data="menu_miplan")],
        [InlineKeyboardButton("💎 Reportar Pago / Suscribirse", callback_data="iniciar_pago")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    bienvenida = (
        f"🦜 ¡Bienvenido a **VENBOT**, {user.first_name}!\n\n"
        "Soy tu analista automatizado para el mercado P2P de Binance (USDT/VES).\n"
        "Elige una opción del menú:"
    )
    if update.message:
        await update.message.reply_text(bienvenida, parse_mode="Markdown", reply_markup=reply_markup)

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
        if not compra or not venta:
            compra, venta, spread, pct = 945.25, 956.00, 10.75, 1.14
        
        pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)
        hora_actual = datetime.now(VET).strftime("%I:%M %p")
        
        mensaje = (
            f"🦜 **VENBOT PREDICCIONES**\n"
            f"⏱ ({hora_actual}) | BLOQUE P2P\n"
            f"🟢 COMPRA (10k): {compra:.2f} Bs\n"
            f"🔴 VENTA (300k): {venta:.2f} Bs\n"
            f"⚡ MARGEN: {spread:.2f} Bs ({pct:.2f}%)\n\n"
            f"🔮 **PROYECCIÓN +7H (IA QUANT - XGBOOST)**\n"
            f"🟢 Recompra Esperada: {pred['pred_compra_str']}\n"
            f"🔴 Venta Esperada: {pred['pred_venta_str']}\n"
            f"🎯 Dirección: {pred['tendencia']}\n\n"
            f"📊 Piso: {pred['piso_str']} | Techo: {pred['techo_str']}\n"
            f"💾 Base de Datos: {pred['muestras']} Muestras"
        )
        target = update.message or update.callback_query.message
        await target.reply_text(mensaje, parse_mode="Markdown")
    except Exception as e:
        print(f"Error en predicción telegram: {e}")

async def cmd_calcular(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = update.message or update.callback_query.message
    texto_ayuda = (
        "🧮 **Calculadora Rápida P2P**\n\n"
        "Usa el comando indicando la cantidad de USDT que deseas calcular.\n"
        "Ejemplo: `/calcular 100`"
    )
    if update.callback_query:
        await target.edit_text(texto_ayuda, parse_mode="Markdown")
    else:
        await target.reply_text(texto_ayuda, parse_mode="Markdown")

async def cmd_calcular_monto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Por favor indica la cantidad. Ejemplo: `/calcular 100`", parse_mode="Markdown")
        return
    try:
        cantidad = float(context.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ Cantidad inválida. Usa solo números, ej: `/calcular 50.5`", parse_mode="Markdown")
        return

    compra, venta, spread, _ = await asyncio.to_thread(fetch_binance_p2p)
    if not compra or not venta:
        compra, venta = 945.25, 956.00

    total_costo_compra = cantidad * compra
    total_venta_estimada = cantidad * venta
    ganancia_neta = total_venta_estimada - total_costo_compra

    mensaje = (
        f"🧮 **SIMULACIÓN DE OPERACIÓN ({cantidad} USDT)**\n\n"
        f"🟢 Inversión estimada (Compra a {compra:.2f}): `{total_costo_compra:,.2f} Bs`\n"
        f"🔴 Retorno estimado (Venta a {venta:.2f}): `{total_venta_estimada:,.2f} Bs`\n"
        f"⚡ **Ganancia Neta Estimada:** `{ganancia_neta:,.2f} Bs`"
    )
    await update.message.reply_text(mensaje, parse_mode="Markdown")

async def cmd_miplan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = get_db_connection()
    rol = "gratuito"
    vencimiento = "Indefinido"
    
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT rol, suscripcion_hasta FROM usuarios WHERE telegram_id = %s;", (user.id,))
                res = cursor.fetchone()
                if res:
                    rol = res[0]
                    if res[1]:
                        vencimiento = res[1].strftime("%d/%m/%Y %I:%M %p")
        except Exception:
            pass
        finally:
            conn.close()

    mensaje = (
        f"👤 **ESTADO DE TU CUENTA - VENBOT**\n\n"
        f"📌 **Usuario:** {user.first_name}\n"
        f"🏷 **Plan Actual:** `{rol.upper()}`\n"
        f"⏳ **Válido Hasta:** {vencimiento}\n\n"
        f"Para actualizar tu plan a **Premium** o **VIP**, haz clic en el botón de abajo."
    )
    keyboard = [
        [InlineKeyboardButton("💎 Adquirir / Renovar Plan", callback_data="iniciar_pago")],
        [InlineKeyboardButton("🔙 Menú Principal", callback_data="menu_inicio")]
    ]
    target = update.message or update.callback_query.message
    if update.callback_query:
        await target.edit_text(mensaje, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await target.reply_text(mensaje, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def cmd_ayuda_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensaje = (
        "📋 **MENÚ DE COMANDOS DISPONIBLES**\n\n"
        "/start - Inicia el bot y muestra el panel principal.\n"
        "/prediccion - Muestra el estado actual del mercado P2P y proyecciones de IA.\n"
        "/calcular [cantidad] - Calcula la ganancia estimada para X cantidad de USDT.\n"
        "/miplan - Consulta el estado actual de tu suscripción y plan activo.\n"
        "/ayuda - Muestra este listado de comandos."
    )
    target = update.message or update.callback_query.message
    if update.callback_query:
        await target.edit_text(mensaje, parse_mode="Markdown")
    else:
        await target.reply_text(mensaje, parse_mode="Markdown")

# ==========================================
# PANEL DE ADMINISTRACIÓN DE PAGOS DESDE TELEGRAM
# ==========================================
async def cmd_admin_pagos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id != ADMIN_TELEGRAM_ID:
        await update.message.reply_text("⛔ No tienes permisos de administrador para ejecutar este comando.")
        return

    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("⚠️ Error de conexión con la base de datos.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, telegram_id, plan, monto, referencia, creado_en FROM pagos WHERE estado = 'pendiente' ORDER BY id ASC LIMIT 5;")
            filas = cursor.fetchall()
            if not filas:
                await update.message.reply_text("✅ No hay pagos pendientes por aprobar en este momento.")
                return

            for fila in filas:
                p_id, t_id, plan, monto, ref, fecha = fila
                keyboard = [[InlineKeyboardButton(f"✅ Aprobar Pago #{p_id}", callback_data=f"aprove_{p_id}")]]
                await update.message.reply_text(
                    f"📦 **Pago Pendiente #{p_id}**\n"
                    f"• Telegram ID: `{t_id}`\n"
                    f"• Plan: `{plan.upper()}` (${monto})\n"
                    f"• Referencia: `{ref}`",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Error al consultar pagos: {e}")
    finally:
        conn.close()

async def callback_aprobar_pago_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not query.data.startswith("aprove_"):
        return

    user = update.effective_user
    if user.id != ADMIN_TELEGRAM_ID:
        await query.message.reply_text("⛔ No tienes permisos para aprobar pagos.")
        return

    pago_id = int(query.data.split("_")[1])
    conn = get_db_connection()
    if not conn:
        await query.message.reply_text("⚠️ Error de conexión con la base de datos.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT telegram_id, plan FROM pagos WHERE id = %s AND estado = 'pendiente';", (pago_id,))
            res = cursor.fetchone()
            if not res:
                await query.message.edit_text(f"⚠️ El pago #{pago_id} ya fue aprobado o no existe.")
                return

            telegram_id, plan = res
            nueva_expiracion = datetime.now(VET) + timedelta(days=30)

            cursor.execute("UPDATE pagos SET estado = 'aprobado' WHERE id = %s;", (pago_id,))
            cursor.execute(
                "UPDATE usuarios SET rol = %s, suscripcion_hasta = %s WHERE telegram_id = %s;",
                (plan, nueva_expiracion, telegram_id)
            )
            conn.commit()

            if telegram_application:
                try:
                    mensaje_usuario = (
                        f"🎉 **¡Pago Aprobado con Éxito!**\n\n"
                        f"Tu plan `{plan.upper()}` ha sido activado en VENBOT.\n"
                        f"Válido hasta: {nueva_expiracion.strftime('%d/%m/%Y %I:%M %p')}\n\n"
                        f"Disfruta de todas las funciones exclusivas 🦜"
                    )
                    await telegram_application.bot.send_message(chat_id=telegram_id, text=mensaje_usuario, parse_mode="Markdown")
                except Exception:
                    pass

            await query.message.edit_text(f"✅ **Pago #{pago_id} Aprobado Exitosamente**\n• Usuario ID: `{telegram_id}`\n• Plan Asignado: `{plan.upper()}`")
    except Exception as e:
        await query.message.reply_text(f"⚠️ Error al procesar aprobación: {e}")
    finally:
        conn.close()

# ==========================================
# CONVERSATION HANDLER PARA PAGOS (FASE 3)
# ==========================================
async def iniciar_pago_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("⭐ Plan Premium ($7/mes)", callback_data="plan_premium")],
        [InlineKeyboardButton("🔥 Plan VIP ($15/mes)", callback_data="plan_vip")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="menu_inicio")]
    ]
    await query.message.edit_text(
        "💎 **SELECCIONA TU PLAN DE SUSCRIPCIÓN**\n\n"
        "Elige el plan que deseas adquirir para procesar tu reporte de pago:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECCIONANDO_PLAN

async def recibir_seleccion_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    datos_plan = query.data
    if datos_plan == "plan_premium":
        context.user_data["plan_elegido"] = "premium"
        context.user_data["monto_plan"] = 7.0
    elif datos_plan == "plan_vip":
        context.user_data["plan_elegido"] = "vip"
        context.user_data["monto_plan"] = 15.0
    else:
        await query.message.edit_text("Operación cancelada.")
        return ConversationHandler.END

    instrucciones = (
        f"💳 **DATOS PARA EL PAGO ({context.user_data['plan_elegido'].upper()})**\n\n"
        "Realiza tu pago por los siguientes medios:\n\n"
        "📱 **Pago Móvil:**\n"
        "• Banco: `Banco Mercantil`\n"
        "• Teléfono: `0424-5734635`\n"
        "• Cédula: `20414065`\n\n"
        "🟡 **Binance Pay:**\n"
        "• Email: `nazaretgarcia69@gmail.com`\n\n"
        "--- \n"
        "Una vez realizado el pago, **escribe aquí abajo los últimos 4 dígitos o número de referencia** de tu transferencia:\n"
        "*(Ejemplo: 4829)*"
    )
    keyboard = [[InlineKeyboardButton("❌ Cancelar", callback_data="menu_inicio")]]
    await query.message.edit_text(instrucciones, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    return ESPERANDO_REFERENCIA

async def recibir_referencia_pago(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    referencia = update.message.text.strip()
    plan = context.user_data.get("plan_elegido", "premium")
    monto = context.user_data.get("monto_plan", 7.0)

    conn = get_db_connection()
    registrado = False
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO pagos (telegram_id, plan, monto, referencia, metodo, estado) VALUES (%s, %s, %s, %s, 'Pago Movil / Transferencia', 'pendiente');",
                    (user.id, plan, monto, referencia)
                )
                conn.commit()
                registrado = True
        except Exception as e:
            print(f"Error guardando pago: {e}")
        finally:
            conn.close()

    if registrado:
        mensaje_exito = (
            f"🦜 **¡Pago reportado con éxito!**\n\n"
            f"📦 **Plan:** {plan.upper()}\n"
            f"🔢 **Referencia:** `{referencia}`\n"
            f"🔄 **Estado:** Pendiente de verificación por el equipo.\n\n"
            f"En los próximos minutos tu cuenta será activada automáticamente. Usa `/miplan` para verificar."
        )
    else:
        mensaje_exito = "⚠️ Hubo un error al registrar la referencia o el número de referencia ya fue utilizado. Intenta nuevamente con `/start`."

    await update.message.reply_text(mensaje_exito, parse_mode="Markdown")
    return ConversationHandler.END

async def cancelar_conversacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("📊 Ver Predicción P2P", callback_data="menu_prediccion")],
        [InlineKeyboardButton("👤 Mi Plan", callback_data="menu_miplan")]
    ]
    await query.message.edit_text("Operación cancelada. Menú principal:", reply_markup=InlineKeyboardMarkup(keyboard))
    return ConversationHandler.END

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "menu_prediccion":
        await cmd_prediccion(update, context)
    elif query.data == "menu_miplan":
        await cmd_miplan(update, context)
    elif query.data == "menu_calcular":
        await cmd_calcular(update, context)
    elif query.data == "menu_inicio":
        keyboard = [
            [InlineKeyboardButton("📊 Ver Predicción P2P", callback_data="menu_prediccion")],
            [InlineKeyboardButton("🧮 Calculadora Rápida", callback_data="menu_calcular")],
            [InlineKeyboardButton("👤 Mi Plan & Membresía", callback_data="menu_miplan")],
            [InlineKeyboardButton("💎 Reportar Pago / Suscribirse", callback_data="iniciar_pago")]
        ]
        await query.message.edit_text("Panel Principal VENBOT:", reply_markup=InlineKeyboardMarkup(keyboard))
