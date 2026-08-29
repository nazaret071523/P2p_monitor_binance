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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler

from google import genai
from google.genai import types

# Deshabilitar advertencias SSL en caso de fluctuaciones en el certificado del BCV
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de zona horaria Venezuela
VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

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
    modelo_por_defecto = "gemini-1.5-flash"
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
        res = requests.get(url, headers=headers, verify=False, timeout=4)
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
    except Exception as e:
        print(f"Error consultando sitio oficial BCV: {e}")

    try:
        res_backup = requests.get("https://rates.dolarvzla.com/bcv/current.json", timeout=3).json()
        usd_bcv = float(res_backup.get("current", {}).get("usd", usd_bcv))
        eur_bcv = float(res_backup.get("current", {}).get("eur", eur_bcv))
    except Exception as e:
        print(f"Error consultando API respaldo BCV: {e}")

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
                    CREATE TABLE IF NOT EXISTS usuarios_p2p (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE,
                        username TEXT,
                        estado_suscripcion TEXT DEFAULT 'pendiente',
                        referencia_pago TEXT,
                        fecha_expiracion TIMESTAMP,
                        tipo_plan TEXT DEFAULT 'vip',
                        password TEXT
                    );
                ''')
                # Asegurar columnas si la tabla ya existía (CORREGIDO CON COMILLAS DOBLES AQUÍ 👇)
                cursor.execute('ALTER TABLE usuarios_p2p ADD COLUMN IF NOT EXISTS tipo_plan TEXT DEFAULT "vip";')
                cursor.execute('ALTER TABLE usuarios_p2p ADD COLUMN IF NOT EXISTS password TEXT;')
                conn.commit()
        finally:
            conn.close()

init_db()

def registrar_pago_db(telegram_id: int, username: str, referencia: str, plan_elegido: str = 'vip') -> bool:
    conn = get_db_connection()
    if not conn:
        return False
    try:
        import random
        pass_temporal = f"vb_{telegram_id}_{random.randint(100, 999)}"
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usuarios_p2p (telegram_id, username, estado_suscripcion, referencia_pago, tipo_plan, password)
                    VALUES (%s, %s, 'pendiente', %s, %s, %s)
                    ON CONFLICT (telegram_id) 
                    DO UPDATE SET referencia_pago = %s, estado_suscripcion = 'pendiente', tipo_plan = %s;
                """, (telegram_id, username, referencia, plan_elegido, pass_temporal, referencia, plan_elegido))
        return True
    except Exception as e:
        print(f"Error en registrar_pago_db: {e}")
        return False
    finally:
        conn.close()

def verificar_estado_usuario(telegram_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {"estado": "error"}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT estado_suscripcion, fecha_expiracion, referencia_pago, tipo_plan, password, username 
                    FROM usuarios_p2p WHERE telegram_id = %s;
                """, (telegram_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "estado": row[0],
                        "expiracion": row[1],
                        "referencia": row[2],
                        "plan": row[3] or "vip",
                        "password": row[4],
                        "username": row[5]
                    }
        return {"estado": "no_registrado"}
    except Exception as e:
        print(f"Error en verificar_estado_usuario: {e}")
        return {"estado": "error"}
    finally:
        conn.close()

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
# LECTURA P2P BINANCE
# ==========================================
def fetch_binance_p2p():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

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
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=5).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=5).json()

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
    except Exception as e:
        print(f"Error consultando Binance: {e}")
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
        "estado_actual": f"El spread P2P actual se ubica en {spread:.2f} Bs con ordenes activas en compra ({actual_compra:.2f} Bs) y venta ({actual_venta:.2f} Bs).",
        "proyeccion_7_12h": f"Tendencia {tendencia_quant}. Nivel óptimo de recompra estimado en {pred_compra:.2f} Bs.",
        "recomendacion_tactica": "Mantener margen dinámico en los anuncios de compra para acelerar la rotación de capital.",
        "tactica": {
            "texto": f"El spread P2P de {spread:.2f} Bs permite colocación rápida de órdenes en la punta competitiva.",
            "senal": "COMPRA MODERADA", "velocidad": "ALTA (< 5 min)", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} - {pred_venta:.2f} Bs"
        },
        "flujo": {
            "texto": "Absorción constante de volumen P2P orientada a comerciantes no verificados.",
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
            "Eres el analista de mercado P2P para VENBOT en Binance Venezuela (USDT/VES). "
            "PROHIBIDO ABSOLUTAMENTE: Mencionar BCV, tasa oficial, Euro, brechas cambiarías institucionales o entes gubernamentales."
        )

        prompt = f"""
        Datos Binance P2P Tiempo Real:
        - Compra: {actual_compra:.2f} Bs | Venta: {actual_venta:.2f} Bs | Spread: {spread:.2f} Bs
        - Tendencia: {tendencia_quant} | Recompra Proyectada: {pred_compra:.2f} Bs | Venta Proyectada: {pred_venta:.2f} Bs

        Genera este formato JSON estricto enfocando el análisis exclusivamente en Binance P2P:
        {{
          "estado_actual": "Análisis exclusivo de las puntas P2P y spread actual en Binance (1 frase corta).",
          "proyeccion_7_12h": "Proyección de rotación P2P y recompra esperada en Binance (1 frase corta).",
          "recomendacion_tactica": "Recomendación de colocación de anuncios P2P (1 frase corta).",
          "tactica": {{
            "texto": "Lectura operativa P2P.",
            "senal": "COMPRA FUERTE | COMPRA MODERADA | ESPERAR",
            "velocidad": "ALTA (< 5 min) | MEDIA",
            "sombra": "ESCASEZ DE USDT | NORMAL",
            "rango": "{actual_compra:.2f} - {pred_venta:.2f} Bs"
          }},
          "flujo": {{
            "texto": "Análisis del flujo de liquidez P2P.",
            "dominio": "COMPRADORES AGRESIVOS | LATERAL | VENDEDORES ACTIVOS",
            "spread_status": "{spread:.2f} Bs",
            "riesgo": "BAJO | MEDIO | ALTO",
            "proyeccion_12h": "{pred_venta:.2f} Bs"
          }},
          "niveles": {{
            "texto": "Evaluación del soporte y resistencia en el libro P2P.",
            "momentum": "ALTO (80%) | MEDIO (65%) | BAJO",
            "liquidez": "ABUNDANTE | ESTABLE | ESCASA",
            "quiebre": "{pred_compra:.2f} Bs",
            "techo": "{pred_venta:.2f} Bs"
          }}
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
# MOTOR QUANT (XGBOOST QUANT MACHINE LEARNING)
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(actual_compra * 0.999, 2)
        pred_v = round(actual_venta * 1.001, 2)
        tendencia = "➖ ESTABLE / LATERAL"
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

        if total_muestras >= 10:
            recent_x = np.arange(min(total_muestras, 30))
            recent_y = compras[-len(recent_x):]
            slope_c, _ = np.polyfit(recent_x, recent_y, 1)
        else:
            slope_c = 0.0

        if slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"

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
        "analisis_ia": analisis_ia
    }

# ==========================================
# RECOLECCIÓN AUTOMÁTICA
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
# BOT DE TELEGRAM (COMANDOS Y BOTONES)
# ==========================================
telegram_app = None

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        datos_usuario = verificar_estado_usuario(user_id)
        
        if datos_usuario.get("estado") != "activo":
            await update.message.reply_text(
                "🔒 *Contenido Exclusivo para Miembros Suscritos*\n\n"
                "No tienes una suscripción activa. Usa `/suscribir` para ver los planes y desbloquear las señales de predicción.",
                parse_mode="Markdown"
            )
            return

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
        await update.message.reply_text(mensaje, parse_mode="Markdown")
    except Exception as e:
        print(f"Error en comando telegram: {e}")
        await update.message.reply_text("⚠️ Ocurrió un error al procesar la predicción.")

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = [
        [InlineKeyboardButton("⭐ Plan PREMIUN (5 USD)", callback_data="plan_vip")],
        [InlineKeyboardButton("🚀 Plan VIP (15 USD)", callback_data="plan_premium")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="plan_cancelar")]
    ]
    reply_markup = InlineKeyboardMarkup(teclado)
    texto = "💎 **SELECCIONA TU PLAN DE SUSCRIPCIÓN**\n\nElige el plan que deseas adquirir para procesar tu reporte de pago:"
    await update.message.reply_text(texto, reply_markup=reply_markup, parse_mode="Markdown")

async def callback_botones_suscripcion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "plan_cancelar":
        await query.message.edit_text("❌ Operación cancelada.")
        return

    plan_seleccionado = "VIP" if data == "plan_vip" else "PREMIUM"
    comando_ejemplo = f"/registrar vip [referencia]" if plan_seleccionado == "VIP" else f"/registrar premium [referencia]"

    texto_metodos = (
        f"💳 **MÉTODOS DE PAGO - PLAN {plan_seleccionado}**\n\n"
        "🇻🇪 **Pago Móvil (Bs. a Tasa BCV):**\n"
        "• Banco: Mercantil (0105)\n"
        "• Teléfono: 0424-5734635\n"
        "• C.I: V-20.414.065\n\n"
        "🌍 **Binance Pay:**\n"
        "• Pay ID / Email: `nazaretgarcia69@gmail.com`\n\n"
        "📝 **¿Cómo registrar tu pago?**\n"
        f"Envía el comando con tu número de referencia:\n`{comando_ejemplo}`"
    )
    await query.message.edit_text(texto_metodos, parse_mode="Markdown")

async def cmd_registrar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "⚠️ **Formato incorrecto.**\n"
            "Debes especificar el plan y la referencia. Ejemplo:\n"
            "`/registrar vip 8492` o `/registrar premium 8492`", 
            parse_mode="Markdown"
        )
        return
    
    plan_elegido = args[0].lower()
    if plan_elegido not in ["vip", "premium"]:
        await update.message.reply_text("⚠️ El plan debe ser exactamente `vip` o `premium`.")
        return
        
    referencia = args[1]
    exito = registrar_pago_db(user_id, username, referencia, plan_elegido)
    
    if exito:
        datos = verificar_estado_usuario(user_id)
        password_web = datos.get("password", "N/A")
        await update.message.reply_text(
            "✅ **¡Comprobante enviado con éxito!**\n\n"
            f"• Plan solicitado: **{plan_elegido.upper()}**\n"
            f"• Referencia registrada: `{referencia}`\n\n"
            f"🔐 **Tus credenciales generadas para el Monitor Web:**\n"
            f"• Usuario / ID: `{user_id}`\n"
            f"• Contraseña: `{password_web}`\n\n"
            "Tu pago está en estado **pendiente** de aprobación por el administrador.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Ocurrió un error al registrar el pago en el sistema.")

async def cmd_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    datos = verificar_estado_usuario(user_id)
    
    if datos.get("estado") == "no_registrado":
        await update.message.reply_text("❌ No estás registrado en el sistema. Usa `/suscribir` para comenzar.", parse_mode="Markdown")
        return
        
    password_web = datos.get("password")
    plan = datos.get("plan", "vip").upper()
    estado = datos.get("estado").upper()
    
    mensaje = (
        f"🔐 **Tus Credenciales para el Monitor Web**\n\n"
        f"📦 Plan: **{plan}**\n"
        f"🟢 Estado: **{estado}**\n\n"
        f"• Usuario (Telegram ID): `{user_id}`\n"
        f"• Contraseña Web: `{password_web}`\n\n"
        "Usa estos datos en la pantalla de inicio de sesión de la plataforma web."
    )
    await update.message.reply_text(mensaje, parse_mode="Markdown")

async def cmd_miplan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    datos = verificar_estado_usuario(user_id)
    estado = datos.get("estado")
    plan_actual = datos.get("plan", "vip").upper()
    
    if estado == "activo":
        exp = datos.get("expiracion")
        exp_texto = exp if exp else "Ilimitado / Vitalicio"
        await update.message.reply_text(
            f"✨ *Tu suscripción está ACTIVA*\n"
            f"📦 Plan: **{plan_actual}**\n"
            f"⏳ Vence el: `{exp_texto}`\n\n"
            "💡 Usa `/password` para ver tus credenciales de acceso web.", 
            parse_mode="Markdown"
        )
    elif estado == "pendiente":
        ref = datos.get("referencia")
        await update.message.reply_text(
            f"⏳ *Suscripción en revisión*\n"
            f"📦 Plan solicitado: **{plan_actual}**\n"
            f"🔢 Referencia enviada: `{ref}`\n"
            "Espera la aprobación del administrador.", 
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ No tienes una suscripción activa. Usa `/suscribir` para ver los planes disponibles.", parse_mode="Markdown")

async def iniciar_telegram_bot():
    global telegram_app
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ TELEGRAM_BOT_TOKEN no configurado.")
        return
    try:
        telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
        telegram_app.add_handler(CommandHandler("registrar", cmd_registrar))
        telegram_app.add_handler(CommandHandler("password", cmd_password))
        telegram_app.add_handler(CommandHandler("miplan", cmd_miplan))
        telegram_app.add_handler(CallbackQueryHandler(callback_botones_suscripcion))
        
        await telegram_app.initialize()
        await telegram_app.start()
        print("🤖 Bot de Telegram inicializado con éxito con manejo de contraseñas web.")
    except Exception as e:
        print(f"Error al iniciar el bot de Telegram: {e}")

# ==========================================
# SERVIDOR FASTAPI Y ENDPOINTS ASÍNCRONOS
# ==========================================
@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    asyncio.create_task(tarea_recoleccion_automatica())
    await iniciar_telegram_bot()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"]
)

@app.api_route("/", methods=["GET", "HEAD"])
async def home():
    return {"status": "ok", "message": "Venbot P2P Activo con Autenticación Web y XGBoost"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    if not telegram_app:
        return {"status": "error", "message": "Telegram app not initialized"}
    try:
        json_data = await request.json()
        update = Update.de_json(json_data, telegram_app.bot)
        await telegram_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        print(f"Error procesando webhook: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/login")
async def api_login(payload: dict = Body(...)):
    usuario_ingresado = str(payload.get("username", "")).strip()
    password_ingresado = str(payload.get("password", "")).strip()

    if not usuario_ingresado or not password_ingresado:
        return JSONResponse(status_code=400, content={"success": False, "message": "Faltan credenciales"})

    conn = get_db_connection()
    if not conn:
        return JSONResponse(status_code=500, content={"success": False, "message": "Error de base de datos"})

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT telegram_id, estado_suscripcion, tipo_plan, password 
                FROM usuarios_p2p 
                WHERE (CAST(telegram_id AS TEXT) = %s OR username ILIKE %s) AND password = %s;
            """, (usuario_ingresado, usuario_ingresado, password_ingresado))
            row = cur.fetchone()

            if row:
                telegram_id, estado, plan, _ = row
                if estado == "activo":
                    return {"success": True, "message": "Login exitoso", "plan": plan, "telegram_id": telegram_id}
                else:
                    return JSONResponse(status_code=403, content={"success": False, "message": "Tu cuenta está pendiente de aprobación o inactiva."})
            else:
                return JSONResponse(status_code=401, content={"success": False, "message": "Usuario o contraseña inválidos."})
    except Exception as e:
        print(f"Error en login: {e}")
        return JSONResponse(status_code=500, content={"success": False, "message": "Error interno en el servidor"})
    finally:
        conn.close()

@app.get("/api/stream")
async def event_stream():
    async def event_generator():
        yield "retry: 3000\n\n"
        while True:
            try:
                compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
                usd_bcv, eur_bcv = await asyncio.to_thread(obtener_tasas_oficiales_bcv)
                
                if not compra:
                    compra, venta, spread, pct = 945.25, 956.00, 10.75, 1.14

                pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)

                payload = {
                    "compra": compra,
                    "venta": venta,
                    "spread": spread,
                    "pct_bruto": pct,
                    "diferencia": spread,
                    "buy_price": compra,
                    "sell_price": venta,
                    "bcv": usd_bcv,
                    "euro": eur_bcv,
                    "prediccion": pred,
                    "timestamp": datetime.now(VET).isoformat(),
                    "status": "connected"
                }
                yield f"data: {json.dumps(payload)}\n\n"
            except Exception as e:
                print(f"Error en stream generator: {e}")
            await asyncio.sleep(5)

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    }
    return StreamingResponse(event_generator(), headers=headers)

@app.get("/api/v1/p2p-rates")
async def get_p2p_rates_v1():
    compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
    usd_bcv, eur_bcv = await asyncio.to_thread(obtener_tasas_oficiales_bcv)
    if not compra:
        compra, venta = 945.25, 956.00
    data = {
        "buy_price": compra,
        "sell_price": venta,
        "bcv_price": usd_bcv,
        "euro_price": eur_bcv
    }
    return JSONResponse(
        content=data,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )
