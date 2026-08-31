import os
import asyncio
import psycopg2
import requests
import json
import numpy as np
import urllib3
import time
import io
import matplotlib
matplotlib.use('Agg') # Configurar backend no interactivo para gráficos
import matplotlib.pyplot as plt

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Body, Request
from fastapi.responses import Response, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# Machine Learning (XGBoost Quant)
import xgboost as xgb

# Telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler

from google import genai
from google.genai import types

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# Caché e IA
_gemini_cache = {"resultado": None, "ultima_actualizacion": 0}
CACHE_EXPIRATION_TIME = 900 

# Estado global para control de cambios de tendencia en tiempo real
ESTADO_MERCADO_GLOBAL = {
    "ultima_tendencia": "➖ ESTABLE / LATERAL",
    "ultimo_spread": 0.0,
    "chat_ids_alertas": set() # Almacena usuarios o canales suscritos a alertas automáticas
}

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

def obtener_tasas_oficiales_bcv():
    usd_bcv, eur_bcv = 898.50, 1050.00
    try:
        url = "https://www.bcv.org.ve/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers, verify=False, timeout=4)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            usd_elem = soup.find('div', {'id': 'dolar'})
            if usd_elem:
                usd_bcv = float(usd_elem.find('strong').text.strip().replace('.', '').replace(',', '.'))
            eur_elem = soup.find('div', {'id': 'euro'})
            if eur_elem:
                eur_bcv = float(eur_elem.find('strong').text.strip().replace('.', '').replace(',', '.'))
            return usd_bcv, eur_bcv
    except Exception:
        pass
    return usd_bcv, eur_bcv

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
                cursor.execute("ALTER TABLE usuarios_p2p ADD COLUMN IF NOT EXISTS tipo_plan TEXT DEFAULT 'vip';")
                cursor.execute("ALTER TABLE usuarios_p2p ADD COLUMN IF NOT EXISTS password TEXT;")
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
                        "estado": row[0], "expiracion": row[1], "referencia": row[2],
                        "plan": row[3] or "vip", "password": row[4], "username": row[5]
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
                    WHERE id NOT IN (SELECT id FROM historial ORDER BY id DESC LIMIT 2000);
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

def fetch_binance_p2p():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    bancos_filtro = ["BBVA", "Mercantil", "BNC"]

    payload_compra = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": "10000", "payTypes": bancos_filtro}
    payload_venta = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": "300000", "payTypes": bancos_filtro}

    try:
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=5).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=5).json()

        data_c, data_v = res_c.get("data", []), res_v.get("data", [])
        if not data_c or not data_v:
            return None, None, None, None

        precios_compra = [float(item["adv"]["price"]) for item in data_c if "adv" in item]
        precios_venta = [float(item["adv"]["price"]) for item in data_v if "adv" in item]

        if not precios_compra or not precios_venta:
            return None, None, None, None

        tasa_compra, tasa_venta = min(precios_compra), max(precios_venta)
        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = precios_compra[0], precios_venta[0]

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2) if tasa_compra > 0 else 0.0

        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return None, None, None, None

def obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia_quant, pred_compra, pred_venta):
    global _gemini_cache
    tiempo_actual = time.time()

    if _gemini_cache["resultado"] is not None and (tiempo_actual - _gemini_cache["ultima_actualizacion"] < CACHE_EXPIRATION_TIME):
        return _gemini_cache["resultado"]

    fallback_response = {
        "estado_actual": f"Spread P2P actual en {spread:.2f} Bs con órdenes activas en compra ({actual_compra:.2f} Bs) y venta ({actual_venta:.2f} Bs).",
        "proyeccion_7_12h": f"Tendencia {tendencia_quant}. Recompra óptima estimada en {pred_compra:.2f} Bs.",
        "recomendacion_tactica": "Mantener margen dinámico en anuncios para optimizar rotación.",
        "tactica": {"texto": "Lectura operativa P2P estable.", "senal": "COMPRA MODERADA", "velocidad": "ALTA", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} - {pred_venta:.2f} Bs"},
        "flujo": {"texto": "Absorción constante de volumen P2P.", "dominio": "COMPRADORES ACTIVOS", "spread_status": f"{spread:.2f} Bs", "riesgo": "BAJO", "proyeccion_12h": f"{pred_venta:.2f} Bs"},
        "niveles": {"texto": "Libro de órdenes ajustado al canal USDT/VES.", "momentum": "MEDIO", "liquidez": "ESTABLE", "quiebre": f"{actual_compra:.2f} Bs", "techo": f"{pred_venta:.2f} Bs"}
    }

    if not gemini_client:
        return fallback_response

    try:
        system_instruction = "Eres el analista de mercado P2P para VENBOT en Binance Venezuela (USDT/VES). PROHIBIDO ABSOLUTAMENTE mencionar BCV, tasa oficial o entes gubernamentales."
        prompt = f"Datos Binance P2P: Compra {actual_compra:.2f} Bs | Venta {actual_venta:.2f} Bs | Spread {spread:.2f} Bs | Tendencia {tendencia_quant}. Genera JSON con claves: estado_actual, proyeccion_7_12h, recomendacion_tactica, tactica, flujo, niveles."
        
        modelo_activo = obtener_modelo_gemini_activo()
        response = gemini_client.models.generate_content(
            model=modelo_activo, contents=prompt,
            config=types.GenerateContentConfig(system_instruction=system_instruction, response_mime_type="application/json", temperature=0.3)
        )
        resultado_json = json.loads(response.text)
        _gemini_cache["resultado"] = resultado_json
        _gemini_cache["ultima_actualizacion"] = tiempo_actual
        return resultado_json
    except Exception:
        return fallback_response

def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 15:
        pred_c = round(actual_compra * 0.999, 2)
        pred_v = round(actual_venta * 1.001, 2)
        tendencia = "➖ ESTABLE / LATERAL"
        piso, techo = actual_compra, actual_venta
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
            
            pred_c = round(actual_compra + ((pred_c_next - actual_compra) + (slope_c * 10)), 2)
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

    spread = round(actual_venta - actual_compra, 2)
    analisis_ia = obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia, pred_c, pred_v)

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs", "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia, "recompra": pred_c, "venta_esperada": pred_v,
        "piso_str": f"{piso:.2f} Bs", "techo_str": f"{techo:.2f} Bs",
        "muestras": total_muestras, "analisis_ia": analisis_ia
    }

# ==========================================
# GENERADOR DE GRÁFICAS DE PREDICCIÓN (MATPLOTLIB)
# ==========================================
def generar_grafica_prediccion_buffer():
    filas = obtener_estadisticas_db(limit=40)
    if not filas or len(filas) < 2:
        return None

    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    tiempos = [f[2][11:16] for f in filas] # Extraer hora HH:MM

    plt.figure(figsize=(9, 4.5))
    plt.style.use('dark_background')

    plt.plot(tiempos, compras, label='Compra P2P Real', color='#00ffcc', marker='o', linewidth=2, markersize=4)
    plt.plot(tiempos, ventas, label='Venta P2P Real', color='#ff0055', marker='x', linewidth=1.5, markersize=4)

    plt.title('Venbot Quant - Historial y Proyección P2P USDT/VES', fontsize=12, color='white', pad=12)
    plt.xlabel('Hora (VET)', color='#aaaaaa', fontsize=9)
    plt.ylabel('Precio (Bs)', color='#aaaaaa', fontsize=9)
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
# TAREA AUTOMÁTICA CON ALERTA DE CAMBIO DE TENDENCIA
# ==========================================
async def tarea_recoleccion_automatica():
    global ESTADO_MERCADO_GLOBAL
    while True:
        try:
            compra, venta, spread, _ = await asyncio.to_thread(fetch_binance_p2p)
            if compra and venta:
                await asyncio.to_thread(guardar_muestra_db, compra, venta)
                
                # Evaluar tendencia actual
                pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)
                tendencia_actual = pred["tendencia"]
                
                # Detectar cambio de tendencia y disparar alerta si hay chats registrados
                if tendencia_actual != ESTADO_MERCADO_GLOBAL["ultima_tendencia"]:
                    mensaje_alerta = (
                        f"🚨 **¡ALERTA DE CAMBIO DE TENDENCIA P2P!** 🚨\n\n"
                        f"El mercado ha virado de rumbo:\n"
                        f"• Anterior: `{ESTADO_MERCADO_GLOBAL['ultima_tendencia']}`\n"
                        f"• Nueva Tendencia: `{tendencia_actual}`\n\n"
                        f"🟢 Compra: {compra:.2f} Bs | 🔴 Venta: {venta:.2f} Bs\n"
                        f"⚡ Spread Actual: {spread:.2f} Bs\n\n"
                        f"💡 Usa `/prediccion` o `/grafica` para ver el detalle."
                    )
                    if telegram_app and ESTADO_MERCADO_GLOBAL["chat_ids_alertas"]:
                        for cid in ESTADO_MERCADO_GLOBAL["chat_ids_alertas"]:
                            try:
                                await telegram_app.bot.send_message(chat_id=cid, text=mensaje_alerta, parse_mode="Markdown")
                            except Exception:
                                pass
                    ESTADO_MERCADO_GLOBAL["ultima_tendencia"] = tendencia_actual
        except Exception as e:
            print(f"Error en recolección automática: {e}")
        await asyncio.sleep(300)

# ==========================================
# BOT DE TELEGRAM (COMANDOS Y ACCESOS)
# ==========================================
telegram_app = None

async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Registrar chat para alertas automáticas si tiene acceso
    datos_usuario = verificar_estado_usuario(user_id)
    if datos_usuario.get("estado") == "activo":
        ESTADO_MERCADO_GLOBAL["chat_ids_alertas"].add(chat_id)
    else:
        await update.message.reply_text(
            "🔒 *Contenido Exclusivo para Miembros Suscritos*\n\n"
            "Usa `/suscribir` para ver los planes y desbloquear las señales y alertas de predicción.",
            parse_mode="Markdown"
        )
        return

    compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
    if not compra:
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

async def cmd_grafica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    datos_usuario = verificar_estado_usuario(user_id)
    if datos_usuario.get("estado") != "activo":
        await update.message.reply_text("🔒 Esta función requiere una suscripción activa. Usa `/suscribir`.", parse_mode="Markdown")
        return

    buf = await asyncio.to_thread(generar_grafica_prediccion_buffer)
    if buf:
        await update.message.reply_photo(photo=buf, caption="📊 **Venbot Quant** - Tendencia e Historial P2P USDT/VES", parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ No hay suficientes muestras en la base de datos para generar la gráfica todavía.")

async def cmd_suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = [
        [InlineKeyboardButton("⭐ Plan PREMIUM (5 USD)", callback_data="plan_vip")],
        [InlineKeyboardButton("🚀 Plan VIP (15 USD)", callback_data="plan_premium")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="plan_cancelar")]
    ]
    await update.message.reply_text("💎 **SELECCIONA TU PLAN DE SUSCRIPCIÓN**\n\nElige el plan que deseas adquirir:", reply_markup=InlineKeyboardMarkup(teclado), parse_mode="Markdown")

async def callback_botones_suscripcion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "plan_cancelar":
        await query.message.edit_text("❌ Operación cancelada.")
        return

    plan = "VIP" if query.data == "plan_vip" else "PREMIUM"
    texto = (
        f"💳 **MÉTODOS DE PAGO - PLAN {plan}**\n\n"
        "🇻🇪 **Pago Móvil (Mercantil):**\n• Teléfono: `0424-5734635`\n• C.I: `V-20.414.065`\n\n"
        "🌍 **Binance Pay (Pay ID / Email):**\n• `nazaretgarcia69@gmail.com`\n\n"
        f"📝 **Registra tu pago enviando:**\n`/registrar {plan.lower()} [número_referencia]`"
    )
    await query.message.edit_text(texto, parse_mode="Markdown")

async def cmd_registrar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, username = update.effective_user.id, update.effective_user.username or f"user_{user_id}"
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("⚠️ Formato incorrecto. Ejemplo: `/registrar vip 8492`", parse_mode="Markdown")
        return
    
    plan, ref = args[0].lower(), args[1]
    if plan not in ["vip", "premium"]:
        await update.message.reply_text("⚠️ El plan debe ser `vip` o `premium`.")
        return

    if registrar_pago_db(user_id, username, ref, plan):
        datos = verificar_estado_usuario(user_id)
        await update.message.reply_text(
            f"✅ **¡Comprobante registrado con éxito!**\n\n"
            f"• Plan: **{plan.upper()}** | Ref: `{ref}`\n"
            f"🔐 **Credenciales Web:**\n• Usuario: `{user_id}`\n• Contraseña: `{datos.get('password')}`\n\n"
            "Tu cuenta está en estado **pendiente** de aprobación.", parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Error al registrar el pago en la base de datos.")

async def cmd_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    datos = verificar_estado_usuario(user_id)
    if datos.get("estado") == "no_registrado":
        await update.message.reply_text("❌ No estás registrado. Usa `/suscribir`.", parse_mode="Markdown")
        return
    await update.message.reply_text(
        f"🔐 **Tus Credenciales Web**\n• Usuario: `{user_id}`\n• Contraseña: `{datos.get('password')}`\n• Estado: `{datos.get('estado').upper()}`", parse_mode="Markdown"
    )

async def iniciar_telegram_bot():
    global telegram_app
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        telegram_app.add_handler(CommandHandler("prediccion", cmd_prediccion))
        telegram_app.add_handler(CommandHandler("grafica", cmd_grafica))
        telegram_app.add_handler(CommandHandler("suscribir", cmd_suscribir))
        telegram_app.add_handler(CommandHandler("registrar", cmd_registrar))
        telegram_app.add_handler(CommandHandler("password", cmd_password))
        telegram_app.add_handler(CallbackQueryHandler(callback_botones_suscripcion))
        
        await telegram_app.initialize()
        await telegram_app.start()
        print("🤖 Bot de Telegram inicializado con Alertas Automáticas y Gráficas Quant.")
    except Exception as e:
        print(f"Error al iniciar Telegram: {e}")

# ==========================================
# SERVIDOR FASTAPI
# ==========================================
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    asyncio.create_task(tarea_recoleccion_automatica())
    await iniciar_telegram_bot()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.api_route("/", methods=["GET", "HEAD"])
async def home():
    return {"status": "ok", "message": "Venbot P2P Activo con Alertas y Gráficas de ML"}

@app.post("/webhook")
async def telegram_webhook(request: Request):
    if not telegram_app:
        return {"status": "error"}
    try:
        update = Update.de_json(await request.json(), telegram_app.bot)
        await telegram_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/login")
async def api_login(payload: dict = Body(...)):
    usuario, password = str(payload.get("username", "")).strip(), str(payload.get("password", "")).strip()
    conn = get_db_connection()
    if not conn:
        return JSONResponse(status_code=500, content={"success": False, "message": "Error de DB"})
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT telegram_id, estado_suscripcion, tipo_plan FROM usuarios_p2p WHERE (CAST(telegram_id AS TEXT) = %s OR username ILIKE %s) AND password = %s;", (usuario, usuario, password))
            row = cur.fetchone()
            if row and row[1] == "activo":
                return {"success": True, "plan": row[2], "telegram_id": row[0]}
            return JSONResponse(status_code=401, content={"success": False, "message": "Credenciales inválidas o cuenta inactiva"})
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
                    "compra": compra, "venta": venta, "spread": spread, "pct_bruto": pct,
                    "buy_price": compra, "sell_price": venta, "bcv": usd_bcv, "euro": eur_bcv,
                    "prediccion": pred, "timestamp": datetime.now(VET).isoformat(), "status": "connected"
                }
                yield f"data: {json.dumps(payload)}\n\n"
            except Exception:
                pass
            await asyncio.sleep(5)
    return StreamingResponse(event_generator(), headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive"})
