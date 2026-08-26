import os
import asyncio
import psycopg2
import requests
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Integración con la SDK Oficial de Gemini
from google import genai
from google.genai import types

VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Inicialización del cliente de Gemini si existe la API Key
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

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
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historial (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT,
                    compra REAL,
                    venta REAL
                );
            ''')
            conn.commit()
        conn.close()

init_db()

def guardar_muestra_db(compra, venta):
    """Guarda muestras y mantiene una ventana deslizante de 2000 registros"""
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
            conn.close()
        except Exception as e:
            print(f"Error guardando en DB: {e}")

def obtener_estadisticas_db(limit=2000):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT compra, venta, timestamp FROM historial ORDER BY id ASC LIMIT %s;", (limit,))
                filas = cursor.fetchall()
            conn.close()
            return filas
        except Exception as e:
            print(f"Error leyendo DB: {e}")
            return []
    return []

# ==========================================
# LECTURA P2P BINANCE
# ==========================================
def fetch_binance_p2p():
    """Consulta rápida a Binance P2P sin escribir en la DB"""
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
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=8).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=8).json()

        data_c = res_c.get("data", [])
        data_v = res_v.get("data", [])

        if not data_c or not data_v:
            return None, None, None, None

        precios_compra = [float(item["adv"]["price"]) for item in data_c]
        precios_venta = [float(item["adv"]["price"]) for item in data_v]

        tasa_compra = min(precios_compra)
        tasa_venta = max(precios_venta)

        if tasa_compra >= tasa_venta:
            tasa_compra, tasa_venta = min(precios_compra[0], precios_venta[0]), max(precios_compra[0], precios_venta[0])

        spread = round(tasa_venta - tasa_compra, 2)
        pct_bruto = round((spread / tasa_compra) * 100, 2) if tasa_compra > 0 else 0.0

        return tasa_compra, tasa_venta, spread, pct_bruto
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return None, None, None, None

# ==========================================
# GEMINI IA - ANÁLISIS ENFOCADO EN COMERCIANTE P2P
# ==========================================
def obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia_quant, pred_compra, pred_venta):
    """Genera un análisis enfocado exclusivamente en la operativa del comerciante P2P no verificado"""
    if not gemini_client:
        return {
            "estado_actual": f"Mercado P2P operando con compra en {actual_compra:.2f} Bs y venta en {actual_venta:.2f} Bs (Spread: {spread:.2f} Bs).",
            "proyeccion_7_12h": f"Tendencia {tendencia_quant}. Recompra esperada cerca de {pred_compra:.2f} Bs a 7-12h.",
            "recomendacion_tactica": "Mantener rotación de inventario ajustando anuncios en la punta competitiva del mercado P2P."
        }

    try:
        system_instruction = (
            "Eres un asesor cuantitativo especializado en la operativa del comerciante P2P no verificado de Binance en Venezuela. "
            "El usuario es un comerciante que busca publicar anuncios de compra y venta para obtener spread en USDT/VES. "
            "DEBES redactar el informe 100% enfocado en su postura operativa (cuándo colocar órdenes de compra, cuándo vender, "
            "gestión de inventario y velocidad de rotación). Usa un tono profesional, claro y directo. "
            "Mantén una sola tesis lógica de mercado (alcista, bajista o lateral) para garantizar coherencia estricta entre las 3 secciones."
        )

        prompt = f"""
        Datos actuales del mercado Binance P2P:
        - Tasa de Compra Anuncio: {actual_compra:.2f} Bs
        - Tasa de Venta Anuncio: {actual_venta:.2f} Bs
        - Spread Operativo: {spread:.2f} Bs
        - Tendencia Quant: {tendencia_quant}
        - Recompra Proyectada (7-12h): {pred_compra:.2f} Bs
        - Venta Proyectada (7-12h): {pred_venta:.2f} Bs

        Genera una respuesta en formato JSON con la siguiente estructura exacta:
        {{
          "estado_actual": "Diagnóstico de liquidez y spread enfocado en la posición del comerciante P2P (1-2 frases).",
          "proyeccion_7_12h": "Lectura a 7-12h indicando al comerciante si acumular USDT o rotar saldo rápido (1-2 frases).",
          "recomendacion_tactica": "Estrategia directa de posicionamiento de anuncios (compra/venta) para el comerciante P2P (1-2 frases)."
        }}
        """

        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                temperature=0.2
            ),
        )

        return json.loads(response.text)
    except Exception as e:
        print(f"Error generando análisis con Gemini: {e}")
        return {
            "estado_actual": f"Mercado P2P cotizando en {actual_compra:.2f} Bs / {actual_venta:.2f} Bs.",
            "proyeccion_7_12h": f"Dirección del mercado estimada: {tendencia_quant}.",
            "recomendacion_tactica": "Ajustar anuncios de compra y venta según la liquidez de la bancafactorial disponible."
        }

# ==========================================
# MOTOR QUANT DE ALTA PRECISIÓN + IA
# ==========================================
def motor_quant_inteligente(actual_compra, actual_venta):
    filas = obtener_estadisticas_db()
    total_muestras = len(filas)

    if total_muestras < 10:
        pred_c = round(actual_compra * 0.999, 2)
        pred_v = round(actual_venta * 1.001, 2)
        tendencia = "➖ ESTABLE / LATERAL"
        direccion = "LATERAL"
        piso = actual_compra
        techo = actual_venta
    else:
        compras = np.array([f[0] for f in filas])
        ventas = np.array([f[1] for f in filas])

        piso = np.min(compras)
        techo = np.max(ventas)

        # 1. MEDIA MÓVIL EXPONENCIAL (EMA-12)
        alpha = 2 / (12 + 1)
        ema_c = compras[0]
        ema_v = ventas[0]
        for i in range(1, total_muestras):
            ema_c = (compras[i] * alpha) + (ema_c * (1 - alpha))
            ema_v = (ventas[i] * alpha) + (ema_v * (1 - alpha))

        # 2. CÁLCULO DE LA TENDENCIA
        ventana_reciente = min(total_muestras, 30)
        x = np.arange(ventana_reciente)
        y_c = compras[-ventana_reciente:]
        slope_c, _ = np.polyfit(x, y_c, 1)

        # 3. PROYECCIÓN A +7 HORAS
        pasos_7h = 84
        factor_amortiguacion = 0.35 
        delta_proyectado = slope_c * pasos_7h * factor_amortiguacion

        pred_c = round(actual_compra + delta_proyectado, 2)
        spread_historico_promedio = np.mean(ventas - compras)
        pred_v = round(pred_c + spread_historico_promedio, 2)

        # 4. CLASIFICACIÓN
        if slope_c > 0.015:
            tendencia = "🚀 ALCISTA"
            direccion = "ALCISTA"
        elif slope_c < -0.015:
            tendencia = "🔻 BAJISTA"
            direccion = "BAJISTA"
        else:
            tendencia = "➖ ESTABLE / LATERAL"
            direccion = "LATERAL"

    spread = round(actual_venta - actual_compra, 2)
    analisis_ia = obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia, pred_c, pred_v)

    return {
        "pred_compra_str": f"{pred_c:.2f} Bs",
        "pred_venta_str": f"{pred_v:.2f} Bs",
        "tendencia": tendencia,
        "direccion": direccion,
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
            compra, venta, _, _ = fetch_binance_p2p()
            if compra and venta:
                guardar_muestra_db(compra, venta)
                print(f"[{datetime.now(VET).strftime('%I:%M %p')}] Muestra guardada: Compra {compra} | Venta {venta}")
        except Exception as e:
            print(f"Error en recolección automática: {e}")
        
        await asyncio.sleep(300)

# ==========================================
# COMANDO TELEGRAM
# ==========================================
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, pct = fetch_binance_p2p()
    if not compra:
        await update.message.reply_text("❌ Error al consultar la API de Binance.")
        return

    pred = motor_quant_inteligente(compra, venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")
    ia = pred["analisis_ia"]

    msg = (
        f"🦜 <b>VENBOT PREDICCIONES P2P</b>\n"
        f"🕒 ({hora_ve}) | BLOQUE 4\n"
        f"🟢 <b>COMPRA (10k):</b> {compra:.2f} Bs\n"
        f"🔴 <b>VENTA (300k):</b> {venta:.2f} Bs\n"
        f"⚡ <b>MARGEN:</b> {spread:.2f} Bs ({pct:.2f}%)\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🔮 <b>PROYECCIÓN 7-12H (IA QUANT)</b>\n"
        f"🟢 Recompra Esperada: <b>{pred['pred_compra_str']}</b>\n"
        f"🔴 Venta Esperada: <b>{pred['pred_venta_str']}</b>\n"
        f"🎯 Dirección: <b>{pred['tendencia']}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🧠 <b>LECTURA ESTRATÉGICA P2P (IA)</b>\n"
        f"📌 <b>Estado:</b> {ia['estado_actual']}\n"
        f"📈 <b>Proyección (7-12h):</b> {ia['proyeccion_7_12h']}\n"
        f"💡 <b>Acción Comerciante:</b> {ia['recomendacion_tactica']}\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"📊 Piso: <b>{pred['piso_str']}</b> | Techo: <b>{pred['techo_str']}</b>\n"
        f"💾 Base de Datos: <b>{pred['muestras']} Muestras</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

# ==========================================
# SERVIDOR FASTAPI CON TAREAS EN SEGUNDO PLANO
# ==========================================
telegram_app = None

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    global telegram_app
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    
    asyncio.create_task(tarea_recoleccion_automatica())
    
    if token:
        telegram_app = Application.builder().token(token).build()
        telegram_app.add_handler(CommandHandler("prediccion", prediccion_cmd))
        await telegram_app.initialize()
        await telegram_app.start()
        await telegram_app.updater.start_polling()
    yield
    if telegram_app:
        await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"]
)

@app.api_route("/", methods=["GET", "HEAD"])
def home():
    return {"status": "ok", "message": "Venbot P2P Activo"}

@app.get("/api/actual")
def get_actual():
    compra, venta, spread, pct = fetch_binance_p2p()
    if not compra:
        return {"error": "Sin datos"}
    pred = motor_quant_inteligente(compra, venta)
    return {
        "compra": compra, 
        "venta": venta, 
        "spread": spread, 
        "pct_bruto": pct, 
        "bcv": 898.50,
        "prediccion": pred
    }

@app.get("/api/historico")
def get_historico(periodo: str = "1d"):
    filas = obtener_estadisticas_db()
    resultado = []
    
    for f in filas:
        try:
            hora_f = datetime.fromisoformat(f[2]).strftime("%I:%M %p")
        except:
            hora_f = "12:00"
            
        resultado.append({
            "hora": hora_f,
            "compra": f[0],
            "venta": f[1]
        })
    return resultado

@app.post("/api/chat")
def api_chat(payload: dict = Body(...)):
    prompt = payload.get("prompt", "").lower()
    compra, venta, spread, _ = fetch_binance_p2p()
    pred = motor_quant_inteligente(compra, venta)
    ia = pred["analisis_ia"]
    
    if "precio" in prompt or "cuanto" in prompt:
        respuesta = f"Actualmente el anuncio de compra P2P está en {compra:.2f} Bs y el de venta en {venta:.2f} Bs."
    elif "comprar" in prompt:
        respuesta = f"La mejor tasa para publicar anuncio de compra en bancos seleccionados es {compra:.2f} Bs."
    elif "vender" in prompt:
        respuesta = f"Puedes publicar tu anuncio de venta a una tasa media de {venta:.2f} Bs."
    elif "tendencia" in prompt or "proyeccion" in prompt:
        respuesta = f"IA P2P: {ia['proyeccion_7_12h']} (Estrategia sugerida: {ia['recomendacion_tactica']})"
    else:
        respuesta = f"Hola, soy VenBot AI. Estado actual del mercado P2P: {ia['estado_actual']} Recomendación para el comerciante: {ia['recomendacion_tactica']}"

    return {"response": respuesta}

@app.get("/api/historial")
def get_historial():
    filas = obtener_estadisticas_db()
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    return {"compras": compras, "ventas": ventas, "total": len(filas)}
