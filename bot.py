import os
import asyncio
import psycopg2
import requests
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body
from fastapi.responses import Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# SDK Oficial de Gemini
from google import genai
from google.genai import types

VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

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
# LECTURA P2P BINANCE (REFERENCIAS BANCARIAS)
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
        res_c = requests.post(url, json=payload_compra, headers=headers, timeout=8).json()
        res_v = requests.post(url, json=payload_venta, headers=headers, timeout=8).json()

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
# GEMINI IA - ANÁLISIS COHERENTE FRONTEND Y BOT
# ==========================================
def obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia_quant, pred_compra, pred_venta):
    if not gemini_client:
        return {
            "estado_actual": f"Mercado P2P en {actual_compra:.2f} Bs y {actual_venta:.2f} Bs. Spread: {spread:.2f} Bs.",
            "proyeccion_7_12h": f"Tendencia {tendencia_quant}. Recompra esperada en {pred_compra:.2f} Bs.",
            "recomendacion_tactica": "Mantener postura competitiva en anuncios de compra y rotación continua.",
            "tactica": {
                "texto": "Momento de publicar anuncios de compra y rotar saldo rápido.",
                "senal": "COMPRA MODERADA", "velocidad": "ALTA (< 5 min)", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} - {pred_venta:.2f} Bs"
            },
            "flujo": {
                "texto": "Absorción constante de oferta en la punta competitiva.",
                "dominio": "COMPRADORES ACTIVOS", "spread_status": f"{spread:.2f} Bs", "riesgo": "BAJO", "proyeccion_12h": f"{pred_venta:.2f} Bs"
            },
            "niveles": {
                "texto": "Rango definido entre soporte actual y techo estimado.",
                "momentum": "MEDIO (65%)", "liquidez": "ESTABLE", "quiebre": f"{actual_compra:.2f} Bs", "techo": f"{pred_venta:.2f} Bs"
            }
        }

    try:
        system_instruction = (
            "Eres el asesor cuantitativo de VENBOT para comerciantes P2P NO VERIFICADOS en Binance Venezuela. "
            "Genera un informe unificado en JSON orientado a la operativa de anuncios (USDT/VES) sin contradicciones."
        )

        prompt = f"""
        Datos P2P Binance: Compra {actual_compra:.2f} Bs, Venta {actual_venta:.2f} Bs, Spread {spread:.2f} Bs.
        Proyección 7-12h: Recompra {pred_compra:.2f} Bs, Venta {pred_venta:.2f} Bs, Tendencia {tendencia_quant}.

        Genera este JSON exacto:
        {{
          "estado_actual": "Diagnóstico rápido del mercado para Telegram (1 frase).",
          "proyeccion_7_12h": "Lectura a 7-12h para Telegram (1 frase).",
          "recomendacion_tactica": "Acción directa del comerciante P2P para Telegram (1 frase).",
          "tactica": {{
            "texto": "Análisis pestaña táctica del web UI.",
            "senal": "COMPRA FUERTE | COMPRA MODERADA | ESPERAR",
            "velocidad": "ALTA (< 5 min) | MEDIA",
            "sombra": "ESCASEZ DE USDT | NORMAL",
            "rango": "{actual_compra:.2f} - {pred_venta:.2f} Bs"
          }},
          "flujo": {{
            "texto": "Análisis pestaña flujo del web UI.",
            "dominio": "COMPRADORES AGRESIVOS | LATERAL",
            "spread_status": "{spread:.2f} Bs (Excelente)",
            "riesgo": "BAJO | MEDIO",
            "proyeccion_12h": "{pred_venta:.2f} Bs"
          }},
          "niveles": {{
            "texto": "Análisis pestaña niveles del web UI.",
            "momentum": "ALTO (80%) | MEDIO",
            "liquidez": "ABUNDANTE | ESTABLE",
            "quiebre": "{pred_compra:.2f} Bs",
            "techo": "{pred_venta:.2f} Bs"
          }}
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
        print(f"Error Gemini: {e}")
        return {
            "estado_actual": f"Mercado P2P en {actual_compra:.2f} / {actual_venta:.2f} Bs.",
            "proyeccion_7_12h": f"Tendencia {tendencia_quant}.",
            "recomendacion_tactica": "Ajustar anuncios en la punta competitiva.",
            "tactica": {"texto": "Mercado estable.", "senal": "COMPRA MODERADA", "velocidad": "MEDIA", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} Bs"},
            "flujo": {"texto": "Rotación normal.", "dominio": "LATERAL", "spread_status": f"{spread:.2f} Bs", "riesgo": "MEDIO", "proyeccion_12h": f"{actual_venta:.2f} Bs"},
            "niveles": {"texto": "Rango acotado.", "momentum": "MEDIO", "liquidez": "ESTABLE", "quiebre": f"{actual_compra:.2f} Bs", "techo": f"{actual_venta:.2f} Bs"}
        }

# ==========================================
# MOTOR QUANT
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

        ventana_reciente = min(total_muestras, 30)
        x = np.arange(ventana_reciente)
        y_c = compras[-ventana_reciente:]
        slope_c, _ = np.polyfit(x, y_c, 1)

        pasos_7h = 84
        factor_amortiguacion = 0.35 
        delta_proyectado = slope_c * pasos_7h * factor_amortiguacion

        pred_c = round(actual_compra + delta_proyectado, 2)
        spread_historico_promedio = np.mean(ventas - compras)
        pred_v = round(pred_c + spread_historico_promedio, 2)

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
            compra, venta, _, _ = await asyncio.to_thread(fetch_binance_p2p)
            if compra and venta:
                await asyncio.to_thread(guardar_muestra_db, compra, venta)
        except Exception as e:
            print(f"Error en recolección: {e}")
        await asyncio.sleep(300)

# ==========================================
# COMANDO TELEGRAM (ESTRUCTURA EXACTA ORIGINAL)
# ==========================================
async def prediccion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
    if not compra:
        await update.message.reply_text("❌ Error al consultar Binance P2P.")
        return

    pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)
    hora_ve = datetime.now(VET).strftime("%I:%M %p")

    msg = (
        f"🦜 <b>VENBOT PREDICCIONES</b>\n"
        f"🕒 ({hora_ve}) | BLOQUE 4\n"
        f"🟢 <b>COMPRA (10k):</b> {compra:.2f} Bs\n"
        f"🔴 <b>VENTA (300k):</b> {venta:.2f} Bs\n"
        f"⚡ <b>MARGEN:</b> {spread:.2f} Bs ({pct:.2f}%)\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🔮 <b>PROYECCIÓN +7H (IA QUANT)</b>\n"
        f"🟢 Recompra Esperada: <b>{pred['pred_compra_str']}</b>\n"
        f"🔴 Venta Esperada: <b>{pred['pred_venta_str']}</b>\n"
        f"🎯 Dirección: <b>{pred['tendencia']}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"📊 Piso: <b>{pred['piso_str']}</b> | Techo: <b>{pred['techo_str']}</b>\n"
        f"💾 Base de Datos: <b>{pred['muestras']} Muestras</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

# ==========================================
# SERVIDOR FASTAPI Y CSS FRONTEND
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

@app.get("/styles.css")
def get_custom_css():
    css_content = """
    body {
      background-color: #0B1120 !important;
      color: #FFFFFF !important;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
    }

    p, span, label, div, .description, .ia-report-text {
      color: #E2E8F0 !important;
      font-size: 0.95rem !important;
      line-height: 1.5 !important;
    }

    .text-secondary, .text-muted, small, footer p, .ia-disclaimer-text {
      color: #94A3B8 !important;
    }

    h1, h2, h3, h4, h5, .card-title, .metric-label {
      color: #38BDF8 !important;
      font-weight: 600 !important;
    }

    .metric-value, .highlight-text {
      color: #FFFFFF !important;
      font-weight: 700 !important;
    }
    """
    return Response(content=css_content, media_type="text/css")

@app.get("/api/actual")
def get_actual():
    compra, venta, spread, pct = fetch_binance_p2p()
    if not compra:
        return JSONResponse(
            content={"error": "Sin datos"},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    pred = motor_quant_inteligente(compra, venta)
    data = {
        "compra": compra, 
        "venta": venta, 
        "spread": spread, 
        "pct_bruto": pct, 
        "bcv": 898.50,
        "prediccion": pred
    }
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )

@app.get("/api/historico")
def get_historico(periodo: str = "1d"):
    filas = obtener_estadisticas_db()
    resultado = []
    for f in filas:
        try:
            hora_f = datetime.fromisoformat(f[2]).strftime("%I:%M %p")
        except:
            hora_f = "12:00"
        resultado.append({"hora": hora_f, "compra": f[0], "venta": f[1]})
    return JSONResponse(
        content=resultado,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )

@app.post("/api/chat")
def api_chat(payload: dict = Body(...)):
    prompt = payload.get("prompt", "").lower()
    compra, venta, spread, _ = fetch_binance_p2p()
    pred = motor_quant_inteligente(compra, venta)
    ia = pred["analisis_ia"]
    
    if "precio" in prompt or "cuanto" in prompt:
        respuesta = f"La compra P2P está en {compra:.2f} Bs y la venta en {venta:.2f} Bs."
    elif "comprar" in prompt:
        respuesta = f"Tasa recomendada para comprar P2P: {compra:.2f} Bs."
    elif "vender" in prompt:
        respuesta = f"Tasa recomendada para vender P2P: {venta:.2f} Bs."
    else:
        respuesta = f"Diagnóstico: {ia.get('estado_actual', '')} Recomendación: {ia.get('recomendacion_tactica', '')}"

    return {"response": respuesta}

@app.get("/api/historial")
def get_historial():
    filas = obtener_estadisticas_db()
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    return JSONResponse(
        content={"compras": compras, "ventas": ventas, "total": len(filas)},
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )
