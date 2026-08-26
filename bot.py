import os
import asyncio
import psycopg2
import requests
import json
import numpy as np
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body
from fastapi.responses import Response, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from google import genai
from google.genai import types

# Deshabilitar advertencias SSL en caso de fluctuaciones en el certificado del BCV
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración de zona horaria Venezuela
VET = timezone(timedelta(hours=-4))
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ==========================================
# AUTODESCUBRIMIENTO AUTÓNOMO DE MODELOS IA
# ==========================================
def obtener_modelo_gemini_activo() -> str:
    """Consulta de forma autónoma los modelos disponibles para usar siempre el Flash más reciente."""
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
    except Exception as e:
        print(f"Aviso en autodescubrimiento de modelo: {e}. Usando respaldo predeterminado.")
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
# GEMINI IA (EXCLUSIVO P2P BINANCE)
# ==========================================
def obtener_analisis_ia_coherente(actual_compra, actual_venta, spread, tendencia_quant, pred_compra, pred_venta):
    if not gemini_client:
        return {
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

    try:
        system_instruction = (
            "Eres el analista de mercado P2P para VENBOT en Binance Venezuela (USDT/VES). "
            "PROHIBIDO ABSOLUTAMENTE: Mencionar BCV, tasa oficial, Euro, brechas cambiarías institucionales o entes gubernamentales. "
            "Tus respuestas deben tratar exclusivamente sobre: libro de órdenes P2P, spread, punta de compra/venta y estrategia de anuncios."
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
            "texto": "Lectura operativa P2P. Evalúa la dinámica entre anuncios de compra y venta en Binance.",
            "senal": "COMPRA FUERTE | COMPRA MODERADA | ESPERAR",
            "velocidad": "ALTA (< 5 min) | MEDIA",
            "sombra": "ESCASEZ DE USDT | NORMAL",
            "rango": "{actual_compra:.2f} - {pred_venta:.2f} Bs"
          }},
          "flujo": {{
            "texto": "Análisis del flujo de liquidez P2P y velocidad de ejecución de órdenes.",
            "dominio": "COMPRADORES AGRESIVOS | LATERAL | VENDEDORES ACTIVOS",
            "spread_status": "{spread:.2f} Bs",
            "riesgo": "BAJO | MEDIO | ALTO",
            "proyeccion_12h": "{pred_venta:.2f} Bs"
          }},
          "niveles": {{
            "texto": "Evaluación del soporte de compra y resistencia de venta en el libro P2P.",
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

        return json.loads(response.text)
    except Exception as e:
        print(f"Error Gemini: {e}")
        return {
            "estado_actual": f"Mercado P2P cotizando en {actual_compra:.2f} Bs compra y {actual_venta:.2f} Bs venta.",
            "proyeccion_7_12h": f"Tendencia general P2P: {tendencia_quant}.",
            "recomendacion_tactica": "Ajustar anuncios P2P en el primer bloque competitivo.",
            "tactica": {"texto": f"Spread de {spread:.2f} Bs. Rotación regular de USDT.", "senal": "COMPRA MODERADA", "velocidad": "MEDIA", "sombra": "NORMAL", "rango": f"{actual_compra:.2f} Bs"},
            "flujo": {"texto": "Volumen P2P operando dentro del canal proyectado.", "dominio": "LATERAL", "spread_status": f"{spread:.2f} Bs", "riesgo": "MEDIO", "proyeccion_12h": f"{actual_venta:.2f} Bs"},
            "niveles": {"texto": "Límites operativos del mercado P2P delimitados.", "momentum": "MEDIO", "liquidez": "ESTABLE", "quiebre": f"{actual_compra:.2f} Bs", "techo": f"{actual_venta:.2f} Bs"}
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
# SERVIDOR FASTAPI Y ENDPOINTS ASÍNCRONOS
# ==========================================
@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    asyncio.create_task(tarea_recoleccion_automatica())
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
    return {"status": "ok", "message": "Venbot P2P Activo"}

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

@app.get("/styles.css")
async def get_custom_css():
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
async def get_actual():
    compra, venta, spread, pct = await asyncio.to_thread(fetch_binance_p2p)
    usd_bcv, eur_bcv = await asyncio.to_thread(obtener_tasas_oficiales_bcv)
    
    if not compra:
        return JSONResponse(
            content={"error": "Sin datos"},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
        )
    pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)
    data = {
        "compra": compra, 
        "venta": venta, 
        "spread": spread, 
        "pct_bruto": pct, 
        "diferencia": spread,
        "bcv": usd_bcv,
        "euro": eur_bcv,
        "prediccion": pred
    }
    return JSONResponse(
        content=data,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.get("/api/historico")
async def get_historico(periodo: str = "1d"):
    filas = await asyncio.to_thread(obtener_estadisticas_db)
    resultado = []
    for f in filas:
        try:
            hora_f = datetime.fromisoformat(f[2]).strftime("%I:%M %p")
        except:
            hora_f = "12:00"
        resultado.append({"hora": hora_f, "compra": f[0], "venta": f[1]})
    return JSONResponse(
        content=resultado,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.post("/api/chat")
async def api_chat(payload: dict = Body(...)):
    prompt = payload.get("prompt", "").lower()
    compra, venta, spread, _ = await asyncio.to_thread(fetch_binance_p2p)
    pred = await asyncio.to_thread(motor_quant_inteligente, compra, venta)
    ia = pred["analisis_ia"]
    
    if "precio" in prompt or "cuanto" in prompt:
        respuesta = f"La compra P2P está en {compra:.2f} Bs y la venta en {venta:.2f} Bs."
    elif "comprar" in prompt:
        respuesta = f"Tasa recomendada para comprar P2P: {compra:.2f} Bs."
    elif "vender" in prompt:
        respuesta = f"Tasa recomendada para vender P2P: {venta:.2f} Bs."
    else:
        respuesta = f"Diagnóstico P2P: {ia.get('estado_actual', '')} Recomendación: {ia.get('recomendacion_tactica', '')}"

    return {"response": respuesta}

@app.get("/api/historial")
async def get_historial():
    filas = await asyncio.to_thread(obtener_estadisticas_db)
    compras = [f[0] for f in filas]
    ventas = [f[1] for f in filas]
    return JSONResponse(
        content={"compras": compras, "ventas": ventas, "total": len(filas)},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )
