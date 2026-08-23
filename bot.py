import sqlite3
import requests
import datetime
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================
TOKEN_TELEGRAM = "TU_TOKEN_DE_TELEGRAM_AQUI"
RENDER_URL = "https://p2p-monitor-binance.onrender.com"
DB_NAME = "database.db"

app = Flask(__name__)
CORS(app)

# ==========================================
# BASE DE DATOS Y PERSISTENCIA
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historial_p2p (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            precio_compra REAL,
            precio_venta REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def guardar_registro(compra, venta):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO historial_p2p (precio_compra, precio_venta) VALUES (?, ?)", 
        (compra, venta)
    )
    conn.commit()
    conn.close()

# ==========================================
# MOTOR DE SCRAPING DE BINANCE P2P
# ==========================================
def obtener_precios_binance():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {"Content-Type": "application/json"}
    
    # Bancos filtrados: Provincial, Mercantil, BNC
    pay_types = ["BBVA", "Mercantil", "BNC"]

    try:
        # Petición Compra (Recompra - Bloque 10k)
        body_buy = {
            "asset": "USDT", "fiat": "VES", "merchantCheck": False,
            "page": 1, "rows": 10, "tradeType": "BUY", "transAmount": 10000,
            "payTypes": pay_types
        }
        res_buy = requests.post(url, json=body_buy, headers=headers, timeout=10).json()
        compra = float(res_buy['data'][0]['adv']['price']) if res_buy.get('data') else 0.0

        # Petición Venta (Venta - Bloque 300k)
        body_sell = {
            "asset": "USDT", "fiat": "VES", "merchantCheck": False,
            "page": 1, "rows": 10, "tradeType": "SELL", "transAmount": 300000,
            "payTypes": pay_types
        }
        res_sell = requests.post(url, json=body_sell, headers=headers, timeout=10).json()
        venta = float(res_sell['data'][0]['adv']['price']) if res_sell.get('data') else 0.0

        if compra > 0 and venta > 0:
            guardar_registro(compra, venta)

        return compra, venta
    except Exception as e:
        print(f"Error consultando Binance: {e}")
        return 0.0, 0.0

# ==========================================
# MOTOR IA QUANT INTELIGENTE (PROYECCIÓN)
# ==========================================
def calcular_prediccion_ia():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Toma hasta 200 muestras para calcular piso y techo amplios
    cursor.execute('''
        SELECT precio_compra, precio_venta, timestamp 
        FROM historial_p2p 
        ORDER BY id DESC LIMIT 200
    ''')
    registros = cursor.fetchall()
    conn.close()

    if not registros:
        return None

    total_muestras = len(registros)
    compras = [r[0] for r in registros]
    ventas = [r[1] for r in registros]

    compra_actual = compras[0]
    venta_actual = ventas[0]

    # Piso y techo reales sobre el historial registrado
    piso_real = min(compras)
    techo_real = max(ventas)

    # Cálculo dinámico de medias móviles
    if total_muestras >= 3:
        media_compra = sum(compras) / total_muestras
        media_venta = sum(ventas) / total_muestras

        # IA Ponderada: combina el valor actual con la tendencia histórica
        peso_historico = 0.35
        pred_compra = (compra_actual * (1 - peso_historico)) + (media_compra * peso_historico)
        pred_venta = (venta_actual * (1 - peso_historico)) + (media_venta * peso_historico)
        
        diff = compras[0] - compras[-1]
        if diff > 0.3:
            tendencia = "ALCISTA 🚀"
        elif diff < -0.3:
            tendencia = "BAJISTA 📉"
        else:
            tendencia = "ESTABLE / LATERAL ↔️"
    else:
        # Ajuste inteligente para pocas muestras tras reinicio
        pred_compra = compra_actual * 0.9985
        pred_venta = venta_actual * 1.0015
        piso_real = compra_actual * 0.995
        techo_real = venta_actual * 1.005
        tendencia = "ESTABLE / LATERAL ↔️"

    return {
        "pred_compra_str": f"{pred_compra:.2f} Bs",
        "pred_venta_str": f"{pred_venta:.2f} Bs",
        "piso_str": f"{piso_real:.2f} Bs",
        "techo_str": f"{techo_real:.2f} Bs",
        "tendencia": tendencia,
        "muestras": total_muestras
    }

# ==========================================
# RUTAS DE LA API WEB
# ==========================================
@app.route('/api/actual', methods=['GET'])
def api_actual():
    compra, venta = obtener_precios_binance()
    spread = venta - compra
    pct = (spread / compra * 100) if compra > 0 else 0
    pred = calcular_prediccion_ia()

    return jsonify({
        "compra": compra,
        "venta": venta,
        "spread": spread,
        "pct_bruto": round(pct, 2),
        "pred": pred
    })

@app.route('/api/historial', methods=['GET'])
def api_historial():
    rango = request.args.get('rango', '1d')
    limite = 24 if rango == '1d' else (168 if rango == '7d' else 720)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT precio_compra, precio_venta, timestamp 
        FROM historial_p2p 
        ORDER BY id DESC LIMIT ?
    ''', (limite,))
    registros = cursor.fetchall()
    conn.close()

    registros.reverse()
    labels = [r[2].split()[1][:5] if ' ' in r[2] else r[2] for r in registros]
    compras = [r[0] for r in registros]
    ventas = [r[1] for r in registros]

    return jsonify({
        "labels": labels,
        "compras": compras,
        "ventas": ventas
    })

# ==========================================
# COMANDOS BOT DE TELEGRAM
# ==========================================
async def cmd_prediccion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    compra, venta = obtener_precios_binance()
    spread = venta - compra
    pct = (spread / compra * 100) if compra > 0 else 0
    pred = calcular_prediccion_ia()

    hora_actual = datetime.datetime.now().strftime("%I:%M %p")

    mensaje = (
        f"<b>VENBOT PREDICCIONES</b>\n"
        f"⏰ {hora_actual} | BLOQUE 4\n\n"
        f"🟢 <b>COMPRA (10k):</b> {compra:.2f} Bs\n"
        f"🔴 <b>VENTA (300k):</b> {venta:.2f} Bs\n"
        f"⚡ <b>MARGEN:</b> {spread:.2f} Bs ({pct:.2f}%)\n"
        f"───────────────────\n"
        f"🔮 <b>PROYECCIÓN +7H (IA QUANT)</b>\n"
        f"• Recompra Esperada: <b>{pred['pred_compra_str']}</b>\n"
        f"• Venta Esperada: <b>{pred['pred_venta_str']}</b>\n"
        f"• Dirección: <b>{pred['tendencia']}</b>\n"
        f"───────────────────\n"
        f"📊 Piso: <b>{pred['piso_str']}</b> | Techo: <b>{pred['techo_str']}</b>\n"
        f"🧠 Base de Datos: <b>{pred['muestras']} Muestras</b>"
    )

    await update.message.reply_text(mensaje, parse_mode="HTML")

# ==========================================
# INICIALIZACIÓN
# ==========================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
