import os
import time
import sqlite3
import requests
import schedule
import asyncio
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# REEMPLAZA CON TU TOKEN DE BOTFATHER
TELEGRAM_TOKEN = "8579313357:AAGfJ4NfawMpcA1f1gRGTUAZCvEfl0ZbLZM"
DB_URL = "postgresql://postgres.ozowlqqxsiqkfklzakjb:[AbrilAlessandro30$]@aws-0-us-west-2.pooler.supabase.com:5432/postgres"
def init_db():
    conn = sqlite3.connect("p2p_data.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            bank TEXT,
            buy_price REAL,
            sell_price REAL
        )
    ''')
    conn.commit()
    conn.close()

def fetch_and_store_binance():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = {
        "asset": "USDT", "fiat": "VES", "merchantCheck": False,
        "page": 1, "payTypes": ["BBVA"], "rows": 1, "tradeType": "BUY", "transAmount": "250000"
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        data = res.json()
        if data.get("data"):
            buy_price = float(data["data"][0]["adv"]["price"])
            conn = sqlite3.connect("p2p_data.db")
            cursor = conn.cursor()
            cursor.execute("INSERT INTO prices (bank, buy_price, sell_price) VALUES (?, ?, ?)", ("BBVA", buy_price, buy_price + 10.0))
            conn.commit()
            conn.close()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Precio guardado: {buy_price} Bs")
    except Exception as e:
        print(f"Error al conectar con Binance: {e}")

def calculate_prediction():
    conn = sqlite3.connect("p2p_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT buy_price FROM prices ORDER BY id DESC LIMIT 60")
    rows = cursor.fetchall()
    conn.close()

    if len(rows) < 5:
        return "Insuficientes datos acumulados. Recolectando información de mercado..."

    prices = [r[0] for r in reversed(rows)]
    current_price = prices[-1]
    trend = prices[-1] - prices[0]
    target_time = (datetime.now() + timedelta(hours=1)).strftime("%H:%M")
    
    margin = 4.0
    floor = current_price - margin + (trend * 0.5)
    ceiling = current_price + margin + (trend * 0.5)
    
    estado = "🚀 ALCISTA" if trend > 0.3 else ("⚠️ BAJISTA" if trend < -0.3 else "↔️ LATERAL")

    return (
        f"🔮 *PROYECCIÓN A 1 HORA ({target_time})*\n\n"
        f"📌 *Precio Actual:* {current_price:.2f} Bs\n"
        f"📊 *Estado:* {estado}\n"
        f"🟢 *Piso Estimado (+1H):* {floor:.2f} Bs\n"
        f"🔴 *Techo Estimado (+1H):* {ceiling:.2f} Bs\n\n"
        f"💡 _Basado en {len(prices)} lecturas guardadas en la nube._"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("¡Bot P2P Activo! Usa /prediccion para consultar la proyección a 1 hora.")

async def predict_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = calculate_prediction()
    await update.message.reply_text(msg, parse_mode="Markdown")

async def main():
    init_db()
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", predict_command))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    print("Servidor en ejecución...")

    while True:
        fetch_and_store_binance()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
