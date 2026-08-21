import os
import time
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import psycopg2
import requests
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from xgboost import XGBRegressor

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ==================== SERVIDOR SALUD DE RENDER ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()

# ==================== CONFIGURACIÓN ====================
TELEGRAM_TOKEN = "8579313357:AAH-ImigUgbAM59dOwy4sSi00j26u9EEjA8"
DB_URL = "postgresql://postgres.ozowlqqxsiqkfklzakjb:Abrilalessandro30@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

def get_db_connection():
    return psycopg2.connect(DB_URL, connect_timeout=10)

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                bank VARCHAR(50),
                buy_price REAL,
                sell_price REAL
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("Base de datos conectada e inicializada.")
    except Exception as e:
        print(f"Error en init_db: {e}")

# ==================== RECOLECTOR INDEPENDIENTE ====================
def background_collector():
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "payTypes": ["BBVA"],
        "rows": 5,
        "tradeType": "BUY"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Content-Type": "application/json"
    }
    
    while True:
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("data") and len(data["data"]) > 0:
                    buy_price = float(data["data"][0]["adv"]["price"])
                    
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO prices (bank, buy_price, sell_price) VALUES (%s, %s, %s)",
                        ("BBVA", buy_price, buy_price + 0.5)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Exito: Registrado {buy_price} Bs")
            else:
                print(f"Binance dio status code: {res.status_code}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error Recolector: {e}")
        
        time.sleep(30)

# ==================== MOTOR ML (XGBOOST) ====================
def calculate_ml_prediction():
    try:
        conn = get_db_connection()
        query = "SELECT timestamp, buy_price FROM prices ORDER BY id ASC;"
        df = pd.read_sql(query, conn)
        conn.close()

        total_records = len(df)
        if total_records < 30:
            return None, f"Se requieren al menos 30 lecturas para activar Machine Learning (Actuales: {total_records})."

        df['price'] = df['buy_price']
        df['lag_1'] = df['price'].shift(1)
        df['lag_2'] = df['price'].shift(2)
        df['lag_5'] = df['price'].shift(5)
        df['ma_5'] = df['price'].rolling(window=5).mean()
        df['ma_15'] = df['price'].rolling(window=15).mean()
        df['volatility'] = df['price'].rolling(window=5).std()
        df['target'] = df['price'].shift(-60)

        df_clean = df.dropna().copy()

        if len(df_clean) < 10:
            return None, f"Procesando matriz... ({len(df_clean)} muestras de entrenamiento, {total_records} totales)."

        features = ['price', 'lag_1', 'lag_2', 'lag_5', 'ma_5', 'ma_15', 'volatility']
        X = df_clean[features]
        y = df_clean['target']

        model = XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=4, random_state=42)
        model.fit(X, y)

        latest_row = df.iloc[-1]
        last_features = pd.DataFrame([{
            'price': latest_row['price'],
            'lag_1': df['price'].iloc[-2] if len(df) > 1 else latest_row['price'],
            'lag_2': df['price'].iloc[-3] if len(df) > 2 else latest_row['price'],
            'lag_5': df['price'].iloc[-6] if len(df) > 5 else latest_row['price'],
            'ma_5': df['price'].tail(5).mean(),
            'ma_15': df['price'].tail(15).mean(),
            'volatility': df['price'].tail(5).std()
        }])

        predicted_price = float(model.predict(last_features)[0])
        current_price = float(latest_row['price'])

        return {
            'current_price': current_price,
            'predicted_price': predicted_price,
            'samples': len(df_clean),
            'total_records': total_records
        }, None

    except Exception as e:
        return None, f"Error en el motor ML: {e}"

# ==================== COMANDOS TELEGRAM ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚡ *Javis Robo-Engine Activo*\nUsa /prediccion para calcular tendencias.", parse_mode="Markdown")

async def predict_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data, err = calculate_ml_prediction()
    if err:
        await update.message.reply_text(f"⚙️ *SISTEMA ML*\n\n{err}", parse_mode="Markdown")
        return

    curr = data['current_price']
    pred = data['predicted_price']
    diff = pred - curr
    target_time = (datetime.now() + timedelta(hours=1)).strftime("%H:%M")

    if diff > 0.3:
        estado = "🚀 ALCISTA (Fuerte)"
    elif diff < -0.3:
        estado = "⚠️ BAJISTA (Fuerte)"
    else:
        estado = "↔️ LATERAL / ESTABLE"

    margin = abs(diff) * 0.5 + 1.5
    floor = pred - margin
    ceiling = pred + margin

    report = (
        f"🤖 *PREDICCIÓN MACHINE LEARNING (XGBoost)*\n"
        f"⏱️ *Proyección para:* {target_time} (+1 Hora)\n\n"
        f"📌 *Precio Actual:* {curr:.2f} Bs\n"
        f"🎯 *Predicción ML:* {pred:.2f} Bs\n"
        f"📊 *Tendencia Estimada:* {estado}\n\n"
        f"🟢 *Piso Calculado:* {floor:.2f} Bs\n"
        f"🔴 *Techo Calculado:* {ceiling:.2f} Bs\n\n"
        f"🧠 _Modelado con {data['samples']} variables y {data['total_records']} lecturas._"
    )
    await update.message.reply_text(report, parse_mode="Markdown")

async def seed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        base_price = 36.50
        for i in range(35):
            fake_price = base_price + (i * 0.02) + (np.random.randn() * 0.05)
            cursor.execute(
                "INSERT INTO prices (bank, buy_price, sell_price) VALUES (%s, %s, %s)",
                ("BBVA", fake_price, fake_price + 0.5)
            )
        conn.commit()
        cursor.close()
        conn.close()
        await update.message.reply_text("✅ *35 lecturas de prueba insertadas.* Ya puedes ejecutar /prediccion", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error generando datos: {e}")

# ==================== INICIALIZACIÓN ====================
async def run_bot():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", predict_command))
    app.add_handler(CommandHandler("seed", seed_command))

    print("Iniciando Polling de Telegram...")
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()

if __name__ == "__main__":
    t_health = threading.Thread(target=run_health_server, daemon=True)
    t_health.start()

    init_db()

    t_collector = threading.Thread(target=background_collector, daemon=True)
    t_collector.start()

    try:
        asyncio.run(run_bot())
    except (KeyboardInterrupt, SystemExit):
        pass
