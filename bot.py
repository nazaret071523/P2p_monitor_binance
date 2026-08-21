import os
import time
import asyncio
import psycopg2
import requests
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from xgboost import XGBRegressor

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ==================== CONFIGURACIÓN ====================
TELEGRAM_TOKEN = "8579313357:AAGfJ4NfawMpcA1f1gRGTUAZCvEfl0ZbLZM"
DB_URL = "postgresql://postgres.ozowlqqxsiqkfklzakjb:[AbrilAlessandro30$]@aws-0-us-west-2.pooler.supabase.com:5432/postgres"  # Pega tu URL de Supabase

def get_db_connection():
    return psycopg2.connect(DB_URL)

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
        print("Base de datos PostgreSQL inicializada.")
    except Exception as e:
        print(f"Error inicializando BD: {e}")

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
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO prices (bank, buy_price, sell_price) VALUES (%s, %s, %s)",
                ("BBVA", buy_price, buy_price + 10.0)
            )
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Registrado en PostgreSQL: {buy_price} Bs")
    except Exception as e:
        print(f"Error al obtener/guardar precio de Binance: {e}")

# ==================== MOTOR DE MACHINE LEARNING ====================
def train_and_predict_ml():
    try:
        conn = get_db_connection()
        query = "SELECT timestamp, buy_price FROM prices ORDER BY id ASC;"
        df = pd.read_sql(query, conn)
        conn.close()

        if len(df) < 30:
            return None, f"Se requieren al menos 30 lecturas para activar Machine Learning (Actuales: {len(df)})."

        # Creación de variables técnicas (Feature Engineering)
        df['price'] = df['buy_price']
        df['lag_1'] = df['price'].shift(1)
        df['lag_2'] = df['price'].shift(2)
        df['lag_5'] = df['price'].shift(5)
        df['ma_5'] = df['price'].rolling(window=5).mean()
        df['ma_15'] = df['price'].rolling(window=15).mean()
        df['volatility'] = df['price'].rolling(window=5).std()
        df['target'] = df['price'].shift(-60)  # Objetivo: Precio proyectado a 60 lecturas (~1 Hora)

        df_clean = df.dropna().copy()

        if len(df_clean) < 15:
            return None, f"Acumulando más historial estructurado para el entrenamiento ML ({len(df_clean)} muestras válidas)."

        features = ['price', 'lag_1', 'lag_2', 'lag_5', 'ma_5', 'ma_15', 'volatility']
        X = df_clean[features]
        y = df_clean['target']

        # Entrenar modelo XGBoost
        model = XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=4, random_state=42)
        model.fit(X, y)

        # Preparar la última lectura para predecir el futuro
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
            'total_records': len(df)
        }, None

    except Exception as e:
        return None, f"Error en el motor ML: {e}"

def generate_prediction_report():
    data, err = train_and_predict_ml()
    if err:
        return f"⚙️ *SISTEMA DE ENTRENAMIENTO ML*\n\n{err}"

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

    # Margen dinamico según desviación esperada
    margin = abs(diff) * 0.5 + 1.5
    floor = pred - margin
    ceiling = pred + margin

    return (
        f"🤖 *PREDICCIÓN MACHINE LEARNING (XGBoost)*\n"
        f"⏱️ *Proyección para:* {target_time} (+1 Hora)\n\n"
        f"📌 *Precio Actual:* {curr:.2f} Bs\n"
        f"🎯 *Predicción ML:* {pred:.2f} Bs\n"
        f"📊 *Tendencia Estimada:* {estado}\n\n"
        f"🟢 *Piso Calculado:* {floor:.2f} Bs\n"
        f"🔴 *Techo Calculado:* {ceiling:.2f} Bs\n\n"
        f"🧠 _Modelo entrenado con {data['samples']} patrones e historial completo de {data['total_records']} lecturas._"
    )

# ==================== COMANDOS TELEGRAM ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("¡Bot P2P Inteligente (XGBoost + PostgreSQL) Activo! Usa /prediccion para consultar.")

async def predict_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = generate_prediction_report()
    await update.message.reply_text(msg, parse_mode="Markdown")

async def main():
    init_db()
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("prediccion", predict_command))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    print("Servidor con Machine Learning listo y escuchando...")

    while True:
        fetch_and_store_binance()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
