import os
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

TOKEN_PAGOS = os.getenv("TELEGRAM_TOKEN_PAGOS")

async def start_pagos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💳 Módulo de pagos y pasarela activa.")

def main():
    if not TOKEN_PAGOS:
        print("⚠️ Token de pagos no configurado.")
        return
    app = ApplicationBuilder().token(TOKEN_PAGOS).build()
    app.add_handler(CommandHandler("start", start_pagos))
    print("🚀 Bot de pagos corriendo en segundo plano...")
    app.run_polling()

if __name__ == "__main__":
    main()
