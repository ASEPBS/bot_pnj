import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from openai import OpenAI

# Ambil token dari environment variables
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Inisialisasi OpenAI client
openai = OpenAI(api_key=OPENAI_API_KEY)

def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text('Halo! Aku bot penjaga Bicolink.')

def handle_message(update: Update, context: CallbackContext) -> None:
    user_message = update.message.text

    # Kirim ke GPT untuk diproses
    response = openai.Completion.create(
        engine="davinci",  # Atau engine GPT yang kamu mau
        prompt=user_message,
        max_tokens=50
    )

    bot_reply = response.choices[0].text.strip()
    update.message.reply_text(bot_reply)

def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    app.run_polling()

if __name__ == '__main__':
    main()
