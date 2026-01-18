import os
import logging
import asyncio
from telegram import Bot
from telegram.error import TelegramError

logger = logging.getLogger("Notifications")

async def send_telegram_alert(message: str):
    """
    Sends a Telegram message to the configured chat ID.
    Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.warning("Telegram configuration missing. Skipping alert.")
        return

    try:
        bot = Bot(token=token)
        # sendMessage is a coroutine in v20+
        await bot.send_message(chat_id=chat_id, text=message)
        logger.info(f"Telegram alert sent: {message}")
    except TelegramError as e:
        logger.error(f"Failed to send Telegram alert: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending Telegram alert: {e}")

def send_alert_sync(message: str):
    """
    Synchronous wrapper for sending alerts.
    """
    try:
        # Check if there is a running loop in the current thread
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # If we are in an async context, schedule the task
        loop.create_task(send_telegram_alert(message))
    else:
        # If no loop is running (e.g., in a thread pool), run a new one
        asyncio.run(send_telegram_alert(message))
