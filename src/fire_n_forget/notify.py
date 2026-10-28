import os

import httpx

CHAT_ID: str = os.getenv("CHAT_ID", "")
TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"


def notify(message: str):
    with httpx.Client() as client:
        response = client.post(
            URL,
            json={"chat_id": CHAT_ID, "text": message},
        )
        response.raise_for_status()


async def anotify(message: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            URL,
            json={"chat_id": CHAT_ID, "text": message},
        )
        response.raise_for_status()
