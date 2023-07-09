import requests
import os
from datetime import datetime

# tg_token = os.getenv("TG_TOKEN")
# Данные изменены на мои 13.03.23
tg_token = "5802313190:AAE5APsUh6Bs6m3s8OAKTxuEEdYgAr0oAkA"
target_chanel_ids = ["352318527"]


def send_message(message, chat_ids=["352318527"]):
    for chat_id in chat_ids:
        requests.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            data={"chat_id": chat_id, "text": message},
        )


today = datetime.today().strftime("%Y-%m-%d")
done_msg = "ok"
send_message(done_msg, target_chanel_ids)
