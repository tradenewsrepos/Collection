import os
from datetime import datetime, timedelta

import requests


LOGS_PATH = './logs/refresh_log.txt'
TG_TOKEN = os.getenv("TG_TOKEN")


def write_logs(status, error=None, duration=0):
    if status == "success":
        if not os.path.isdir('logs'):
            os.mkdir('logs')    
        with open(LOGS_PATH, "a") as f:
            f.write("success" + " " + str(datetime.today()) + " took time: " + str(timedelta(seconds=duration)) + "\n")
    if status == "error":
        with open(LOGS_PATH, "a") as f:
            f.write("error" + " " + str(datetime.today()) + f" {error}" + "\n")
    print('Вывод в log завершен')        


def send_message(message, tg_token=TG_TOKEN, chat_ids=["352318527"]):
    for chat_id in chat_ids:
        requests.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            data={"chat_id": chat_id, "text": message},
        )
        
write_logs("success", error=None, duration=100)        
