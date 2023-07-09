import json
import os
import requests
from logger import create_logger
from datetime import datetime


# use tg tg_bot for alarm signals if container fails
tg_token = os.getenv("TG_TOKEN")
# chat id of trade news tg_bot monitoring
target_chanel_ids = ["805670446"]

news_pg_login = os.getenv("POSTGRES_USER")
news_pg_password = os.getenv("POSTGRES_PASSWORD")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
news_pg_db = os.getenv("POSTGRES_DB")

NER_SERVER = os.getenv("NER_SERVER")
RE_SERVER = os.getenv("RE_SERVER")
WORD_PRODUCT_CLF_SERVER = os.getenv("WORD_PROD_CLF_SERVER")
TEXT_SPEC_PRODUCT_CLF_SERVER = os.getenv("TEXT_SPEC_PROD_CLF_SERVER")
CLF_NEWS_SERVER = os.getenv("CLF_NEWS_SERVER")
LANGUAGE_CLF_SERVER = os.getenv("LANG_SERVER")
MODEL_NAMES_SERVER = os.getenv("MODELS_NAMES")

if MODEL_NAMES_SERVER:
    try:
        model_names = requests.get(MODEL_NAMES_SERVER).json()
    except:
        raise Exception("Check server address of MODEL_NAMES_SERVER")

if not os.path.exists("/trade_news_auto_labelling"):
    os.mkdir("/trade_news_auto_labelling")
if os.path.exists("/trade_news_auto_labelling"):
    brat_folder = "/trade_news_auto_labelling"
elif os.path.exists("/home/lutova-da/trade_news/data/brat/data/trade_news_auto_labelling/"):
    brat_folder = "/home/lutova-da/trade_news/data/brat/data/trade_news_auto_labelling/"

if not os.path.exists("../logs"):
    os.mkdir("../logs")
log = create_logger(f"start_from_{datetime.now().date().strftime('%Y-%m-%d')}.txt")
