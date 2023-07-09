import os
import re
from datetime import datetime

import psycopg2
import requests
import pymorphy2
import nltk

nltk.download("stopwords")
from nltk.corpus import stopwords

stop_words = stopwords.words("russian")
stop_words_en = stopwords.words("english")
stop_words.extend(stop_words_en)
stop_words = set(stop_words)

morph = pymorphy2.MorphAnalyzer()

stop_words_add = {
    "sputnik.",
    "ria.ru",
    "Rosiya Segodnya",
    "252 60 2022",
    "252 60",
    "1920 1080 true 1920 1440 true 1920 1920 true",
}
stop_words.update(stop_words_add)


def get_pg(dbname, user, password, host, port):
    conn = psycopg2.connect(
        dbname=dbname, user=user, host=host, password=password, port=port
    )
    cur = conn.cursor()
    return conn, cur


def text_preprocess(text: str) -> str:
    """
    Функция удаляет символы, которые не несут информации и контекста, включая
    также некоторые специфичные стопслова.
    Текст на выходе подается в BERT подобные модели
    """
    # удаляем спецсимволы
    text = re.sub("\\n", " ", text)
    text = re.sub("\xa0", " ", text)
    text = re.sub("&#34;", "", text)

    # удаляем URL
    text = re.sub(
        r"https?:\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])?",
        " ",
        text,
    )
    # удаляем адреса емейлов
    text = re.sub(r"[\w\.]+@([\w-]+\.)+[\w-]{2,4}", "", text)
    # удаляем адреса страниц
    text = re.sub(r"\b\w+\.[A-Za-z]{2,6}", "", text)
    # удаляем мета дату
    # /html/head/meta[@name='og:title']/@content
    text = re.sub(r"\/html\/head\/meta.+\/@[a-z]+", "", text)

    # удаляем даты в формате dd.mm.yyyy
    text = re.sub(r"\d{2}\.\d{2}\.\d{4}", "", text)

    # удаляем даты в формате yyyy.dd.mm
    text = re.sub(r"\d{4}\.\d{2}\.\d{2}", "", text)

    # удаляем время в формате HH:MM
    text = re.sub(r"\b\d{2}:\d{2}\b", "", text)

    # удаляем даты в формате YYYY-MM-DDTHH:MM+TZ (2022-06-09T16:12+0500)
    # или без TZ (2022-06-09T16:12)
    text = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(\+\d{4})*", "", text)

    # удаляем телефонные номера в формате 7 495 123-4556
    text = re.sub(r"\b7 \d{3} \d{3}-\d{4}\b", "", text)
    # удаляем телефонные номера в формате +74951234556
    sentence = re.sub(r"\+7\d{10}", "", text)
    sentence = sentence.split(".")[:-1]
    sentence = ".".join(sentence)
    sentence = " ".join(
        [word for word in sentence.split(" ") if word and word not in stop_words_add]
    )
    return sentence


def remove_stopwords(text: str) -> str:
    """
    Удаляет все стопслова.
    Текст на выходе подается в линейные модели без контекста
    """
    text = " ".join(
        [word for word in text.split(" ") if word and word not in stop_words]
    )
    return text


def get_language(text):
    """

    """
    pass


def ru_lemmatize(text: str) -> str:
    text = " ".join(morph.parse(word)[0].normal_form for word in text.split())
    return text


def en_lemmatize(text: str) -> str:

    pass


def send_message(tg_token, message, chat_ids):
    for chat_id in chat_ids:
        requests.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            data={"chat_id": chat_id, "text": message},
        )
