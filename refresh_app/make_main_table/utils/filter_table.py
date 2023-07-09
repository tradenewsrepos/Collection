import pymorphy2
import re

from .filter_lists import (
    norm_war_rus,
    norm_stock_rus,
    norm_war_en,
    norm_stock_en,
    norm_en
)

morph = pymorphy2.MorphAnalyzer()


def is_cirillic(text):
    """
    Функция определяет язык текста.
    Возвращает True для русского языка.
    """
    return len(re.findall('[А-Яа-яёЁ]+', text)) > 0


def text_normalise(text):
    """
    Функция нормализует текст
    """
    text = re.sub(r'[^\w\s]', '', text)
    text = " ".join(morph.parse(word)[0].normal_form for word in text.split())
    return text


def count_entries(text, list_name):
    """
    Функция подсчета вхождение слов из списка в указанный текст
    """
    counter = 0
    for word in list_name:
        if word in text:
            counter += 1
    return counter


def news_filter(text):
    """
    Функция фильтрует текст по словам из списка.
    Для русского текста - нормализация, для английского - перевод в нижний регистр.
    При 1 вхождении стопслов в текст возвращается False
    """
    if is_cirillic(text):
        text = text_normalise(text)
        for stoplist in [norm_war_rus, norm_stock_rus]:
            counter = count_entries(text, stoplist)
            if counter >= 1:
                return False
    else:
        text = re.sub(r'[^\w\s]', '', text)
        text = text.lower()
        for stoplist in [norm_war_en, norm_stock_en, norm_en]:
            counter = count_entries(text, stoplist)
            if counter >= 1:
                return False
    return True
