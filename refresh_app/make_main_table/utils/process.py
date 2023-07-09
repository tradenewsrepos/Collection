import re
import json
from collections import defaultdict
from typing import List, Tuple, Dict

import pymorphy2
import razdel
from tqdm import tqdm
from .trade_utils import loc_dict
from .trade_utils import (
    names_upper_case,
    stoplist,
    countries_and_regions,
    query_regions_dict_reversed,
    query_regions_dict as query_regions,
    en_ru_locs
)
from .filter_table import news_filter

morph = pymorphy2.MorphAnalyzer()


def translate_locs(list_locs, en_dict_locs=en_ru_locs):
    cyrilic = re.findall('[А-Яа-яёЁ]+', list_locs[0])
    if not cyrilic:
        ru_locs = []
        for loc in list_locs:
            if loc in en_dict_locs.keys():
                ru_locs.append(en_dict_locs[loc])
        return ru_locs, list_locs
    else:
        return list_locs, list_locs,


def get_known_locations(locations: List) -> List:
    """
    Функция проверяет есть ли страны локация у нас в константах, и если есть записывает их в список.
    Это позволит убрать названия локация (сел, городов), о которых мы не знаем.
    В unknown записываются неизвестные локации, по которым возможно будет расширить данные.
    """
    known_locations = set()
    unknown = set()
    for loc in locations:
        loc = loc.replace("Разные ", "")
        if loc in countries_and_regions:
            known_locations.add(loc)
        else:
            unknown.add(loc)
    known_locations = sorted(known_locations)
    return known_locations, unknown


def process_queryset_classes(q):
    classes = [cl for cl in sorted(set(q.classes.split("; "))) if cl]
    return classes


def process_queryset_dates(q):
    dates = [d[:10] for d in q.dates]
    dates = sorted(set(dates))
    return dates[-1]


def get_article_abstract(
        article_body: str,
        search_countries: List[str],
        region_locations: List[str]=None,
):
    """
    article_body - это title + "\n\n" + text
    search_countries - список стран, каждая страна не обязательно из одного слова (['Турецкая Республика', 'Турция'])
                    + могут быть прилагательные (['Сербия', 'Словакия', 'Турция', 'словацкие'])

    """
    stoplist_middle = [
        "Читайте нас на:",
    ]
    stoplist_end = [
        "Читайте также",
        "Читайте нас в Telegram",
        "Почему так произошло, читайте здесь",
        "Читайте полный текст",
        "Читайте еще",
        "Полный текст статьи читайте",
        "Полный текст интервью",
        "Читайте ранее",
        "Читайте подробнее",
        "Подробнее по этой теме читайте",
        "Подробнее читайте в",
        "Все новости Белоруссии читайте на",
        "Подписывайтесь на видео-новости",
    ]
    # подготовка строк локаций
    search_countries_parts = []
    for sc in search_countries:
        if " " in sc:
            for sc_part in sc.split():
                search_countries_parts.append(sc_part)
        else:
            search_countries_parts.append(sc)
    search_countries = [sc.lower()[:-1] for sc in search_countries_parts]

    # очистка
    for pattern in stoplist_middle:
        article_body = re.sub(pattern, "", article_body, flags=re.I)
    for pattern in stoplist_end:
        article_body = re.sub(f"{pattern}.+", "", article_body, flags=re.I)

    # добавить пробелы, если в изначальном тексте слипшиеся предложения
    article_body = re.sub(r"\.([а-яА-яёЁa-zA-Z])", r". \1", article_body)

    # разделение заголовка и текста новости
    title = article_body
    text = ""
    for split_chars in ["\\n\\n", "\n\n"]:
        if split_chars in article_body:
            splited_text = article_body.split(split_chars)
            if len(splited_text) <= 2:
                title, text = splited_text
            else:
                title, text = splited_text[:2]

    article_body = title
    sents = []

    # только предложения, содержащие указанные локации и регионы
    if search_countries:
        sents = razdel.sentenize(text)
        if region_locations:
            sents = [
                s
                for s in sents
                if any(a.lower() in s.text.lower() for a in region_locations)
                   or any(sc in s.text.lower() for sc in search_countries)
            ]
        else:
            sents = [
                s for s in sents if any(sc in s.text.lower() for sc in search_countries)
            ]

        sents = [
            s
            for s in sents
            if len(s.text) > 10
               and (s.text not in title)
               and (title not in s.text)  # без повторения заголовка
               and not any(
                [
                    term in s.text
                    for term in ["sputnik.", "ria.ru", "http", "Rosiya Segodnya"]
                ]
            )
            # посторонний текст в спутнике и риа
        ]
        # Leave only 5 sentences
        sents = sents[:5]

        abstract = " ".join([s.text for s in sents])  # абстракт
        if abstract:
            article_body = title + ". " + abstract  # заголовок + абстракт
    for s in sents:
        # len("\\n\\n") == 4
        s.start += len(title) + 4
        s.stop += len(title) + 4
    sents = [razdel.substring.Substring(0, len(title) + 1, title)] + sents

    return article_body, sents


def remove_location_duplicates(locations: List) -> List:
    to_del = []
    for l_i, l in enumerate(locations):
        if l_i == len(locations) - 1:
            break
        if l in locations[l_i + 1]:
            to_del.append(l_i)
    locations = [loc for l_i, loc in enumerate(locations) if l_i not in to_del]

    to_del = []
    for l_i, l in enumerate(locations):
        if l_i == 0:
            continue
        if l in locations[l_i - 1]:
            to_del.append(l_i)
    locations = [loc for l_i, loc in enumerate(locations) if l_i not in to_del]

    return locations


def normalize_locations(locations: List) -> List:
    """
    Названия стран приводятся к нормальному значению.
    Прилагательные типа "британская", приводятся к нормальному виду и далее заменяются значением из словаря.
    """
    # британско-шведский -> Великобритания, Швеция
    new_countries = []
    for l_i, l in enumerate(locations):
        if l in countries_and_regions:
            continue
        normal_form = morph.parse(l)[0].normal_form
        if normal_form in stoplist:
            locations[l_i] = ""
            continue

        new_form = loc_dict.get(normal_form.lower())
        if new_form:
            new_form = new_form.split(", ")
            new_countries.extend(new_form)
            locations[l_i] = ""

        if normal_form in names_upper_case:
            normal_form = normal_form.upper()
        else:
            normal_form = normal_form.capitalize()

        region = query_regions_dict_reversed.get(normal_form)
        if region:
            locations[l_i] = normal_form
    locations.extend(new_countries)
    locations = [loc for loc in locations if loc]
    locations = set(sorted(locations))
    return locations


def get_regions_from_countries(locations: List) -> Dict:
    """
    Из списка стран и регионов делаем словарь регион - страны.
    В словаре сохраняем только географические регионы.
    Если в списке locations уже есть регион, то оставляем его в виде ключа с пустым списком стран.
    """
    regions = defaultdict(set)
    for loc in locations:
        region = query_regions_dict_reversed.get(loc)
        loc = loc.replace("Разные ", "")
        if loc in query_regions and "Страны" not in loc:
            regions[loc].add(loc)
        if region:
            for reg in region:
                if "Страны" in reg:
                    regions[reg].add(loc)
    regions = {
        r: c for r, c in sorted(regions.items(), key=lambda x: len(x[1]), reverse=True)
    }

    return regions


def process_text_locations(location_str: str) -> Tuple[List, Dict]:
    """
    Функция преобразует список стран из базы в список стран из известных.
    Выполняются следующие преобразования:
    - удаляются дубликаты;
    - названия стран приводят к нормальным, прилагательные преобразуются в существительны;
    - список сокращается до списка стран и регионов, названия которых изначально заданы;
    - по известным странам составляем словарь вида {регион_1: [страна_1, страна_2], регион_2: [страна_3, страна_4]}
    - к названиям регионов добавляем "Разные", например "Страны Африки" -> "Разные Страны Африки"
    """
    locations = location_str.split(", ")
    locations = remove_location_duplicates(locations)
    locations, en_locations = translate_locs(locations)
    locations = normalize_locations(locations)
    locations, _ = get_known_locations(locations)
    regions = get_regions_from_countries(locations)
    # add "Разные " to region name
    locations = [("Разные " + loc) if "Страны" in loc else loc for loc in locations]

    locations = sorted(set(locations))
    return locations, en_locations, regions


# 06.06.2023
# Внесено изменение в возврат значений вместе с List[Dict], теперь возвращается и Set[int] 
def excluded_article_add(articles: set(), art_id):
    
    for article_id in iter(art_id):
        articles.add(int(article_id))


def process_data(data):
    to_del_ids = set()
    seen_news = set()
    data_processed = []
    excluded_article = set()
    for row in tqdm(data):
        row_dict = {}
        if not row.dates or not row.dates[0]:
            to_del_ids.add(row.id)
            excluded_article_add(excluded_article, row.article_ids)
            continue
        if not row.title:
            to_del_ids.add(row.id)
            excluded_article_add(excluded_article, row.article_ids)
            continue
        if not news_filter(row.title):
            to_del_ids.add(row.id)
            excluded_article_add(excluded_article, row.article_ids)
            continue

        if row.product:
            if str(row.locations) + str(row.product) + str(row.itc_codes) in seen_news:
                to_del_ids.add(row.id)
                excluded_article_add(excluded_article, row.article_ids)
                continue
            seen_news.add(str(row.locations) + str(row.product) + str(row.itc_codes))

        location_str = row.locations
        locations, en_locations, regions = process_text_locations(location_str)

        if locations:
            search_string = locations[0] + row.itc_codes + str(row.dates)
            if search_string in seen_news:
                to_del_ids.add(row.id)
                excluded_article_add(excluded_article, row.article_ids)
                continue
            seen_news.add(search_string)
        else:
            to_del_ids.add(row.id)
            excluded_article_add(excluded_article, row.article_ids)
            continue

        title = row.title
        cyrilic = re.findall('[А-Яа-яёЁ]+', title)
        if not cyrilic:
            title, sents = get_article_abstract(title, en_locations)
        else:
            title, sents = get_article_abstract(title, locations)
        locations_filter = [sc.lower()[:-1] for sc in locations]
        if not sents and not any(sc in title.lower() for sc in locations_filter):
            to_del_ids.add(row.id)
            excluded_article_add(excluded_article, row.article_ids)
            continue
        location_str = ", ".join(locations)

        row_dict["id"] = row.id
        row_dict["dates"] = process_queryset_dates(row)
        row_dict["classes"] = process_queryset_classes(row)
        row_dict["itc_codes"] = row.itc_codes
        row_dict["locations"] = location_str
        row_dict["title"] = title
        row_dict["url"] = row.url
        row_dict["article_ids"] = row.article_ids
        row_dict["product"] = row.product
        row_dict["status"] = row.status
        data_processed.append(row_dict)
    
    data_processed = [q for q in data_processed if q["id"] not in to_del_ids]
    
    return [data_processed, excluded_article]
