import time
import json
import os
from typing import List, Dict, AnyStr

import pymorphy2
import razdel
import requests
from config import (
    NER_SERVER,
    WORD_PRODUCT_CLF_SERVER,
    TEXT_SPEC_PRODUCT_CLF_SERVER,
)

morph = pymorphy2.MorphAnalyzer()

with open("./data/relations_dict.json") as f:
    relations_dict = json.load(f)
    # convert lists to sets
    # 'AGE_DIED_AT': {'keys': ['PERSON'], 'values': ['AGE']}, -->
    # 'AGE_DIED_AT': {'keys': {'PERSON'}, 'values': {'AGE'}},
    for k, v in relations_dict.items():
        for k2, v2 in v.items():
            v[k2] = set(v2)

with open("./data/allowed_matches.json") as f:
    subj_relations_dict = json.load(f)
    subj_relations_dict = {k: set(v) for k, v in subj_relations_dict.items()}


def lemmatize(text):
    """
    http://10.8.0.4:3050/gordeev-di/coronavirus_texts_monitoring/
    src/branch/master/newsfeedner/utils.py#L246

    by Alexander Shatilov
    """
    lemmas = []
    for word in text.split():
        if word.isupper():
            lemmas.append(word)
            continue
        p = morph.parse(word)[0]
        word_nominative = p.inflect({"nomn"})

        if word_nominative:
            word_norm = word_nominative.word
        else:
            word_norm = p.normal_form

        # convert to initial letter case
        if word.islower():
            result = word_norm
        elif word.isupper():
            result = word_norm.upper()
        else:
            result = word_norm.capitalize()
        lemmas.append(result)
    lemmas_str = " ".join(lemmas)
    return lemmas_str


def match_relations_with_ner(relations, named_entities):
    for r in relations:
        subjs = [n for n in named_entities if n["start"] == r["subj_charstart"]]
        objs = [n for n in named_entities if n["start"] == r["obj_charstart"]]
        if subjs and objs:
            r["subj"] = subjs[0]
            r["obj"] = objs[0]
    return relations


def ner_whole_text_inference(text):
    named_entities = []
    start = 0
    while True:
        cut_text = text[start:]
        if not cut_text:
            break
        try:
            # start_time = time.time()
            response = requests.post(NER_SERVER, json={"text": cut_text})
            # print("NERO_CLF_SERVER --- %s seconds ---" % (time.time() - start_time))
            paragraph_named_entities = response.json()
        except Exception as ex:
            print(ex, text)
            break
        if not paragraph_named_entities:
            break
        for pne in paragraph_named_entities:
            # dicts are mutable
            pne["start"] += start
            pne["end"] += start
        ner_end = paragraph_named_entities[-1]["end"] + 1
        named_entities += paragraph_named_entities
        start += ner_end

    for n in named_entities:
        # the difference in the API by Sergey and Adis
        if "entity_group" in n:
            n["entity"] = n["entity_group"]
    return named_entities


def postprocess_entities(named_entities, is_selected):
    """
    Функция обрабатывает сущности. Каждая сущность с тегом PRODUCT классифицируется
    по классам СМТК кодов.
    Для выбранных источников классифицируем продукты и если продукт не определен то приписываем класс 93
    """
    named_entities = [n for n in named_entities if len(n["word"]) < 50]
    named_entities = [n for n in named_entities if n["score"] > 0.7]
    for n in named_entities:
        n["lemma"] = lemmatize(n["word"])

    # classify products
    for i, n in enumerate(named_entities):

        if n["entity"] == "PRODUCT":
            # start_time = time.time()
            smtk_ru_response = requests.post(WORD_PRODUCT_CLF_SERVER, json={"text": n["word"]}).json()
            # print("WORD_CLF_SERVER --- %s seconds ---" % (time.time() - start_time),  end='')
            smtk_class = smtk_ru_response["class"]
            # print(smtk_class)
            if smtk_class == "100 - not product" and is_selected:
                smtk_class = (
                    "93 - Специальные операции и товары, не классифицированные по типу"
                )
            n["smtk_class"] = smtk_class
    for n_i, n in enumerate(named_entities):
        n["order_id"] = n_i + 1
    return named_entities


def create_brat_labels(named_entities, relations):
    brat_labels: List[str] = []
    for ner_i, ner in enumerate(named_entities):
        start = ner["start"]
        end = ner["end"]
        label_index = len(brat_labels) + 1
        label_str = f"T{label_index}	" f"{ner['entity']} {start} {end}	{ner['word']}"
        brat_labels.append(label_str)
    relation_id = 1
    for r in relations:
        if "subj" in r and "obj" in r:
            label_str = (
                f"R{relation_id}	"
                f"{r['relation']} "
                f"Arg1:T{r['subj']['order_id']} "
                f"Arg2:T{r['obj']['order_id']}"
            )
            brat_labels.append(label_str)
            relation_id += 1
    return brat_labels


def map_bpe_ners_to_razdel(named_entities, text):
    razdel_tokens = razdel.tokenize(text)
    last_entity = 0
    wrong_entities = set()
    for i, razdel_token in enumerate(razdel_tokens):
        prev_entity = None
        while True:
            if last_entity >= len(named_entities):
                break
            n = named_entities[last_entity]

            if n["start"] > razdel_token.stop:
                break
            elif n["start"] >= razdel_token.start and n["start"] <= razdel_token.stop:
                # we see a nested entity for the first time
                # ["Ил"]
                if prev_entity is None:
                    prev_entity = named_entities[last_entity - 1]
                # we add it to the prev_entity
                # last_entity = 10 ["Ил-"]
                # last_entity = 11 ["Ил-76"]
                if (
                    prev_entity["start"] >= razdel_token.start
                    and prev_entity["start"] <= razdel_token.stop
                    and prev_entity["entity"] == n["entity"]
                ):
                    prev_entity["end"] = n["end"]
                    prev_entity["word"] = text[prev_entity["start"]: n["end"]]
                    wrong_entities.add(last_entity)
                else:
                    prev_entity = None
                last_entity += 1
            elif n["stop"] < razdel_token.start:
                last_entity += 1
                prev_entity = None
    # clear unmapped entities
    named_entities = [
        n for i, n in enumerate(named_entities) if i not in wrong_entities
    ]
    return named_entities


def filter_relations(relations):
    relations = [r for r in relations if abs(r["subj_start"] - r["obj_start"]) < 20]
    relations = [
        r for r in relations if abs(r["subj_charstart"] - r["obj_charstart"]) < 200
    ]
    wrong_relations = []
    for r_i, r in enumerate(relations):
        if r["relation"] in relations_dict:
            # wrong subj or obj for this relation type
            if (
                r["subj_type"] not in relations_dict[r["relation"]]["keys"]
                or r["obj_type"] not in relations_dict[r["relation"]]["values"]
            ):
                wrong_relations.append(r_i)
                continue
        if r["subj_type"] in subj_relations_dict:
            if r["obj_type"] not in subj_relations_dict[r["subj_type"]]:
                wrong_relations.append(r_i)
                continue
    relations = [r for r_i, r in enumerate(relations) if r_i not in wrong_relations]
    return relations


def write_entites_relations_to_file(
    brat_folder, today_date, named_entities, relations, text, article_id
):
    brat_labels = create_brat_labels(named_entities, relations)
    path = brat_folder + "/" + today_date + "/"
    if not os.path.exists(path):
        os.mkdir(brat_folder + "/" + today_date + "/")
    if brat_labels:
        file_path = f"{brat_folder}/{today_date}/{article_id}"
        with open(file_path + ".txt", "w") as f:
            f.write(text)
        with open(file_path + ".ann", "w") as f:
            f.write("\n".join(brat_labels))
    # print(
    #     "http://10.8.0.5:8003/index.xhtml#/trade_news_auto_labelling/"
    #     f"{today_date}/{article_id}"
    # )


def special_prods_clf(text: AnyStr) -> Dict:
    """
    Функция возвращает класс "93 - Специальные операции и товары, не классифицированные по типу",
    если новость содержит упоминания таких товаров или "no_special_product", если в новости
    нет подобных товаров
    """
    # start_time = time.time()
    response = requests.post(TEXT_SPEC_PRODUCT_CLF_SERVER, json={"text": text}) # type: ignore
    # print("TEXT_CLF_SERVER --- %s seconds ---" % (time.time() - start_time))
    pred = response.json()["class"]
    return pred


def contains_special_products(entities: List[Dict]) -> Dict:
    """
    Функция проверяет последовательность сущностей на наличие товаров
    определенных классов класса
    "93 - Специальные операции и товары, не классифицированные по типу",
    """
    spec_prods = [e for e in entities if "93" in e.get("smtk_class", "")]
    if spec_prods:
        return True
    return False
