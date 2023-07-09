import json
import os
import time
from datetime import datetime, timedelta
from typing import Tuple, Set

import fire
import pymorphy2
import requests
import psycopg2
from ner_processing import (
    ner_whole_text_inference,
    match_relations_with_ner,
    postprocess_entities,
    map_bpe_ners_to_razdel,
    filter_relations,
    write_entites_relations_to_file,
    special_prods_clf,
    contains_special_products,
)
from utils import get_pg, text_preprocess, send_message
from config import (
    tg_token,
    target_chanel_ids,
    news_pg_login,
    news_pg_password,
    postgres_host,
    postgres_port,
    news_pg_db,
    RE_SERVER,
    CLF_NEWS_SERVER,
    LANGUAGE_CLF_SERVER,
    model_names,
    brat_folder,
    log,
)

morph = pymorphy2.MorphAnalyzer()
# TODO заменить все запросы на алхимию,
# для обработки пропущенных новостей задаем вручную дату начала обработки
hours = 4
days = 0
# not inside a container
if not os.path.exists("/trade_news_auto_labelling"):
    days = 5


def get_start_date() -> datetime.date:
    """
    Возвращает дату начала парсинга - следующие день за последней обработанной датой в
    таблице trade_news_events. Если таблица пустая берем значение из переменной окружения START_DATE
    """
    query_table = """
        SELECT MAX(to_date(dates::text, 'YYYY-MM-DD'))
        FROM trade_news_events tne """
    query_mat_view = """
        SELECT MAX(to_date(dates[1]::text, 'YYYY-MM-DD'))
        FROM trade_news_events tne """
    try:
        news_cur.execute("select * from trade_news_events")
        try:
            news_conn.rollback()
            news_cur.execute(query_table)
            date = news_cur.fetchall()[0][0]
        except psycopg2.errors.InvalidDatetimeFormat:
            news_conn.rollback()
            news_cur.execute(query_mat_view)
            date = news_cur.fetchall()[0][0]
    except psycopg2.errors.UndefinedTable: # type: ignore
        news_conn.rollback()
        start_date: str = os.getenv("START_DATE")

        print("start date: ", start_date)
        return datetime.strptime(start_date, "%Y-%m-%d").date()
    print("start date: ", date)
    return date


def write_status(article_id, status):
    # print("write_status", article_id, status)
    query = f"""
    INSERT INTO trade_news_processed
    (article_id, 
     status, 
     clf_version, 
     ner_version, 
     rel_ext_version, 
     prod_word_clf_version, 
     prod_text_clf_version)
        VALUES
        (
        {article_id}, 
        '{status}',
        '{model_names['clf_news']}', 
        '{model_names['ner']}', 
        '{model_names['relation_extraction']}',
        '{model_names['word_clf']}',
        '{model_names['text_clf']}'
        )
    """
    news_cur.execute(query)
    news_conn.commit()


def update_status(article_id, status):
    """
    Обновляет статус для новостей, у которых был статус clf_fail или server_error
    """
    query = f"""
    UPDATE trade_news_processed
    SET status = '{status}'
    WHERE  article_id = {article_id} 
    """
    news_cur.execute(query)
    news_conn.commit()


def write_entities_into_db(named_entities, article_id):
    """
    there is no need to return `named_entities`
    because it is mutable
    but it is easier for understanding (imho)
    news_cur is global
    product_class example - "04 - Зерновые и продукты из них"
    """
    ner_version = model_names.get("ner")[:10]
    article_unique_entities = set()
    article_unique_feed_entities = set()
    # get or save lemmas into the database
    for n in named_entities:
        smtk_class = "NULL"

        if n["entity"] == "PRODUCT":
            smtk_class = n.get("smtk_class")
            if smtk_class:
                smtk_class = f"'{smtk_class}'"
            else:
                smtk_class = "NULL"

        # query if exists
        n["lemma"] = n["lemma"].replace("'", "''")
        n["word"] = n["word"].replace("'", "''")
        query = f"""
        SELECT id from trade_news_entity
        where name = '{n["lemma"]}' and entity_class = '{n["entity"]}';
        """
        news_cur.execute(query)
        ner_id = news_cur.fetchone()
        if ner_id is None:
            query = f"""
            INSERT INTO trade_news_entity
            (name, entity_class, smtk_code)
                VALUES
                ('{n["lemma"]}', '{n["entity"]}', {smtk_class})
                RETURNING id;
            """
            news_cur.execute(query)

            ner_id = news_cur.fetchone()
        ner_id = ner_id[0]
        n["db_id"] = ner_id
        if (n["db_id"], n["start"]) not in article_unique_feed_entities:
            query = f"""
            INSERT INTO trade_news_feedentities
            (words, ent_id, start, "end", score, article_id, ner_version)
                VALUES
                ('{n["word"]}', {ner_id},
                {n["start"]}, {n["end"]}, {round(n["score"], 3)},
                {article_id}, '{ner_version}')
                RETURNING id;
            """
            news_cur.execute(query)
            ner_feed_id = news_cur.fetchone()[0]
            n["ner_feed_id"] = ner_feed_id
            article_unique_feed_entities.add((n["db_id"], n["start"]))

        if ner_id not in article_unique_entities:
            query = f"""
            INSERT INTO trade_news_article_article_entities
            (article_id, feedentities_id)
                VALUES
                ({article_id}, {ner_id})
            """
            news_cur.execute(query)
            article_unique_entities.add(ner_id)

    return named_entities


def news_filter(article_id, text, threshold):
    """
    От модели результат приходит в таком виде:
    {'label': 'COVID-19', 'score': 0.0},
    {'label': 'международные отношения', 'score': 0},
    {'label': 'Россия', 'score': 0.0},
    {'label': 'Социологические опросы', 'score': 0.0},
    {'label': 'аналитика', 'score': 0.0},
    {'label': 'военная тематика', 'score': 0.0},
    {'label': 'меры поддержки', 'score': 0.0},
    {'label': 'мнения', 'score': 0.0},
    {'label': 'политика', 'score': 0.0},
    {'label': 'не по теме', 'score': result[0]['score']},
    {'label': 'другие отношения', 'score': result[1]['score']},
    {'label': 'торговля', 'score': result[2]['score']},
    {'label': 'проекты', 'score': result[3]['score']},
    {'label': 'санкции', 'score': result[4]['score']},
    {'label': 'инвестиция', 'score': result[5]['score']},
    Функция записывает результаты классификации в базу и возвращает статус классификации.
    Статус определяется порогом в переменой threshold
    Под максимальную длину текст подрезается в сервисе с моделями
    """
    try:
        # start_time = time.time() 
        categories = requests.post(CLF_NEWS_SERVER, json={"text": text}).json() # type: ignore
        # print("NEWS_CLF_SERVER --- %s seconds ---" % (time.time() - start_time))
    except Exception as ex:
        return "clf_fail"
    scores = [str(round(c["score"], 4)) for c in categories]
    score_str = ", ".join(scores)
    query = f"""
    INSERT INTO trade_news_classification
    (article_id, covid, foreign_relations, russia, sociology, analytics,
     military, economic_support, opinion, politics, irrelevant, other_relations, 
      trade, projects, sanction, investment)        
        VALUES
        ({article_id}, {score_str})
    """
    
    status = "clf_bad"
    for c in categories:
        if c["label"] not in [
            "другие отношения",
            "инвестиция",
            "проекты",
            "торговля",
            "санкции",
        ]:
            continue
        if c["score"] > threshold:
            status = "clf_good"
            # изменено мной с целью оптимизации
            news_cur.execute(query)
            break
    return status


def get_selected_articles() -> Set:
    # Новости только от мипромторга, торгпредств и экспортцентра
    query_selected = """
               SELECT distinct na.id            
               FROM newsfeedner_article na
               JOIN newsfeedner_feed nf ON na.feed_id = nf.id
               WHERE nf.name::text='minpromtorg'::text 
               OR nf.name::text ilike '%torgpred%'::text 
               OR nf.name::text = 'exportcenter'::text
               """
    news_cur.execute(query_selected)
    selected_ids = {e[0] for e in news_cur.fetchall()}
    return selected_ids


def get_existing_data() -> Tuple[Set, Set, Set]:
    query = """
        select distinct article_id
        from trade_news_processed
        WHERE status not in ('clf_fail', 'server_error')
        """
    query_failed = """
        SELECT distinct article_id    
        FROM trade_news_processed tnp
        WHERE status in ('clf_fail', 'server_error')
        """
    news_cur.execute(query)
    processed = {p[0] for p in news_cur.fetchall()}

    news_cur.execute(query_failed)
    processed_failed = {p[0] for p in news_cur.fetchall()}

    return processed, processed_failed # type: ignore


def get_parse_dates(days, hours):
    """Функция возвращает даты периода, в котором рассматриваем новости и значение шага,
    через который обрабатываем новости, в зависимости от разности между
    сегодняшней датой и стартовой датой обработки
    Период равен 1-му дню.
    При разности даты периода более 7 дней от текущей даты - шаг 5.
    При разности даты периода менее 7 дней от текущей даты - шаг 1.
    """
    global start_date_prev
    now_temp = datetime.now().date()
    days_diff = (now_temp - start_date_prev).days
    news_step = 1
    if days_diff > 7:
        news_step = int(os.getenv("LATE_STEP")) # type: ignore
    if start_date_prev < now_temp:
        date_begin = start_date_prev
        date_end = date_begin + timedelta(days=1)
        start_date_prev = date_end
    else:
        now = datetime.now()
        date_begin = now - timedelta(days=days, hours=hours)
        date_end = date_begin + timedelta(days=1)
    return date_begin, date_end, news_step


def get_language_and_write_to_db(article_id: int, text: str) -> str:
    # start_time = time.time()
    pred = requests.post(LANGUAGE_CLF_SERVER, json={"text": text}).json() # type: ignore
    # print("LANG_CLF_SERVER --- %s seconds ---" % (time.time() - start_time))
    pred_lang = pred.get("language", None)
    query_insert = f"""
    INSERT INTO trade_news_language (article_id, "language")
    VALUES ({article_id}, '{pred_lang}')
    """
    try:
        news_cur.execute(query_insert)
    except psycopg2.errors.UniqueViolation: # type: ignore
        news_conn.rollback()
    return pred_lang


def parse_news(days, hours):
    start_time = time.time()
    processed, processed_failed = get_existing_data() # type: ignore
    selected_ids = get_selected_articles()
    # global
    processed_news_ids.update(processed)
    print("processed_news_ids num: ", len(processed_news_ids))

    del processed

    date_begin, date_end, news_step = get_parse_dates(days, hours)

    str_date_begin = date_begin.strftime("%Y-%m-%d %H:00")
    str_date_end = date_end.strftime("%Y-%m-%d %H:00")
    today_date = date_begin.strftime("%Y-%m-%d")
    query = f"""
        SELECT na.*
        FROM newsfeedner_article na  
        WHERE na.published_parsed >= '{str_date_begin}'::TIMESTAMP 
        AND na.published_parsed < '{str_date_end}'::TIMESTAMP   
        AND NOT na.is_entities_parsed and na.sentiment > 0.45   
    """

    print(query)
    news_cur.execute(query)
    news = news_cur.fetchall()
    # filter already processed news by article_id
    news = [n for n in news if n[0] not in processed_news_ids]

    # filter already processed news by empty body
    news = [n for n in news if n[-2]]
    print("news found:", len(news))
    # filter news to reduce number of old news to process
    if news_step > 1:
        news = [news[i] for i in range(0, len(news), news_step)]

    news_ids = [n[0] for n in news]
    print(f"news to process considering step {news_step}:", len(news))
    print(
        "news found failed that can be updated:", len(processed_failed & set(news_ids))
    )
    print("news processed:", len(processed_news_ids & set(news_ids)))

    for article_i, article in enumerate(news):
        now = datetime.now() # type: ignore
        print(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} Новость за: {str_date_begin} : News {article_i} of {len(news)} article {article[0]} processing ... "
        )
        article_id = article[0]
        processed_news_ids.add(article_id)

        is_selected = False
        if article_id in selected_ids:
            is_selected = True

        title = article[3]
        article_body = article[-2]
        if not article_body:
            if article_id not in processed_failed:
                write_status(article_id, "empty_body")
            else:
                update_status(article_id, "empty_body")
            continue
        text_language = get_language_and_write_to_db(article_id, article_body)
        if text_language not in ["en", "ru"]:
            if article_id not in processed_failed:
                write_status(article_id, "not_ru_or_en_language")
            else:
                update_status(article_id, "not_ru_or_en_language")
            continue

        article_body = text_preprocess(article_body)
        text = title + "\n" + article_body
        
        # добавлено 27.06.2023
        # Проверка на наличие неправильной кодировки
        if text.find('Ð')>0:
            continue
        # конец добавлено
        
        if is_selected:
            # Lower probability for reliable sources
            status = news_filter(article_id, text, threshold=0.2)
        else:
            status = news_filter(article_id, text, threshold=0.5)

        if status not in ("good", "clf_good"):
            # if before was process was failed and now is good - update values
            if processed_failed and article_id not in processed_failed:
                write_status(article_id, status)
            elif article_id in processed_failed:
                update_status(article_id, status)
            continue
        try:
            # start_time = time.time()
            server_output = requests.post(RE_SERVER, json={"text": text}).json()
            # print("RERE_RER_SERVER --- %s seconds --- " % (time.time() - start_time))
        except json.JSONDecodeError:
            # if before process was failed and now is good update values
            if processed_failed and article_id not in processed_failed:
                write_status(article_id, "server_error")
                processed_failed.add(article_id)
            continue

        relations, named_entities = (
            server_output["relations"],
            server_output["entities"],
        )

        if not named_entities:
            named_entities = ner_whole_text_inference(text)

        named_entities = map_bpe_ners_to_razdel(named_entities, text)
        named_entities = postprocess_entities(named_entities, is_selected)
        if not contains_special_products(named_entities):
            special_prod = special_prods_clf(text)
            if special_prod != "no_special_product":
                new_entity = {
                    "entity_group": "PRODUCT",
                    "score": 1,
                    "word": "CLASSIFIED_BY_TEXT",
                    "start": -1,
                    "end": -1,
                    "entity": "PRODUCT",
                    "lemma": "CLASSIFIED_BY_TEXT",
                    "smtk_class": special_prod,
                    "order_id": -1,
                }
                named_entities.append(new_entity)
        named_entities = write_entities_into_db(named_entities, article_id)
        named_entities = [n for n in named_entities if n.get("ner_feed_id")]

        if not named_entities:
            write_status(article_id, "no_filtered_ner")
            continue
        # print("good")
        status = "good"
        # if before process was failed and now is good update values
        if article_id not in processed_failed:
            write_status(article_id, status)
        else:
            update_status(article_id, status)
        relations = filter_relations(relations)
        relations = match_relations_with_ner(relations, named_entities)
        # filter [relation->ner] matching errors
        relations = [
            r for r in relations if type(r["subj"]) == dict and type(r["obj"]) == dict
        ]
        for r in relations:
            if "subj" in r and "obj" in r:
                query = f"""
                INSERT INTO trade_news_relations
                (entity_1, entity_2, relation_type)
                    VALUES
                    ({r["subj"]["ner_feed_id"]},
                    {r["obj"]["ner_feed_id"]},
                    '{r["relation"]}')
                """
                news_cur.execute(query)
        news_conn.commit()
        #Добавлено 27.06.2023
        #Отметка об обработке процессом
        query = f""" 
                UPDATE newsfeedner_article 
                SET is_entities_parsed = NOT is_entities_parsed
                WHERE id={article[0]}
        """
        news_cur.execute(query)
        news_conn.commit()
        #Конец добавлено
        write_entites_relations_to_file(brat_folder, today_date, named_entities, relations, text, article_id)        
        
    
    end_time = time.time()
    parsing_duration = end_time - start_time
    log_str = f"from {date_begin} to {date_end} parsed {len(news)} took time {str(timedelta(seconds=parsing_duration))}"
    print(log_str)
    log.info(log_str)


def main_loop(days=days, hours=hours):
    while True:
        try:
            parse_news(days, hours)
        except (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.InternalError,
            psycopg2.ProgrammingError,
        ) as e:
            today = datetime.now()
            message = f"{today}: {e}.\nPlease check DB connection. Waiting for db repair for 1 hour"
            send_message(tg_token, message, target_chanel_ids)
            log.info(message)
            print(e)
            time.sleep(60 * 60 * 1)
            global news_conn
            global news_cur
            news_conn, news_cur = get_pg(
                news_pg_db,
                news_pg_login,
                news_pg_password,
                postgres_host,
                postgres_port,
            )
            continue

        date_curr = datetime.now().date().strftime("%Y-%m-%d")
        if not os.path.exists("/trade_news_auto_labelling"):
            days += 1
        elif start_date_prev.strftime("%Y-%m-%d") == date_curr:
            print(f'I will sleep for 4 hours. Current time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ')
            time.sleep(60*60*4)
            print(f'I woke up. Current time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ')
 
            continue
        elif  start_date_prev.strftime("%Y-%m-%d") < date_curr:
            continue
        else:
            print("I will sleep for 1 hours")
            time.sleep(60 * 60 * 1)  # 1 hour


if __name__ == "__main__":
    while True:
        try:
            news_conn, news_cur = get_pg(
                news_pg_db,
                news_pg_login,
                news_pg_password,
                postgres_host,
                postgres_port,
            )
            break
        except psycopg2.OperationalError as e:
            today = datetime.now()
            message = f"{today}: {e}.\nPlease check DB connection. Waiting for db repair for 1 hour"
            send_message(tg_token, message, target_chanel_ids)
            log.info(message)

            time.sleep(60 * 60 * 1)
            continue
    # get date to start processing
    start_date_prev: datetime.date = get_start_date() # type: ignore
    print("type(start_date_prev)", type(start_date_prev))
    # get existing entities from db
    query = """
        select distinct article_id from trade_news_article_article_entities 
        """
    news_cur.execute(query)
    processed_news_ids = {p[0] for p in news_cur.fetchall()}
    print(
        "Number of processed articles in trade_news_article_article_entities: ",
        len(processed_news_ids),
    )

    fire.Fire(main_loop)
