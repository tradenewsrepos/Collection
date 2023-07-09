import time
from datetime import datetime

from psycopg2.errors import UndefinedFunction
from sqlalchemy import delete, inspect
from sqlalchemy.exc import ProgrammingError

from .config import inspector

from make_main_table.db.models import TradeNewsEvents, NewsfeednerExcludedIds, TempEvents
from make_main_table.db.raw_sql.events_query import SQL_TRADEEVENTS
from make_main_table.db.raw_sql.mat_view_create_queries import (
    SQL_drop_mat_views,
    SQL_trade_news_view,
    SQL_trade_news_view_all,
    SQL_events_main,
    SQL_events_minprom,
    SQL_events_union_raw,
)
from make_main_table.db.raw_sql.other_queries import SQL_extension
from make_main_table.logger import write_logs, send_message
from make_main_table.utils.process import process_data


def get_time():
    struct = time.localtime()
    start_time = time.strftime('%d.%m.%Y %H:%M', struct)
    return start_time


def drop_materialized_views(session):
    session.execute(SQL_drop_mat_views)
    session.commit()


def create_materialized_views(session):
    session.execute(SQL_extension)
    session.commit()
    print(f'{get_time()} SQL_extension выполнено')
    session.execute(SQL_trade_news_view)
    session.commit()
    print(f'{get_time()} SQL_trade_news_view выполнено')
    # session.execute(SQL_trade_news_view_all)
    session.execute(SQL_events_main)
    session.commit()
    print(f'{get_time()} SQL_events_main выполнено')
    session.execute(SQL_events_minprom)
    session.commit()
    print(f'{get_time()} SQL_events_minprom выполнено')
    session.execute(SQL_events_union_raw)
    print(f'{get_time()} SQL_events_union_raw выполнено')
    session.commit()


def refresh_materialized_view(session):
    """
    Метод запускает существующую функцию в базе данных,
    которая обновляет материализованные представления
    """
    while True:
        try:
            session.execute("select refresh_trade_events_view();")
            break
        except ProgrammingError as e:
            write_logs("error", error=e)
            today = datetime.today()
            message = f"{today}: {e}.\nSomething wrong with matview refresh function. Waiting for db repair for 24 hours"
            send_message(message)
            time.sleep(60 * 60 * 24)
            continue


def select_from_mat_view(session):
    """
    Возвращает данные со статусами новостей.
    После обновления материализованного представления данные приходят только со статусом
    not_seen
    """
    result = session.execute(SQL_TRADEEVENTS)
    return result


def drop_table(engine):
    TradeNewsEvents.__table__.drop(engine, checkfirst=True)

def drop_table_temp(engine):
    TempEvents.__table__.drop(engine, checkfirst=True)    


def create_table(engine):
    TradeNewsEvents.__table__.create(engine, checkfirst=True)

def create_table_temp(engine):
    TempEvents.__table__.create(engine, checkfirst=True) 
    
def copy_table (session):
    query = """
        INSERT INTO trade_news_events (id, classes, itc_codes, locations, title, url, dates, article_ids, product, status)
        SELECT id, classes, itc_codes, locations, title, url, dates, article_ids, product, status FROM temp_events;
    """    
    session.execute(query)   


def clear_table(session):
    """
    Метод очищает таблицу от всех новостей, включая уже обработанные,
    чтобы потом наполнить таблицу новыми данными
    """
    session.execute(delete(TradeNewsEvents))
    session.commit()


def update_table(engine, session):
    if inspector.has_table("trade_news_events", None) and inspector.has_table("newsfeedner_excludedids", None):
        query_events = select_from_mat_view(session)
        # Внесены изменения: запись исключенных записей в таблицу. 
        # Добавлена result_processed = process_data(query_events)
        # process_data возвращает кортеж: Dict и list
        result_processed = process_data(query_events)
        query_events_processed = result_processed[0]
        query_events_sorted = sorted(
            query_events_processed, key=lambda row: row["dates"], reverse=True
        )
        events = [TempEvents(**event) for event in query_events_sorted]
        session.add_all(events)
        # Добавлено: Добавление исключенных записей в таблицу newsfeedner_excludedids -нужно протестировать
        query_ex = """
                SELECT * FROM newsfeedner_excludedids
                """
        query_excluded = session.execute(query_ex)   
        
        excluded_idx = result_processed[1]
        for item in query_excluded:
            if item.excluded_id in excluded_idx:
                excluded_idx.remove(item.excluded_id)
        excluded_ids = [NewsfeednerExcludedIds(excluded_id=excluded_id_x) for excluded_id_x  in iter(excluded_idx)]
        if len(excluded_ids)>0:
            session.add_all(excluded_ids)
        
        session.commit()
