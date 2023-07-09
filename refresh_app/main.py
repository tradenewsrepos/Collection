import os
import time

from datetime import timedelta, datetime

from make_main_table.db.config import Session, engine, inspector
from make_main_table.db.update_table import (
    get_time,
    drop_materialized_views,
    create_materialized_views,
    drop_table,
    create_table,
    create_table_temp,
    drop_table_temp,
    copy_table,
    update_table,
    clear_table,
    refresh_materialized_view,
)
from make_main_table.logger import write_logs, send_message
from sqlalchemy.exc import OperationalError, DBAPIError

# Добавлено 06.06.2023
REFRESH_TIME = os.getenv("REFRESH_TIME")

def get_sleep_time(days_delta=1) -> int:
    now = datetime.today()
    tomorrow = now + timedelta(days=days_delta)
    hour_min = REFRESH_TIME.split(':', 2)
    # Refresh at night REFRESH_TIME UTC
    refresh_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, int(hour_min[0]), int(hour_min[1]), 0, 0)
    sleep = (refresh_time - now).seconds
    return sleep


if __name__ == "__main__":
    """
    Данный скрипт реализует создание таблицы из материализованных представлений.
    В свою очередь материалозованные представления создаются каждые сутки в REFRESH_TIME заданное в .env
    в временной зоне UTC. При создании матпредставлений не учитываются article_id новостей, которые ранее
    были проверены, исключены или согласованы ( excluded_ids, duplicated_ids, approved_ids, checked_ids)
    При каждом запуске выполняется такой алгоритм:
    - обновляются матпредставления;
    - удаляются данные из существующей таблицы;
    - копируются все данные из мат представления;
    - данный сортируются в хронологическом порядке.
    """
    print(f'tn_refresh container started.   {get_time()}')
    first_launch_sleep = get_sleep_time()
    time.sleep(first_launch_sleep)
    print(f'Starting refresh.   {get_time()}')

    while True:
        time_start = time.time()
        with Session() as session:
            print("Start time:", {get_time()})
            send_message(f"Start time: {get_time()}")
            drop_materialized_views(session)
            print(f"{get_time()} Existing views droped")
            try:
                create_materialized_views(session)
                print(f"{get_time()} materialized_views created")
            except DBAPIError as ex:
                send_message("DBAPIError при создании мат. представлений")
                write_logs("error", error=ex)
    
                # handle psycopg2.errors.DiskFull
                if ex.orig.pgcode == '23502':
                    send_message("psycopg2.errors.DiskFull: Нет места. Сообщить админу.")
                else:
                    send_message("DBAPIError не связана с отсутствием места (Diskfull). Сообщить админу.")
                sleep_time = get_sleep_time()
                time.sleep(sleep_time)
            if not inspector.has_table("trade_news_events"):    
                create_table(engine)
                print(f"{get_time()} create target table")
            if not inspector.has_table("temp_events"):      
                create_table_temp(engine)
                print(f"{get_time()} create temporary table")
            else:
                drop_table_temp(engine)
                create_table_temp(engine)     
            update_table(engine, session)
            drop_table(engine)
            create_table(engine)
            copy_table(session)
            print(f"{get_time()} copy temporary table to target table" )
            send_message(f"{get_time()} copy temporary table to target table")
            session.commit()
            
            time_stop = time.time()
            update_duration = time_stop - time_start
            write_logs("success", duration=update_duration)
            print('Все выполнено', get_time())
            send_message(f'Все выполнено за {update_duration} в {get_time()}')
            sleep_time = get_sleep_time()
            time.sleep(sleep_time)

