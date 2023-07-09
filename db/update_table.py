import os
import time
import requests
from datetime import datetime
import numpy as np
from psycopg2.errors import UndefinedFunction
from sqlalchemy import delete
from sqlalchemy.exc import ProgrammingError
import pickle
from db.models import TradeNewsEmbeddings, TradeNewsRelevant
from sqlalchemy import create_engine, select, MetaData, Table, insert, UniqueConstraint


EMBEDDING_SERVER = os.getenv("EMBEDDING_SERVER")


def get_relevant_data(session):
    data = session.execute(select(TradeNewsRelevant)).all()
    data = [(d[0].id, d[0].title, d[0].article_ids) for d in data]
    return data


def get_embeddings(text: str):
    """
    Возвращает словарь
    {"duration":
    "text":
    "embedding":
    "model":
    }
    """
    embedding = requests.post(EMBEDDING_SERVER, json={"text": text}).json()
    return embedding


def insert_embedding(
    session,
    uuid,
    article_ids: str,
    vector: list,
    model: str,
    date_time,
):
    vector_byte = pickle.dumps(np.array(vector, dtype=np.float16))
    embedding = TradeNewsEmbeddings(
        id=uuid,
        article_id=article_ids,
        embedding=vector_byte,
        model=model,
        date_added=date_time,
    )
    session.add(embedding)

