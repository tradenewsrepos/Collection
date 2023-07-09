from sqlalchemy import Column, ARRAY, Integer, Text, VARCHAR, DateTime
from sqlalchemy.dialects.postgresql import UUID, BYTEA
from sqlalchemy.orm import declarative_base
import uuid
Base = declarative_base()


class TradeNewsEmbeddings(Base):
    __tablename__ = "trade_news_embeddings"

    id = Column("id", UUID(as_uuid=True), primary_key=True)
    embedding = Column(BYTEA, nullable=False)
    article_id = Column(VARCHAR, nullable=True)
    model = Column(VARCHAR, nullable=False)
    date_added = Column(DateTime)

class TradeNewsRelevant(Base):
    __tablename__ = "trade_news_relevant"

    id = Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    classes = Column("classes", ARRAY(Text()))
    itc_codes = Column("itc_codes", Text())
    locations = Column("locations", Text())
    title = Column("title", Text())
    url = Column("url", Text())
    dates = Column("dates", Text())
    article_ids = Column("article_ids", ARRAY(Text()))
    product = Column("product", Text())
    user_checked = Column("user_checked", VARCHAR(30))
    user_approved = Column("user_approved", VARCHAR(30))
    date_checked = Column("date_checked", DateTime())
    date_approved = Column("date_approved", DateTime())
    to_delete = Column("to_delete", Integer)
