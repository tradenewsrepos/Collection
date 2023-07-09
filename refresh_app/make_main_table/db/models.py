from sqlalchemy import Column, ARRAY, Date, Integer, Text, VARCHAR, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TradeNewsEventsRaw(Base):
    __tablename__ = "trade_news_events_raw"

    id = Column("id", UUID(as_uuid=True), primary_key=True)
    classes = Column("classes", ARRAY(Text()))
    itc_codes = Column("itc_codes", Text())
    locations = Column("locations", Text())
    title = Column("title", Text())
    url = Column("url", Text())
    dates = Column("dates", ARRAY(Text()))
    article_ids = Column("article_ids", ARRAY(Text()))
    product = Column("product", Text())


class TradeNewsEvents(Base):
    __tablename__ = "trade_news_events"

    id = Column("id", UUID(as_uuid=True), primary_key=True)
    classes = Column("classes", ARRAY(Text()))
    itc_codes = Column("itc_codes", Text())
    locations = Column("locations", Text())
    title = Column("title", Text())
    url = Column("url", Text())
    dates = Column("dates", Text())
    article_ids = Column("article_ids", ARRAY(Text()))
    product = Column("product", Text())
    status = Column("status", Text())

    def __repr__(self):
        return f"TradeNewsEvents(id={self.id})"

class TempEvents(Base):
    __tablename__ = "temp_events"

    id = Column("id", UUID(as_uuid=True), primary_key=True)
    classes = Column("classes", ARRAY(Text()))
    itc_codes = Column("itc_codes", Text())
    locations = Column("locations", Text())
    title = Column("title", Text())
    url = Column("url", Text())
    dates = Column("dates", Text())
    article_ids = Column("article_ids", ARRAY(Text()))
    product = Column("product", Text())
    status = Column("status", Text())

    def __repr__(self):
        return f"TempEvents(id={self.id})"

class NewsfeednerExcludedIds(Base):
    __tablename__ = "newsfeedner_excludedids"
    excluded_id = Column("excluded_id", Integer, primary_key=True)
