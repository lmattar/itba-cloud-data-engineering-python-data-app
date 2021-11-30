from typing import Text
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# symbol, date, open, high, low, close
class stock(Base):
    __tablename__ = "stock"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    date = Column(Date)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)


def create_tables(engine):
    Base.metadata.create_all(engine)
