"""Dummy data model definition."""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockValue(Base):
    """Stock value data model."""

    __tablename__ = "stock_value"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    # More fields maybe?
    ...

    def __repr__(self):
        return f"<StockValue(symbol='{self.symbol}', ...)>"
