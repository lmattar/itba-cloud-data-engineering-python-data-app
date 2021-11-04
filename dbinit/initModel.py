from typing import Text
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy.orm import declarative_base

engine = create_engine('postgresql://postgres:postgres@localhost:5433/stockdb')

Base = declarative_base()

#symbol, date, open, high, low, close
class stock(Base):
    __tablename__ = 'stock'

    id = Column(Integer, primary_key=True,autoincrement=True)
    symbol = Column(String)
    date = Column(Date)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)




Base.metadata.create_all(engine)



with engine.connect() as conn:
    result = conn.execute(Text("INSERT INTO STOCK (symbol) VALUES('GOOG') "))
    result = conn.execute(Text("SELECT * FROM stock"))
    
    for  row in result:
        print(row)