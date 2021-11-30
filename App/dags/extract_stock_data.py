"""Get data from API."""
import json
from datetime import datetime

import numpy as np
import pandas as pd
import requests

BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "PCQVY7EY4NVW7AGD"  # mi api key =
STOCK_FN = "TIME_SERIES_DAILY"


def _get_stock_data(stock_symbol, date):
    date = f"{date:%Y-%m-%d}"  # read execution date from context
    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    data = json.loads(r.content)

    df = (
        pd.DataFrame(data["Time Series (Daily)"])
        .T.reset_index()
        .rename(columns={"index": "date"})
    )

    df = df[df["date"] == date]

    return df


if __name__ == "__main__":
    yesterday = datetime(2021, 11, 4)
    df1 = _get_stock_data("aapl", yesterday)
    print(df1)
