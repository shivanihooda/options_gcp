import pytz
import json
import requests
import pandas as pd
from pandas.io import gbq
import pandas_gbq
from datetime import datetime
import yahoo_fin.options as ops
from google.cloud import logging
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='C:\\Users\\MNC\\Desktop\\options_test\\polished-parser-390314-7be98befde9a.json'
# import yahoo_fin.stock_info as stocks

# ticker_list = {"AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "BRK-B", "META", "V", "UNH", "LLY",
#                "XOM", "JNJ", "JPM", "WMT", "MA", "AVGO", "PG", "ORCL", "HD", "CVX", "MRK", "KO", "PEP",
#                "COST", "ABBV", "BAC", "ADBE", "MCD", "CSCO", "PFE", "CRM", "ACN", "TMO", "NFLX", "ABT",
#                "LIN", "AMD", "DHR", "CMCSA", "NKE", "TXN", "DIS", "WFC", "VZ", "UPS", "PM", "NEE", "MS", "INTC"}

ticker_list = ["AAPL"]

log_client = logging.Client()
log_name = "cloud-funtion-log"
logging = log_client.logger(log_name)


def main():
    #   request.json = request.get_json()

    #   if request.args:
    #     get_options_data()
    #     return f'Data pull complete'

    #   elif request_json:
    #     get_options_data()
    #     return f'Data pull complete'

    #   else:
    # logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG

    logging.log_text("This is an info message.")

    get_options_data()
    return f'Data pull complete'


def get_options_data():
    final_json = []
    count = 0
    for ticker in ticker_list:
        try:
            data = ops.get_calls(ticker)
            json_data = json.loads(data.to_json(orient="records"))
            for json_d in json_data:
                revised_json = {
                    "contract_name": json_d["Contract Name"],
                    "last_trade_date": json_d['Last Trade Date'],
                    "strike": json_d['Strike'],
                    "last_price": json_d["Last Price"],
                    "bid": json_d["Bid"],
                    "ask": json_d["Ask"],
                    "change": json_d["Change"],
                    "percent_change": json_d["% Change"],
                    # "volume": json_d["Volume"],
                    "open_interest": json_d["Open Interest"],
                    "implied_volatility": json_d["Implied Volatility"],
                    "option_type": "Call",
                    "timestamp": datetime.now(pytz.timezone('Europe/London')).strftime("%Y:%m:%d %H:%M:%S")
                }
                final_json.append(revised_json)
            data = ops.get_puts(ticker)
            json_data = json.loads(data.to_json(orient="records"))
            for json_d in json_data:
                revised_json = {
                    "contract_name": json_d["Contract Name"],
                    "last_trade_date": json_d['Last Trade Date'],
                    "strike": json_d['Strike'],
                    "last_price": json_d["Last Price"],
                    "bid": json_d["Bid"],
                    "ask": json_d["Ask"],
                    "change": json_d["Change"],
                    "percent_change": json_d["% Change"],
                    # "volume": json_d["Volume"],
                    "open_interest": json_d["Open Interest"],
                    "implied_volatility": json_d["Implied Volatility"],
                    "option_type": "Put",
                    "timestamp": datetime.now(pytz.timezone('Europe/London')).strftime("%Y:%m:%d %H:%M:%S")
                }
                final_json.append(revised_json)
            count = count + 1
            logging.log_text(f"Final json is ready!")
        except Exception as exc:
            logging.log_text("Exception Occurred: ", ticker, " : ", exc)
            continue
    df = pd.DataFrame(final_json)
    temp = pd.to_datetime(df["timestamp"])
    df["timestamp"] = temp
    # df = df["volume"].astype(str).astype(int)
    print(df.dtypes)
    logging.log_text(f"DF is ready!")
    bq_load('options_data_rt', df)


def bq_load(key, value):
    logging.log_text(f"Off to BQ")
    table_id = "polished-parser-390314.options_data.options_data_rt"
    client = bigquery.Client()
    table = client.get_table(table_id)
    job = client.load_table_from_dataframe(value, table)  # Make an API request.
    logging.log_text("Waiting for job to finish!")
    job.result()
    # project_name = 'polished-parser-390314'
    # dataset_name = 'options_data'
    # table_name = key
    #
    # logging.info(f"Off to BQ")
    #
    # value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_name,
    #              if_exists='replace')

    logging.log_text(f"DONE DONE DONE")

main()
