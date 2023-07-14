import pytz
import json
import requests
import pandas as pd
from datetime import datetime
import yahoo_fin.options as ops
from google.cloud import logging
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='C:\\Users\\MNC\\Desktop\\options_test\\polished-parser-390314-7be98befde9a.json'

ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "BRK-B", "META", "V", "UNH", "LLY",
               "XOM", "JNJ", "JPM", "WMT", "MA", "AVGO", "PG", "ORCL", "HD", "CVX", "MRK", "KO", "PEP",
               "COST", "ABBV", "BAC", "ADBE", "MCD", "CSCO", "PFE", "CRM", "ACN", "TMO", "NFLX", "ABT",
               "LIN", "AMD", "DHR", "CMCSA", "NKE", "TXN", "DIS", "WFC", "VZ", "UPS", "PM", "NEE", "MS", "INTC"
               ]

# ticker_list = ["AAPL"]

log_client = logging.Client()
log_name = "cloud-funtion-log"
logging = log_client.logger(log_name)


def main():

    logging.log_text("This is an info message.")
    get_options_data()

    return f'Data pull complete'


def get_options_data():
    try:
        final_json = []
        count = 0
        for ticker in ticker_list:
            logging.log_text(f"Ticker: {str(ticker)}")
            try:
                expiration_dates_list = ops.get_expiration_dates(ticker)
                expiration_dates = []
                for expiration_date in expiration_dates_list:
                    if "2023" in expiration_date:
                        expiration_dates.append(expiration_date)
                for expiration_date in expiration_dates:
                    data = ops.get_calls(ticker, expiration_date)
                    json_data = json.loads(data.to_json(orient="records"))
                    for json_d in json_data:
                        revised_json = {
                            "ticker_symbol": ticker,
                            "timestamp": datetime.now(pytz.timezone('Europe/London')).strftime("%Y:%m:%d %H:%M:%S"),
                            "expiration_date": expiration_date,
                            "option_type": "Call",
                            "strike": json_d['Strike'],
                            "last_price": json_d["Last Price"],
                            "bid": json_d["Bid"],
                            "ask": json_d["Ask"],
                            "change": json_d["Change"],
                            "percent_change": json_d["% Change"],
                            "volume": str(json_d["Volume"]),
                            "open_interest": json_d["Open Interest"],
                            "implied_volatility": json_d["Implied Volatility"],
                            "last_trade_date": json_d['Last Trade Date'],
                            "contract_name": json_d["Contract Name"]
                        }
                        final_json.append(revised_json)
                    data = ops.get_puts(ticker, expiration_date)
                    json_data = json.loads(data.to_json(orient="records"))
                    for json_d in json_data:
                        revised_json = {
                            "ticker_symbol": ticker,
                            "timestamp": datetime.now(pytz.timezone('Europe/London')).strftime("%Y:%m:%d %H:%M:%S"),
                            "expiration_date": expiration_date,
                            "option_type": "Put",
                            "strike": json_d['Strike'],
                            "last_price": json_d["Last Price"],
                            "bid": json_d["Bid"],
                            "ask": json_d["Ask"],
                            "change": json_d["Change"],
                            "percent_change": json_d["% Change"],
                            "volume": str(json_d["Volume"]),
                            "open_interest": json_d["Open Interest"],
                            "implied_volatility": json_d["Implied Volatility"],
                            "last_trade_date": json_d['Last Trade Date'],
                            "contract_name": json_d["Contract Name"]
                        }
                        final_json.append(revised_json)
                    count = count + 1
            except Exception as exc:
                print("Exception Occurred in payload: ", ticker, " : ", exc)
                continue
        logging.log_text(f"Final json is ready!")
        df = pd.DataFrame(final_json)
        temp = pd.to_datetime(df["timestamp"])
        df["timestamp"] = temp
        expiration_date_temp = pd.to_datetime(df["expiration_date"], format="%B %d, %Y")
        df["expiration_date"] = expiration_date_temp
        logging.log_text(f"DF is ready!")
        bq_load(df)
    except Exception as exc:
        print(f"Exception Occurred: {exc}")


def bq_load(value):
    try:
        logging.log_text(f"Off to BQ")
        table_id = "polished-parser-390314.options_data.options_data_real_time"
        client = bigquery.Client()
        table = client.get_table(table_id)
        job = client.load_table_from_dataframe(value, table)  # Make an API request.
        logging.log_text("Waiting for job to finish!")
        job.result()
        logging.log_text(f"DONE DONE DONE")
    except Exception as exc:
        print("Exception occurred in sending data to big query: ", exc)


main()
