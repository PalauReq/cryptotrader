import logging
from logging.config import fileConfig
from time import asctime, gmtime, sleep

import pandas as pd
from sqlalchemy import create_engine
from trading_api_wrappers import Kraken

logging.config.fileConfig(fname='config/log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

data_file_name = "data/ohlc.csv"


def main():
    since = get_since(data_file_name)
    kraken = Kraken.Public()

    while True:
        logger.info("Starting iteration.")

        config = load_config()

        raw_data = pd.concat([get_data(kraken, pair, config["interval"], since) for pair in config["pairs"]])
        data = transform_data(raw_data)
        append_data(data, data_file_name)

        since = update_since(data)

        logger.info(f"Finished iteration and sleeping for {config['sleep_period'] / 3600} hours.")
        sleep(config["sleep_period"])


def get_since(csv_file_name):
    since = None

    try:
        data = pd.read_csv(csv_file_name)
        since = update_since(data)
    except Exception:
        logger.warning("No csv file found. Setting since to None.")

    return since


def get_data(kraken, pair, interval, since):
    response = kraken.ohlc(pair, interval, since)
    if response["error"]:
        logger.warning(f"Kraken API response returned an error: {response['error']}.")
    data = pd.DataFrame(response["result"][pair],
                        columns=["time", "open", "high", "low", "close", "vwap", "volume", "count"])
    data["pair"] = pair

    return data


def transform_data(raw_data):
    dtype = "float"

    data = raw_data.astype({"open": dtype, "high": dtype, "low": dtype, "close": dtype, "vwap": dtype, "volume": dtype})
    data["timestamp"] = pd.to_datetime(data["time"], unit='s')

    return data


def insert_data(data):
    db_file_name = "data/ohlc.db"
    db_table_name = "ohlc_1"
    logger.debug(f"Inserting a {data.shape} dataframe to {db_file_name}.{db_table_name}.")

    engine = create_engine(f"sqlite:///{db_file_name}", echo=False)
    data.to_sql(db_table_name, con=engine, if_exists='append')
    logger.debug("DB was successfully updated.")


def append_data(data, csv_file_name):
    logger.debug(f"Appending a {data.shape} dataframe to {csv_file_name}.")
    data.to_csv(csv_file_name, mode="a", index=False, header=False)


def load_config():
    config = {"pairs": ["XXBTZEUR", "XETHZEUR"], "interval": 1, "sleep_period": 12 * 60 * 60}
    logger.debug(f"Config updated: {config}.")

    return config


def update_since(data):
    since = data["time"].max()
    logger.debug(f"Since updated to {since}, which is {asctime(gmtime(since))}.")

    return since


if __name__ == "__main__":
    main()
