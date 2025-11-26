import socket
# At the very top of your script, before importing prefect
socket.getaddrinfo('api.prefect.cloud', 443)

# import os
# os.environ["PREFECT_API_ENABLE_HTTP2"] = "false"

import datetime
import random
import time
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from prefect import flow, task, get_run_logger, unmapped

# Dummy schema and mappings
table_names_dict = {"BookKeeping": "bisBookkeepingAccount"}
file_types_dict = {"BookKeeping": "BookKeeping"}


@dataclass
class GenevaExtractData:
    pulled_data: Optional[pd.DataFrame]
    date: datetime.date
    fund_id: int


# Dummy data generator
def create_geneva_dataframe():
    return pd.DataFrame(
        {
            "InvestmentChainId": [1, 2],
            "Quantity_": [100, 200],
            "TaxLotId_": [10, 20],
            "IsBasketSwap": [0, 1],
            "GenericInvestmentChainId": [999, 888],
            "GenericPriceLocal": [50.0, 60.0],
            "PriceLocal": [55.0, 65.0],
            "FundId": [6005, 7000],
        }
    )


@task
def extract_geneva_data(update_row, type_of_pull: str) -> GenevaExtractData:
    logger = get_run_logger()
    logger.info(
        f"Started first step of mapped steps at {datetime.datetime.now().strftime('%Y/%m/%d:%H:%M:%S')}"
    )
    logger.info(f"Extracting for fund {update_row['FundId']} on {update_row['Date']}")
    time.sleep(random.uniform(0.5, 1.0))  # Simulate delay
    data = create_geneva_dataframe()
    return GenevaExtractData(
        pulled_data=data, date=update_row["Date"], fund_id=update_row["FundId"]
    )


@task
def transform_geneva_extract_data(data_input: GenevaExtractData) -> GenevaExtractData:
    logger = get_run_logger()
    df = data_input.pulled_data
    logger.info(f"{df=}")
    if not df.empty:
        df = df.rename({"Quantity_": "Quantity", "TaxLotId_": "TaxLotId"}, axis=1)
        df["CalcInvestmentChainId"] = np.where(
            df["IsBasketSwap"] == 1,
            df["GenericInvestmentChainId"],
            df["InvestmentChainId"],
        )
        df["CalcPriceLocal"] = np.where(
            df["IsBasketSwap"] == 1, df["GenericPriceLocal"], df["PriceLocal"]
        )
        data_input.pulled_data = df
    return data_input


@task
def load_geneva_extract_data(data_input: GenevaExtractData, type_of_pull: str):
    logger = get_run_logger()
    logger.info(
        f"Loading {len(data_input.pulled_data)} rows for fund {data_input.fund_id}"
    )
    time.sleep(0.5)  # Simulate DB load


@flow(name="Geneva Extract MRE")
def extract_geneva_data_pre_backtest_run(type_of_pull: str = "BookKeeping"):
    # Minimal dummy input
    logger = get_run_logger()
    accounts_date_df = pd.DataFrame(
        [
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
            {"Date": datetime.date.today(), "FundId": 6005},
            {"Date": datetime.date.today(), "FundId": 7000},
        ]
    )
    logger.info(
        f"Starting to map at {datetime.datetime.now().strftime('%Y/%m/%d:%H:%M:%S')}"
    )
    results = extract_geneva_data.map(
        accounts_date_df.to_dict("records"), type_of_pull=unmapped(type_of_pull)
    )
    logger.info(
        f"Starting to map 2nd step at {datetime.datetime.now().strftime('%Y/%m/%d:%H:%M:%S')}"
    )
    transformed = transform_geneva_extract_data.map(results)
    logger.info(
        f"Starting to map 3rd step at {datetime.datetime.now().strftime('%Y/%m/%d:%H:%M:%S')}"
    )
    load_geneva_extract_data.map(transformed, type_of_pull=unmapped(type_of_pull))
    transformed.wait()


if __name__ == "__main__":
    extract_geneva_data_pre_backtest_run()
