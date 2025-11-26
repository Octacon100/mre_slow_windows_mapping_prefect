import datetime
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



@task
def extract_geneva_data(account_date_record, type_of_pull):
    # your extraction logic
    pass

@task
def transform_geneva_extract_data(data):
    # your transform logic
    pass

@task
def load_geneva_extract_data(data, type_of_pull):
    # your load logic
    pass

@task  # Make this a subflow
def process_single_account(account_date_record, type_of_pull):
    """Process one account end-to-end"""
    extracted = extract_geneva_data(account_date_record, type_of_pull)
    transformed = transform_geneva_extract_data(extracted)
    load_geneva_extract_data(transformed, type_of_pull)
    return transformed

@flow
def main():
    # Now map the subflow instead of individual tasks
    # This creates far fewer futures to wait on
    
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
    type_of_pull = "bookkeeping"
    results = process_single_account.map(
        accounts_date_df.to_dict("records"),
        type_of_pull=unmapped(type_of_pull)
    )
    results.wait()

if __name__ == "__main__":
    main()