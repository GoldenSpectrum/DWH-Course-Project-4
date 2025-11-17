import pandas as pd
from ingestion.db import load_to_staging

DATA_PATH = "/opt/airflow/data_raw/customer/"

def ingest_user_job():
    df = pd.read_csv(DATA_PATH + "user_job.csv")
    df = df.drop(columns=["Unnamed: 0"])

    df["job_level"] = df["job_level"].fillna("Unknown")

    load_to_staging(df, "stg_user_job")


def ingest_user_data():
    df = pd.read_json(DATA_PATH + "user_data.json")
    load_to_staging(df, "stg_user_data")


def ingest_user_credit_card():
    df = pd.read_pickle(DATA_PATH + "user_credit_card.pickle")
    load_to_staging(df, "stg_user_credit_card")
