import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    return create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres:5432/shopzada"
    )


def load_to_staging(df, table_name):
    engine = get_engine()

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
