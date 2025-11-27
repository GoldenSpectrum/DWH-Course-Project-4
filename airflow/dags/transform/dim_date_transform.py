from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_dim_date():
    truncate_table("dim_date")

    # Fetch raw transaction dates
    order_dates = fetch_df("SELECT transaction_date FROM stg_order_data")

    # Convert, sort, unique
    dates = (
        pd.to_datetime(order_dates["transaction_date"])
        .sort_values()
        .unique()
    )

    # Build dataframe with NEW column names
    df = pd.DataFrame({"date_value": dates})

    df["date_year"] = df["date_value"].dt.year
    df["date_quarter"] = df["date_value"].dt.quarter
    df["date_month"] = df["date_value"].dt.month
    df["date_day"] = df["date_value"].dt.day
    df["day_of_week"] = df["date_value"].dt.weekday
    df["is_weekend"] = df["day_of_week"] >= 5

    # Load into the table
    load_df(df, "dim_date")
