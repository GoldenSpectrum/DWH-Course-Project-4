from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_fact_orders():
    truncate_table("fact_orders")

    orders = fetch_df("SELECT * FROM stg_order_data")
    merchant = fetch_df("SELECT * FROM stg_order_with_merchant")
    delays = fetch_df("SELECT * FROM stg_order_delays")

    df = (
        orders
        .merge(merchant, on="order_id", how="left")
        .merge(delays, on="order_id", how="left")
    )

    df = df.rename(columns={
        "estimated arrival": "order_estimated_arrival",
        "delay in days": "order_delay_in_days",
        "transaction_date": "order_transaction_date"
    })

    df["order_estimated_arrival"] = (
        df["order_estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
    )[0]

    df["order_estimated_arrival"] = (
        pd.to_numeric(df["order_estimated_arrival"], errors="coerce")
          .fillna(0)
          .astype(int)
    )

    df["order_delay_in_days"] = (
        df["order_delay_in_days"]
        .astype(str)
        .str.extract(r"(\d+)")
    )[0]

    df["order_delay_in_days"] = (
        pd.to_numeric(df["order_delay_in_days"], errors="coerce")
          .fillna(0)
          .astype(int)
    )

    df = df[[
        "order_id", "user_id", "order_transaction_date",
        "order_estimated_arrival", "merchant_id",
        "staff_id", "order_delay_in_days"
    ]]

    load_df(df, "fact_orders")
