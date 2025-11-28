import pandas as pd
from ingestion.utils import load_to_staging, truncate_table, normalize_df, dedupe

DATA_PATH = "/opt/airflow/data_raw/operations/"


# --------------------------------------------------------
# LINE ITEM: PRICES
# --------------------------------------------------------
def ingest_line_item_prices():
    truncate_table("stg_line_item_prices")

    files = [
        "line_item_data_prices1.csv",
        "line_item_data_prices2.csv",
        "line_item_data_prices3.parquet",
    ]

    expected = ["index_raw", "order_id", "price", "quantity"]

    all_frames = []

    # 1️⃣ Read all raw files and normalize them
    for file in files:
        path = DATA_PATH + file

        if file.endswith(".csv"):
            df = pd.read_csv(path)
        else:
            df = pd.read_parquet(path)

        df = normalize_df(df, expected)
        all_frames.append(df)

    # 2️⃣ Now combine all sources
    df = pd.concat(all_frames, ignore_index=True)

    # 3️⃣ Clean quantity
    df["quantity"] = (
        df["quantity"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )

    # 4️⃣ True dedupe (cross-file)
    # If you want one record per order_id + price + quantity:
    df = dedupe(df, key="order_id")

    # 5️⃣ Load final clean table
    load_to_staging(df, "stg_line_item_prices")



# --------------------------------------------------------
# LINE ITEM: PRODUCTS
# --------------------------------------------------------
def ingest_line_item_products():
    truncate_table("stg_line_item_products")

    files = [
        "line_item_data_products1.csv",
        "line_item_data_products2.csv",
        "line_item_data_products3.parquet",
    ]

    expected = ["index_raw", "order_id", "product_name", "product_id"]

    all_frames = []

    # Load ALL files first
    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(DATA_PATH + file)
        else:
            df = pd.read_parquet(DATA_PATH + file)

        df = normalize_df(df, expected)
        all_frames.append(df)

    df = pd.concat(all_frames, ignore_index=True)

    # True dedupe across files
    df = dedupe(df, key=["order_id", "product_id"])

    load_to_staging(df, "stg_line_item_products")



# --------------------------------------------------------
# ORDER DATA
# --------------------------------------------------------
def ingest_order_data():
    truncate_table("stg_order_data")

    files = [
        "order_data_20211001-20220101.csv",
        "order_data_20200701-20211001.pickle",
        "order_data_20220101-20221201.xlsx",
        "order_data_20221201-20230601.json",
        "order_data_20200101-20200701.parquet",
        "order_data_20230601-20240101.html",
    ]

    expected = [
        "index_raw",
        "order_id",
        "user_id",
        "estimated arrival",
        "transaction_date",
    ]

    for file in files:
        path = DATA_PATH + file

        if file.endswith(".csv"):
            df = pd.read_csv(path)
        elif file.endswith(".pickle"):
            df = pd.read_pickle(path)
        elif file.endswith(".xlsx"):
            df = pd.read_excel(path)
        elif file.endswith(".json"):
            df = pd.read_json(path)
        elif file.endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_html(path)[0]

        df = normalize_df(df, expected)
        df = dedupe(df, key="order_id")      # safety dedupe

        load_to_staging(df, "stg_order_data")


# --------------------------------------------------------
# ORDER DELAYS
# --------------------------------------------------------
def ingest_order_delays():
    truncate_table("stg_order_delays")

    df = pd.read_html(DATA_PATH + "order_delays.html")[0]

    expected = ["index_raw", "order_id", "delay in days"]

    df = normalize_df(df, expected)
    df = dedupe(df, key="order_id")          # safety dedupe

    load_to_staging(df, "stg_order_delays")
