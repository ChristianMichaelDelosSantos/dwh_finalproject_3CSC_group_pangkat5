import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/operations")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_orders.parquet"


def load_orders():
    """Load all raw order files into DataFrames."""
    dfs = []

    # Load Parquet files
    for file in SOURCE_DIR.glob("order_data_*.parquet"):
        print(f"Loading {file.name}")
        df = pd.read_parquet(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

     # Load CSV files
    for file in SOURCE_DIR.glob("order_data_*.csv"):
        print(f"Loading {file.name}")
        df = pd.read_csv(file)

        # Drop CSV index column if present
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)
        
        # Load Excel files
    for file in SOURCE_DIR.glob("order_data_*.xlsx"):
        print(f"Loading {file.name}")
        df = pd.read_excel(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)
        
    # Load JSON files
    for file in SOURCE_DIR.glob("order_data_*.json"):
        print(f"Loading {file.name}")
        df = pd.read_json(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)
        
    # Load Pickle files
    for file in SOURCE_DIR.glob("order_data_*.pickle"):
        print(f"Loading {file.name}")
        df = pd.read_pickle(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)
        

    for file in SOURCE_DIR.glob("order_data_*.html"):
        print(f"Loading {file.name}")
        tables = pd.read_html(file)
        df = tables[0]  # first table contains orders
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

    return dfs



def normalize_orders(df):
    # standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # drop junk index columns
    junk_cols = [c for c in df.columns if c.startswith("unnamed")]
    if junk_cols:
        df = df.drop(columns=junk_cols)

    # parse known date columns
    date_cols = ["transaction_date", "estimated_arrival", "order_date"]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df

def quality_checks(df):
    # row-level check
    if df.empty:
        raise ValueError("stage_orders is empty")

    # primary key check
    if "order_id" not in df.columns:
        raise ValueError("order_id column missing")

    if df["order_id"].isna().any():
        raise ValueError("Null order_id values found")

    # date sanity check
    if "order_date" in df.columns:
        if df["order_date"].isna().all():
            raise ValueError("order_date could not be parsed for any row")

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    dfs = load_orders()
    if not dfs:
        raise ValueError("No order files loaded")

    orders = pd.concat(dfs, ignore_index=True)
    orders = normalize_orders(orders)
    quality_checks(orders)

    orders.to_parquet(OUTPUT_FILE, index=False)
    print(f"stg_orders written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()