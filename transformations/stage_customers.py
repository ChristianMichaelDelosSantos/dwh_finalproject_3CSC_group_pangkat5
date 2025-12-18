import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/customer_management")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_customers.parquet"


def load_customers():
    dfs = []

    for file in SOURCE_DIR.glob("*.json"):
        print(f"Loading {file.name}")
        df = pd.read_json(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)
        
    for file in SOURCE_DIR.glob("*.csv"):
        print(f"Loading {file.name}")
        df = pd.read_csv(file)

        junk_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
        if junk_cols:
            df = df.drop(columns=junk_cols)

        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

    for file in SOURCE_DIR.glob("*.pickle"):
        print(f"Loading {file.name}")
        df = pd.read_pickle(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)


    return dfs


def normalize_customers(df):
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

    date_cols = ["creation_date", "birthdate"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
    if "credit_card_number" in df.columns:
        df["credit_card_number"] = df["credit_card_number"].astype("string")


    return df


def quality_checks(df):
    if df.empty:
        raise ValueError("stage_customers is empty")

    if "user_id" not in df.columns:
        raise ValueError("Missing required column: user_id")

    if df["user_id"].isna().any():
        raise ValueError("Null user_id values found")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    dfs = load_customers()
    if not dfs:
        raise ValueError("No customer files loaded")

    customers = pd.concat(dfs, ignore_index=True)
    customers = normalize_customers(customers)
    quality_checks(customers)

    customers.to_parquet(OUTPUT_FILE, index=False)
    print("stage_customers written to staging/stage_customers.parquet")


if __name__ == "__main__":
    main()
