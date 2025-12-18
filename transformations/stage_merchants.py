import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/enterprise")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_merchants.parquet"


def load_merchants():
    dfs = []

    for file in SOURCE_DIR.glob("merchant_data*.html"):
        print(f"Loading {file.name}")
        tables = pd.read_html(file)

        # assume first table contains merchant data
        df = tables[0]
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

    return dfs


def normalize_merchants(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    junk_cols = [c for c in df.columns if c.startswith("unnamed")]
    if junk_cols:
        df = df.drop(columns=junk_cols)


    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    return df


def quality_checks(df):
    if df.empty:
        raise ValueError("stage_merchants is empty")

    required_cols = ["merchant_id"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if df["merchant_id"].isna().any():
        raise ValueError("Null merchant_id values found")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    dfs = load_merchants()
    if not dfs:
        raise ValueError("No merchant files loaded")

    merchants = pd.concat(dfs, ignore_index=True)
    merchants = normalize_merchants(merchants)
    quality_checks(merchants)

    merchants.to_parquet(OUTPUT_FILE, index=False)
    print("stage_merchants written to staging/stage_merchants.parquet")


if __name__ == "__main__":
    main()
