import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/marketing")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_campaigns.parquet"


def load_campaigns():
    dfs = []

    for file in SOURCE_DIR.glob("*.csv"):
        print(f"Loading {file.name}")

        if file.name == "campaign_data.csv":
            df = pd.read_csv(file, sep="\t")
        else:
            df = pd.read_csv(file)

        junk_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
        if junk_cols:
            df = df.drop(columns=junk_cols)

        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

    return dfs


def normalize_campaigns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # parse date columns if present
    date_cols = ["campaign_date", "sent_date", "transaction_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def quality_checks(df):
    if df.empty:
        raise ValueError("stage_campaigns is empty")

    # at least one identifying column must exist
    possible_keys = ["campaign_id", "transaction_id"]
    if not any(col in df.columns for col in possible_keys):
        raise ValueError("No campaign identifier column found")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    dfs = load_campaigns()
    if not dfs:
        raise ValueError("No campaign files loaded")

    campaigns = pd.concat(dfs, ignore_index=True)
    campaigns = normalize_campaigns(campaigns)
    quality_checks(campaigns)

    campaigns.to_parquet(OUTPUT_FILE, index=False)
    print("stage_campaigns written to staging/stage_campaigns.parquet")


if __name__ == "__main__":
    main()
