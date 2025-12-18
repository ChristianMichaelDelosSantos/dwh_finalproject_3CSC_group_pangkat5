import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/business")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_products.parquet"


def load_products():
    dfs = []

    for file in SOURCE_DIR.glob("*.xlsx"):
        print(f"Loading {file.name}")
        df = pd.read_excel(file)
        print(f"{file.name} rows: {df.shape[0]}")
        dfs.append(df)

    return dfs


def normalize_products(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    junk_cols = [c for c in df.columns if c.startswith("unnamed")]
    if junk_cols:
        df = df.drop(columns=junk_cols)

    return df


def quality_checks(df):
    if df.empty:
        raise ValueError("stage_products is empty")

    required_cols = ["product_id"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if df["product_id"].isna().any():
        raise ValueError("Null product_id values found")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    dfs = load_products()
    if not dfs:
        raise ValueError("No product files loaded")

    products = pd.concat(dfs, ignore_index=True)
    products = normalize_products(products)
    quality_checks(products)

    products.to_parquet(OUTPUT_FILE, index=False)
    print("stage_products written to staging/stage_products.parquet")


if __name__ == "__main__":
    main()
