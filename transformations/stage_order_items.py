import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("sources/operations")
OUTPUT_DIR = Path("staging")
OUTPUT_FILE = OUTPUT_DIR / "stage_order_items.parquet"


def load_order_items():
    product_dfs = []
    price_dfs = []

    for file in SOURCE_DIR.glob("line_item_data_products*"):
        print(f"Loading {file.name}")
        df = pd.read_parquet(file) if file.suffix == ".parquet" else pd.read_csv(file)
        product_dfs.append(df)

    for file in SOURCE_DIR.glob("line_item_data_prices*"):
        print(f"Loading {file.name}")
        df = pd.read_parquet(file) if file.suffix == ".parquet" else pd.read_csv(file)
        price_dfs.append(df)

    return product_dfs, price_dfs


def normalize_order_items(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    junk_cols = [c for c in df.columns if c.startswith("unnamed")]
    if junk_cols:
        df = df.drop(columns=junk_cols)

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Only drop columns that actually exist
    required = [c for c in ["order_id", "product_id"] if c in df.columns]
    if required:
        df = df.dropna(subset=required)

    return df

def quality_checks(df):
    if df.empty:
        raise ValueError("stg_order_items is empty")

    required_cols = ["order_id", "product_id"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    for col in required_cols:
        if df[col].isna().any():
            raise ValueError(f"Null values found in {col}")

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    product_dfs, price_dfs = load_order_items()
    if not product_dfs or not price_dfs:
        raise ValueError("Missing product or price files")

    products = pd.concat(product_dfs, ignore_index=True)
    prices = pd.concat(price_dfs, ignore_index=True)

    products = normalize_order_items(products)
    prices = normalize_order_items(prices)

    # JOIN products + prices
    order_items = products.merge(
        prices[["order_id", "price", "quantity"]],
        on="order_id",
        how="left"
    )

    quality_checks(order_items)

    order_items.to_parquet(OUTPUT_FILE, index=False)
    print(f"stage_order_items written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()