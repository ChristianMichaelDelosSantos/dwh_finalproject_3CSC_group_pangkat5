import pandas as pd
from pathlib import Path

# Paths
STAGING_DIR = Path("staging")
DASHBOARD_DATA_DIR = Path("dashboards/data")

DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_staging():
    orders = pd.read_parquet(STAGING_DIR / "stage_orders.parquet")
    order_items = pd.read_parquet(STAGING_DIR / "stage_order_items.parquet")
    products = pd.read_parquet(STAGING_DIR / "stage_products.parquet")
    customers = pd.read_parquet(STAGING_DIR / "stage_customers.parquet")
    campaigns = pd.read_parquet(STAGING_DIR / "stage_campaigns.parquet")
    merchants = pd.read_parquet(STAGING_DIR / "stage_merchants.parquet")

    return orders, order_items, products, customers, campaigns, merchants

def generate_kpi_summary(orders, order_items, customers, campaigns):
    total_revenue = order_items["price"].sum()
    total_orders = orders["order_id"].nunique()
    unique_customers = customers["user_id"].nunique()

    campaign_orders = campaigns["order_id"].nunique()
    campaign_revenue = order_items[
        order_items["order_id"].isin(campaigns["order_id"])
    ]["price"].sum()

    kpi_df = pd.DataFrame({
        "metric": [
            "Total Revenue",
            "Total Orders",
            "Unique Customers",
            "Campaign Orders",
            "Campaign Revenue"
        ],
        "value": [
            total_revenue,
            total_orders,
            unique_customers,
            campaign_orders,
            campaign_revenue
        ]
    })

    kpi_df.to_csv(DASHBOARD_DATA_DIR / "kpi_summary.csv", index=False)
    print("** kpi_summary.csv created **")

def generate_revenue_by_product(order_items, products):
    df = order_items.merge(
        products[["product_id", "product_name", "product_type"]],
        on="product_id",
        how="left",
        suffixes=("_oi", "_prod")
    )

    df = df.rename(columns={"product_name_prod": "product_name"})

    revenue_by_product = (
        df.groupby("product_name", as_index=False)
        .agg(
            total_revenue=("price", "sum"),
            total_quantity=("quantity", "sum")
        )
        .sort_values("total_revenue", ascending=False)
    )

    revenue_by_product.to_csv(
        DASHBOARD_DATA_DIR / "revenue_by_product.csv",
        index=False
    )

    print("** revenue_by_product.csv created **")
    
def generate_revenue_by_product_type(order_items, products):
    df = order_items.merge(
        products[["product_id", "product_name", "product_type"]],
        on="product_id",
        how="left",
        suffixes=("_oi", "_prod")
    )

    df = df.rename(columns={"product_type_prod": "product_type"})

    revenue_by_type = (
        df.groupby("product_type", as_index=False)
        .agg(total_revenue=("price", "sum"))
        .sort_values("total_revenue", ascending=False)
    )

    revenue_by_type.to_csv(
        DASHBOARD_DATA_DIR / "revenue_by_product_type.csv",
        index=False
    )

    print("** revenue_by_product_type.csv created **")

def generate_revenue_by_date(orders, order_items):
    orders["transaction_date"] = pd.to_datetime(
        orders["transaction_date"], errors="coerce"
    )

    df = order_items.merge(
        orders[["order_id", "transaction_date"]],
        on="order_id",
        how="left"
    )

    df["date"] = df["transaction_date"].dt.date

    revenue_by_date = (
        df.groupby("date", as_index=False)
        .agg(total_revenue=("price", "sum"))
        .sort_values("date")
    )

    revenue_by_date.to_csv(
        DASHBOARD_DATA_DIR / "revenue_by_date.csv",
        index=False
    )

    print("** revenue_by_date.csv created **")


def generate_campaign_performance(campaigns, order_items):
    df = campaigns.merge(order_items, on="order_id", how="left")

    campaign_perf = (
        df.groupby("campaign_id", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("price", "sum"),
            availed_rate=("availed", "mean")
        )
        .sort_values("total_revenue", ascending=False)
    )

    campaign_perf.to_csv(
        DASHBOARD_DATA_DIR / "campaign_performance.csv",
        index=False
    )

    print("** campaign_performance.csv created **")

def generate_customer_ltv(orders, order_items, customers):
    df = orders.merge(order_items, on="order_id", how="left")
    df = df.merge(customers, on="user_id", how="left")

    clv = (
        df.groupby("user_type", as_index=False)
        .agg(
            total_revenue=("price", "sum"),
            total_orders=("order_id", "nunique")
        )
        .sort_values("total_revenue", ascending=False)
    )

    clv.to_csv(
        DASHBOARD_DATA_DIR / "customer_lifetime_value.csv",
        index=False
    )

    print("** customer_lifetime_value.csv created **")

if __name__ == "__main__":
    orders, order_items, products, customers, campaigns, merchants = load_staging()

    generate_kpi_summary(orders, order_items, customers, campaigns)
    generate_revenue_by_product(order_items, products)
    generate_revenue_by_product_type(order_items, products)
    generate_revenue_by_date(orders, order_items)
    generate_campaign_performance(campaigns, order_items)
    generate_customer_ltv(orders, order_items, customers)
