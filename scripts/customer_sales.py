import pandas as pd

df = pd.read_csv("data/processed/orders_clean.csv")
customer_sales = (
    df.groupby("customer_name")["amount"]
    .sum()
    .reset_index()
    .sort_values(by="amount", ascending=False)
)
customer_sales.to_csv("data/analytics/customer_sales.csv", index=False)
print("Customer sales data saved to 'data/analytics/customer_sales.csv'")
print(customer_sales)
