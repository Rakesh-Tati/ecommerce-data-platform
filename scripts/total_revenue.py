import pandas as pd

df = pd.read_csv("data/processed/orders_clean.csv")
total_revenue = df["amount"].sum()
result = pd.DataFrame({"total_revenue": ["total_revenue"]})
result.to_csv("data/analytics/total_revenue.csv", index=False)
print(f"Total revenue: {total_revenue}")
