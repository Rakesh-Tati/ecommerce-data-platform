import pandas as pd

df = pd.read_csv("data/processed/orders_clean.csv")

city_sales = df.groupby("city")["amount"].sum().reset_index()

city_sales.to_csv("data/analytics/city_sales.csv", index=False)

print("City sales data saved to 'data/analytics/city_sales.csv'")

print(city_sales)
