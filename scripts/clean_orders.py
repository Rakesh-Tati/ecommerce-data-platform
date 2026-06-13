import pandas as pd

df = pd.read_csv("data/raw/orders.csv")

df = df.drop_duplicates()

df["city"] = df["city"].fillna("Unknown")

df.to_csv("data/processed/orders_clean.csv", index=False)

print("Cleaned data saved to 'data/processed/orders_clean.csv'")
