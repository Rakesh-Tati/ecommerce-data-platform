import pandas as pd

df = pd.read_csv("data/raw/orders.csv")

print("Rows:", len(df))
print("Columns:", len(df.columns))

print("\ncolumns:")
print(df.columns)

print("\nnull values:")
print(df.isnull().sum())

print("\nduplicated values:")
print(df.duplicated().sum())
