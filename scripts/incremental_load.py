import pandas as pd

df = pd.read_csv("data/raw/orders.csv")
with open("metadata/watermark.txt", "r") as f:
    watermark = f.read().strip()
    
    new_order = df[df["order_date"] > watermark]
    print(new_order)