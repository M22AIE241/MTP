import pandas as pd
import glob
import os

def make_docs(df, table_name):
    docs = []
    for row in df.to_dict(orient='records'):
        text = (
            f"Table: {table_name} | "
            + " | ".join([f"{k}: {v}" for k, v in row.items()])
        )
        docs.append(text)
    return docs

all_docs = []

csv_files = glob.glob(os.path.join("./qa2_csv_exports", "*.csv"))

print("Found CSV files:", csv_files)

for file_path in csv_files:
    table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
    df = pd.read_csv(file_path)
    print(f"{table_name} → {len(df)} rows")   # debug print
    docs = make_docs(df, table_name)
    all_docs.extend(docs)

print("Total documents created:", len(all_docs))

print("Sample document:")
print(all_docs[0])  # print first document as sample
