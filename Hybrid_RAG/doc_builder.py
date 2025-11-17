import pandas as pd
import glob
import os


def make_docs_from_csvs(csv_dir="../qa2_csv_exports"):
    all_docs = []
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))

    print("Found CSVs:", csv_files)

    for file_path in csv_files:
        table_name = os.path.basename(file_path).split(".")[0].lower()
        df = pd.read_csv(file_path)

        print(f"{table_name} → {len(df)} rows")

        for row in df.to_dict(orient="records"):
            text = (
                f"Table: {table_name} | "
                + " | ".join([f"{k}: {v}" for k, v in row.items()])
            )

            all_docs.append(text)
    return all_docs
