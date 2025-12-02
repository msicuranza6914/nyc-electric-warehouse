from google.colab import auth
auth.authenticate_user()

from google.cloud import storage
import pandas as pd
import numpy as np

project_id = "nyc-electric-warehouse"
bucket_name = "nyc-electric-msicuranza"

storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)

raw_blob_name = "raw/electric_consumption_2010_2025_raw.csv"
processed_blob_name = "processed/electric_consumption_2010_2025_transformed.csv"

raw_local = "/tmp/electric_raw.csv"
processed_local = "/tmp/electric_transformed.csv"

blob = bucket.blob(raw_blob_name)
blob.download_to_filename(raw_local)

df = pd.read_csv(raw_local)
print("Raw data loaded:", df.shape)

bill_date_col = "Service End Date"
df[bill_date_col] = pd.to_datetime(df[bill_date_col], errors="coerce")

df["BillDate_Std"] = df[bill_date_col].dt.strftime("%Y-%m-%d")
df["Bill_Year"] = df[bill_date_col].dt.year
df["Bill_Quarter"] = df[bill_date_col].dt.quarter
df["Bill_Month"] = df[bill_date_col].dt.month
df["Bill_Day"] = df[bill_date_col].dt.day

df = df.dropna(subset=["BillDate_Std"])

dim_cols = [
    "Development Name", "Borough", "Account Name", "Location", "Meter AMR",
    "Meter Scope", "TDS #", "EDP", "RC Code", "Funding Source", "AMP #",
    "Vendor Name", "UMIS BILL ID", "Revenue Month", "Rate Class", "Estimated",
    "Bill Analyzed", "Meter Number"
]

for col in dim_cols:
    if col in df.columns:
        df[col] = df[col].fillna("Unknown")

fact_cols = [
    "# days", "Current Charges", "Consumption (KWH)", "KWH Charges",
    "Consumption (KW)", "KW Charges", "Other charges"
]

for col in fact_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

existing_fact_cols = [c for c in fact_cols if c in df.columns]
if existing_fact_cols:
    df = df.dropna(subset=existing_fact_cols, how="all")

dup_keys = [c for c in ["BillDate_Std", "Development Name", "Meter Number"] if c in df.columns]
if dup_keys:
    df = df.drop_duplicates(subset=dup_keys)

if {"Consumption (KWH)", "# days"}.issubset(df.columns):
    df["Avg_Daily_kWh"] = df["Consumption (KWH)"] / df["# days"]

if {"Current Charges", "# days"}.issubset(df.columns):
    df["Avg_Daily_Current_Charges"] = df["Current Charges"] / df["# days"]

if {"Current Charges", "Consumption (KWH)"}.issubset(df.columns):
    df["Cost_per_kWh"] = df["Current Charges"] / df["Consumption (KWH)"]
    df["Cost_per_kWh"].replace([np.inf, -np.inf], np.nan, inplace=True)

if "Consumption (KWH)" in df.columns:
    median_kwh = df["Consumption (KWH)"].median()
    df["HighUsageFlag"] = (df["Consumption (KWH)"] > median_kwh).astype("int64")

print("Transformed data shape:", df.shape)

df.to_csv(processed_local, index=False)

processed_blob = bucket.blob(processed_blob_name)
processed_blob.upload_from_filename(processed_local)

print("Transformation complete.")
print("Uploaded to:", processed_blob_name)