import pandas as pd
import re
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_PATH = os.path.join(BASE_DIR, "data", "silver", "csr_district_clean.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "silver", "silver_csr_district_year.csv")

df = pd.read_csv(INPUT_PATH)

# Extract Year from Financial Year
df["year"] = df["Year"].astype(str).str.extract(r"(\d{4})").astype(int)
df = df.drop(columns=["Year"])

#Rename columns (standardization)
df = df.rename(columns={
    "StateName": "state_name",
    "DistrictName": "district_name",
    "DistrictCode": "district_code",
    "StateCode": "state_code",
    "CSIR spent": "csir_spent_inr_crores"
})

#Convert currency (Crores INR → USD Millions)
USD_INR_RATE = 83.0

df["csir_spent_usd_millions"] = (df["csir_spent_inr_crores"] * 10) / USD_INR_RATE

#Clean column names (uniform style)
df.columns = df.columns.str.strip().str.lower()

#Basic string cleanup
df["state_name"] = df["state_name"].str.strip()
df["district_name"] = df["district_name"].str.strip()

#Save output (clean silver dataset)
df.to_csv(OUTPUT_PATH, index=False)

print("CSR dataset cleaned and standardized")
print(df.head())