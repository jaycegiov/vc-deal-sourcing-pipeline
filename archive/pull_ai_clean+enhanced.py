import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load API key
load_dotenv()
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY")

BASE_URL = "https://api.company-information.service.gov.uk"

# Example list of Artificial Intelligence / Machine Learning-related SIC codes
AI_companies_sic_codes = ["62012", "63110", "62012", "62020", "63120", "63990","72190","71122"]

results = []

# Create a dynamic date filter (last 90 days)
ninety_days_ago = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Loop through SIC codes
for sic in AI_companies_sic_codes:
    print(f"Fetching companies with SIC {sic}...")
    endpoint = f"{BASE_URL}/advanced-search/companies?sic_codes={sic}&incorporated_from={ninety_days_ago}"

    response = requests.get(endpoint, auth=(API_KEY, ""))

    if response.status_code == 200:
        data = response.json()
        for item in data.get("items", []):
            results.append({
                "name": item.get("company_name"),
                "company_number": item.get("company_number"),
                "status": item.get("company_status"),
                "incorporated": item.get("date_of_creation"),
                "company_type": item.get("type"),  # e.g. ltd, plc
                "company_subtype": item.get("subtype"),  # sometimes useful
                "sic_codes": item.get("sic_codes"),
                "registered_office_locality": item.get("registered_office_address", {}).get("locality", ""),
                "registered_office_postcode": item.get("registered_office_address", {}).get("postal_code", ""),
                "registered_office_address": item.get("registered_office_address", {}).get("address_line_1", ""),
                "previous_names": [prev.get("name") for prev in item.get("previous_company_names", [])] if item.get("previous_company_names") else []
            })
    else:
        print("Error:", response.status_code, response.text)



# Convert to DataFrame
df = pd.DataFrame(results)

# Drop duplicates by company_number
df = df.drop_duplicates(subset=["company_number"])

# Additional processing steps

# 1. Normalize SIC codes (explode into separate rows)
df = df.explode("sic_codes")

# 2. Add company age
df["incorporated"] = pd.to_datetime(df["incorporated"])
df["age_days"] = (datetime.today() - df["incorporated"]).dt.days
df["age_years"] = (df["age_days"] / 365).round(1)

# 3. Flag virtual office / shell addresses
virtual_addresses = ["71-75 Shelton Street", "128 City Road"]
df["is_virtual_office"] = df["registered_office_address"].isin(virtual_addresses)

# 4. Reorder columns for readability
df = df[
    [
        "name",
        "incorporated",
        "age_years",
        "status",
        "sic_codes",
        "company_number",
        "company_type",
        "company_subtype",
        "registered_office_address",
        "registered_office_locality",
        "registered_office_postcode",
        "previous_names",
        "is_virtual_office",
    ]
]

print(df.head())

# Save results
df.to_csv("AI_companies_enhanced.csv", index=False)
print("Saved AI_companies_enhanced.csv")

print(df.head())

# Save results (full enhanced dataset)
df.to_csv("AI_companies_enhanced.csv", index=False)
print("Saved AI_companies_enhanced.csv")

# Filter for VC-focused companies

# Keep only companies younger than 2 years (safety net)
df_filtered = df[df["age_years"] < 2]

# Remove companies flagged as virtual office
df_filtered = df_filtered[df_filtered["is_virtual_office"] == False]

# Drop duplicates again after filtering & exploding SIC codes
df_filtered = df_filtered.drop_duplicates(subset=["company_number"])

# Sort by most recently incorporated
df_filtered = df_filtered.sort_values(by="incorporated", ascending=False)

# Save VC-focused dataset
df_filtered.to_csv("AI_companies_vc_focus.csv", index=False)
print("Saved AI_companies_vc_focus.csv")

