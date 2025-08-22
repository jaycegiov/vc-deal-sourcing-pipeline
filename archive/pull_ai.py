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
                "sic_codes": item.get("sic_codes"),
                "registered_office": item.get("registered_office_address", {}).get("locality", "")
            })
    else:
        print("Error:", response.status_code, response.text)

# Convert to DataFrame
df = pd.DataFrame(results)

# Drop duplicates by company_number
df = df.drop_duplicates(subset=["company_number"])

print(df.head())

# Save results
df.to_csv("AI_companies_clean.csv", index=False)
print("Saved AI_companies_clean.csv")

