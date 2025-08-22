import os
import requests
import pandas as pd
import logging
import yaml
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta
from requests.exceptions import RequestException
from tqdm import tqdm


with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


logging.basicConfig(
    level=logging.INFO,  # can change to DEBUG for more details
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                 # logs to console
        logging.FileHandler(config["files"]["log_file"])      # logs to file
    ]
)


# Function to fetch data with retries and exponential backoff
def fetch_with_retries(
    url,
    auth,
    retries=config["api"]["retries"],
    backoff_factor=config["api"]["backoff_factor"]
):

    """
    Fetch data from API with retries & exponential backoff.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, auth=auth, timeout=10)

            # Success case
            if response.status_code == 200:
                return response

            # Handle rate limiting (429 Too Many Requests)
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            # Handle 5xx server errors with retry
            if 500 <= response.status_code < 600:
                wait_time = backoff_factor ** attempt
                logging.error(f"Server error {response.status_code}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            # For other errors, just return immediately
            logging.error(f"Error: {response.status_code}, {response.text}")
            return response

        except RequestException as e:
            wait_time = backoff_factor ** attempt
            logging.error(f"Network error: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    logging.critical("Failed after maximum retries.")
    return None

# Load API key
load_dotenv()
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY")

if not API_KEY:
    raise ValueError("Missing COMPANIES_HOUSE_API_KEY in .env file")

BASE_URL = config["api"]["base_url"]

# Example list of Artificial Intelligence / Machine Learning-related SIC codes
AI_companies_sic_codes = config["sic_codes"]


def fetch_ai_companies():
    results = []

    # Create a dynamic date filter (last 90 days)
    ninety_days_ago = (datetime.today() - timedelta(days=config["filters"]["days_back"])).strftime("%Y-%m-%d")


    # Loop through SIC codes
    for sic in tqdm(AI_companies_sic_codes, desc="Fetching AI Companies"):
        endpoint = f"{BASE_URL}/advanced-search/companies?sic_codes={sic}&incorporated_from={ninety_days_ago}"
        response = fetch_with_retries(endpoint, auth=(API_KEY, ""))

        if response is None:
            # Skip this SIC code if we failed after retries
            continue

        data = response.json()
        for item in data.get("items", []):
            results.append({
                "name": item.get("company_name"),
                "company_number": item.get("company_number"),
                "status": item.get("company_status"),
                "incorporated": item.get("date_of_creation"),
                "company_type": item.get("type"),  # e.g. ltd, plc
                "company_subtype": item.get("subtype"),
                "sic_codes": item.get("sic_codes"),
                "registered_office_locality": item.get("registered_office_address", {}).get("locality", ""),
                "registered_office_postcode": item.get("registered_office_address", {}).get("postal_code", ""),
                "registered_office_address": item.get("registered_office_address", {}).get("address_line_1", ""),
                "previous_names": [prev.get("name") for prev in item.get("previous_company_names", [])] if item.get("previous_company_names") else []
            })




    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Drop duplicates by company_number
    df = df.drop_duplicates(subset=["company_number"])

    # Additional processing steps

    # 1. Normalize SIC codes (keep them as comma-separated strings instead of multiple rows)
    df["sic_codes"] = df["sic_codes"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )


    # 2. Add company age
    df["incorporated"] = pd.to_datetime(df["incorporated"])
    df["age_days"] = (datetime.today() - df["incorporated"]).dt.days
    df["age_years"] = (df["age_days"] / 365).round(1)

    # 3. Flag virtual office / shell addresses
    virtual_addresses = config["filters"]["virtual_addresses"]
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

    logging.debug(f"Preview of companies dataset:\n{df.head()}")

    # Save results
    df.to_csv(config["files"]["enhanced_csv"], index=False)
    logging.info(f"Saved {config['files']['enhanced_csv']}")

    logging.debug(df.head())


    # Filter for VC-focused companies

    # Keep only companies younger than 2 years (safety net)
    df_filtered = df[df["age_years"] < config["filters"]["max_company_age_years"]]

    # Remove companies flagged as virtual office
    df_filtered = df_filtered[df_filtered["is_virtual_office"] == False]

    # Drop duplicates again after filtering & exploding SIC codes
    df_filtered = df_filtered.drop_duplicates(subset=["company_number"])

    # Sort by most recently incorporated
    df_filtered = df_filtered.sort_values(by="incorporated", ascending=False)

    # Save VC-focused dataset
    df_filtered.to_csv(config["files"]["vc_focus_csv"], index=False)
    logging.info(f"Saved {config['files']['vc_focus_csv']}")

    # Fetch officers for each company in the filtered dataset
    officers_results = []

    for company_number in df_filtered["company_number"].unique():
        logging.info(f"Fetching officers for {company_number}...")
        officer_url = f"{BASE_URL}/company/{company_number}/officers"
        officer_response = fetch_with_retries(officer_url, auth=(API_KEY, ""))


        if officer_response.status_code == 200:
            officer_data = officer_response.json()
            for officer in officer_data.get("items", []):
                officers_results.append({
                    "company_number": company_number,
                    "officer_name": officer.get("name"),
                    "officer_role": officer.get("officer_role"),
                    "appointed_on": officer.get("appointed_on"),
                    "resigned_on": officer.get("resigned_on"),
                    "nationality": officer.get("nationality"),
                    "occupation": officer.get("occupation"),
                    "country_of_residence": officer.get("country_of_residence"),
                    "date_of_birth": officer.get("date_of_birth")
                })
        else:
            logging.error(f"Error fetching officers for {company_number}: {officer_response.status_code}")

    df_officers = pd.DataFrame(officers_results)
    df_officers.to_csv(config["files"]["officers_csv"], index=False)
    logging.info(f"Saved {config['files']['officers_csv']}")

    return df, df_filtered, df_officers

if __name__ == "__main__":
    df, df_filtered, df_officers = fetch_ai_companies()

    logging.info("Pipeline finished")

    logging.info("\n--- Sample Companies ---")
    logging.info(f"\n{df.head()}")
    logging.info(f"\n{df_filtered.head()}")
    logging.info(f"\n{df_officers.head()}")


    logging.info("\n--- VC-Focused Companies ---")
    logging.info(df_filtered.head())

    logging.info("\n--- Officers ---")
    logging.info(df_officers.head())


