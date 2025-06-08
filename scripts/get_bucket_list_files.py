"""
This script lists files in a Google Cloud Storage bucket, processes the file names to extract dates,
filters them, and saves the list to a Parquet file.
"""

import pandas as pd
from google.cloud import storage
import google.auth

# --- Constants ---
BUCKET_NAME = "meteofrancedata"
OUTPUT_FILE = "list_files.parquet"


def get_file_list_from_bucket(bucket_name: str) -> list[str]:
    """
    Lists all files in a given GCS bucket.

    Args:
        bucket_name: The name of the GCS bucket.

    Returns:
        A list of file names.
    """
    try:
        # storage.Client() will automatically find and use credentials from
        # the environment, including those set by `gcloud auth login`.
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        return [blob.name for blob in bucket.list_blobs()]
    except google.auth.exceptions.DefaultCredentialsError:
        print("Authentication failed. Please run 'gcloud auth login' or set GOOGLE_APPLICATION_CREDENTIALS.")
        return []
    except Exception as e:
        print(f"An error occurred while accessing the bucket: {e}")
        return []


def process_file_list(file_list: list[str]) -> pd.DataFrame:
    """
    Processes a list of file names to extract and format dates.

    Args:
        file_list: A list of file names.

    Returns:
        A pandas DataFrame with processed and filtered data.
    """
    if not file_list:
        return pd.DataFrame(columns=['name', 'date'])

    df = pd.DataFrame(file_list, columns=['name'])
    # Extract date from filenames like 'T_IMFR27_C_LFPW_20250103224500.bufr.gz'
    df["date"] = df["name"].str.extract(r"(\d{12})")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d%H%M")
    df = df.sort_values(by="date")
    # Filter for files at 0 or 30 minutes past the hour
    df = df[df["date"].dt.minute.isin([0, 30])]
    return df


def main():
    """
    Main function to execute the script logic.
    """
    print(f"Fetching file list from bucket: {BUCKET_NAME}...")
    file_list = get_file_list_from_bucket(BUCKET_NAME)
    print(f"Found {len(file_list)} total files in the bucket.")

    if file_list:
        df = process_file_list(file_list)
        print(f"Filtered down to {len(df)} files.")
        print(f"Saving dataframe to {OUTPUT_FILE}...")
        df.to_parquet(OUTPUT_FILE, index=False)
        print("Done.")
    else:
        print("No files found or error occurred. Exiting.")


if __name__ == "__main__":
    main()
