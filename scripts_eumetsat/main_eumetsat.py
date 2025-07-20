"""
Main script to download, preprocess, and upload EUMETSAT data for a given date range.
"""
import datetime
import os
import shutil
import pandas as pd
from download_eumetsat import download_and_process_eumetsat_data
from preprocess_eumetsat import preprocess_eumetsat_file


def main():
    """
    Main function to orchestrate the EUMETSAT data processing pipeline.
    """
    start_date = datetime.datetime(2025, 1, 17, 4)
    end_date = datetime.datetime(2025, 7, 31)
    bounding_box = '-10, 39, 12, 52'
    gcp_bucket_name = 'eumetsat_preprocess' # Please replace with your bucket name

    current_date = start_date
    while current_date <= end_date:
        # Process one week at a time
        week_start = current_date
        week_end = current_date + pd.DateOffset(weeks=1) - pd.DateOffset(days=1)
        if week_end > end_date:
            week_end = end_date

        print(f"Processing data from {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

        download_and_process_eumetsat_data(week_start, week_end, bounding_box, gcp_bucket_name)

        current_date += pd.DateOffset(weeks=1)

    print("All processing complete!")

if __name__ == "__main__":
    main()
