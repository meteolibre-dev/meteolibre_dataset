"""
This script creates an index (parquet file) by reading lists of radar and ground station files.
It processes filenames to extract dates, filters data based on time frequency,
and merges radar and ground station dataframes into a final index.
"""

import os
import pandas as pd

def create_radar_dataframe(h5_dir="../data/h5"):
    """
    Creates a Pandas DataFrame for radar files.

    Args:
        h5_dir (str, optional): Path to the directory containing H5 radar files.
                                 Defaults to "../data/h5".

    Returns:
        pd.DataFrame: DataFrame containing radar file paths and extracted datetime information.
    """
    list_files = os.listdir(h5_dir)
    list_files = [f for f in list_files if f.endswith(".h5")]

    df_radar = pd.DataFrame(list_files, columns=["file_path"])
    df_radar["file_path_h5"] = "h5/" + df_radar["file_path"]
    df_radar["date"] = df_radar["file_path"].str.extract(r"(\d{12})")
    df_radar["datetime"] = pd.to_datetime(df_radar["date"], format="%Y%m%d%H%M", errors="raise")
    df_radar = df_radar[df_radar["datetime"].dt.minute.isin([0, 30])] # filter by frequency

    print(f"Radar DataFrame - First 5 rows:\n{df_radar.head()}")
    print(f"Number of rows in Radar DataFrame: {len(df_radar)}")
    return df_radar

def create_groundstation_dataframe(npz_dir="../data/groundstation_npz"):
    """
    Creates a Pandas DataFrame for ground station files.

    Args:
        npz_dir (str, optional): Path to the directory containing NPZ ground station files.
                                  Defaults to "../data/groundstation_npz".

    Returns:
        pd.DataFrame: DataFrame containing ground station file paths and extracted datetime information.
    """
    list_files = os.listdir(npz_dir)
    list_files = [f for f in list_files if f.endswith(".npz")]

    df_groundstations = pd.DataFrame(list_files, columns=["file_path"])
    df_groundstations["file_path_npz"] = "groundstation_npz/" + df_groundstations["file_path"]
    df_groundstations["date"] = df_groundstations["file_path"].str.extract(r"(\d{12})")
    df_groundstations["datetime"] = pd.to_datetime(df_groundstations["date"], format="%Y%m%d%H%M", errors="raise")

    print(f"Ground Station DataFrame - First 5 rows:\n{df_groundstations.head()}")
    return df_groundstations

def merge_dataframes(df_radar, df_groundstations):
    """
    Merges radar and ground station DataFrames based on datetime.
    Removes redundant columns after merging.

    Args:
        df_radar (pd.DataFrame): DataFrame containing radar data.
        df_groundstations (pd.DataFrame): DataFrame containing ground station data.

    Returns:
        pd.DataFrame: Merged DataFrame with selected columns.
    """
    df_radar_renamed = df_radar[['datetime', 'file_path_h5']].rename(columns={'file_path_h5': 'radar_file_path'})
    df_groundstations_renamed = df_groundstations[['datetime', 'file_path_npz']].rename(columns={'file_path_npz': 'groundstation_file_path'})

    df_merged = pd.merge(df_radar_renamed, df_groundstations_renamed, how="inner", on="datetime")

    print(f"Merged DataFrame - First 5 rows:\n{df_merged.head()}")
    print(f"Length of the Merged DataFrame: {len(df_merged)}")
    return df_merged

def save_dataframe_to_parquet(df, output_path="../data/index.parquet"):
    """
    Saves a Pandas DataFrame to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_path (str, optional): Path to save the parquet file.
                                     Defaults to "../data/index.parquet".
    """
    df.to_parquet(output_path, index=False)
    print(f"DataFrame saved to parquet file: {output_path}")

if __name__ == "__main__":
    df_radar = create_radar_dataframe()
    df_groundstations = create_groundstation_dataframe()
    df_index = merge_dataframes(df_radar, df_groundstations)
    save_dataframe_to_parquet(df_index)
