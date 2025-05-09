import polars as pl
import numpy as np
import os
from tqdm import tqdm

columns_measurements = [
    "RR1",
    "FF",
    "DD",
    "T",
]

columns_positions = ["position_x", "position_y"]
groundstations_info_path = "../data/groundstations_filter/total_transformed.parquet"
dir_h5 = "../data/h5/"
dir_npz_preprocess = "../data/groundstation_npz/"


def process_h5_files_and_create_npz(
    dir_h5: str,
    groundstations_info_path: str,
    dir_npz_preprocess: str,
    columns_measurements: list,
    columns_positions: list,
):
    """
    Processes HDF5 files containing ground station data, transforms the data into
    image format, and saves it as NPZ files.

    This function iterates through HDF5 files in the specified directory, extracts
    date information from filenames, filters data based on time frequency,
    transforms ground station data for each timestamp into a 2D image-like format,
    and saves each timestamp's data as a compressed NPZ file.

    Args:
        dir_h5 (str): Directory containing HDF5 files.
        groundstations_info_path (str): Path to the Parquet file containing ground station information.
        dir_npz_preprocess (str): Directory to save the preprocessed NPZ files.
        columns_measurements (list): List of measurement columns to be extracted from ground station data.
        columns_positions (list): List of position columns ('position_x', 'position_y').
    """

    ############# First get list of h5 files #############
    list_files = os.listdir(dir_h5)

    # remove non h5 files
    list_files = [f for f in list_files if f.endswith(".h5")]

    df_files = pl.DataFrame({"file_path": list_files})
    df_files = df_files.with_columns(
        (pl.lit("h5/") + pl.col("file_path")).alias("file_path_h5")
    )

    print("First few rows of H5 file list:")
    print(df_files.head())

    # data is of form T_IPRN20_C_LFPW_20250125033000.h5
    # we want to extract the date from the filename
    df_files = df_files.with_columns(
        pl.col("file_path").str.extract(r"(\d{12})").alias("date")
    )

    # now we want to sort the dataframe by date
    df_files = df_files.sort(by="date")

    # preprocess the date to get datetime format
    df_files = df_files.with_columns(
        pl.col("date")
        .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
        .alias("datetime")
    )
    # now we want to keep on date that have a frequency of 1 hour or 30 minutes
    df_files = df_files.filter(pl.col("datetime").dt.minute().is_in([0, 30]))

    # print the number of rows
    print(f"Number of H5 files to process: {len(df_files)}")

    # now we sort the dataframe by datetime
    df_files = df_files.sort(by="datetime")

    ####### Second get ground station information ########

    groundstations_info_df = pl.read_parquet(
        groundstations_info_path, columns=columns_measurements + columns_positions + ["datetime"]
    )

    print("\nFirst few rows of ground station info:")
    print(groundstations_info_df.head())

    # get statistics of the dataframe
    print("\nStatistics of ground station info:")
    print(groundstations_info_df.describe())

    def transform_groundstation_data_into_image(df_ground_stations):
        """
        Transforms ground station dataframe into a 2D image-like numpy array.

        Args:
            df_ground_stations (pl.DataFrame): DataFrame containing ground station data
                                                with columns for position_x, position_y,
                                                and measurement columns.

        Returns:
            tuple: A tuple containing:
                - mask (np.ndarray): A boolean mask indicating valid data points in the image.
                - image_result (np.ndarray): A 3D numpy array representing the ground station data in image format (lon, lat, channels).
        """
        lat = np.array(
            df_ground_stations["position_x"].to_numpy(), dtype=np.int64,
        )
        lon = np.array(df_ground_stations["position_y"].to_numpy(), dtype=np.int64)

        nb_channels = len(columns_measurements)
        image_result = (
            np.ones((3472, 3472, nb_channels), dtype=np.float32) * -100
        )

        measurements = df_ground_stations[columns_measurements].to_numpy()

        image_result[lon, lat, :] = np.array(measurements)
        mask = image_result != -100

        return mask, image_result

    ####### Third write npz files for every time stamp ########
    for i in tqdm(range(len(df_files))):
        print(f"Processing H5 file {i+1}/{len(df_files)}")
        datetime = df_files[i, "datetime"]

        file_name_to_write = (
            "ground_stations_" + datetime.strftime("%Y%m%d%H%M") + ".npz"
        )
        full_path_npz = os.path.join(dir_npz_preprocess, file_name_to_write)
        if os.path.exists(full_path_npz):
            print(f"File {file_name_to_write} already exists, skipping")
            continue

        print(f"Processing data for datetime: {datetime}")
        # get ground station information for this time stamp
        df_ground_stations = groundstations_info_df.filter(
            pl.col("datetime") == datetime
        )
        _, image_result = transform_groundstation_data_into_image(
            df_ground_stations
        )

        # save the image result in a npz format
        # NPZ files are more efficient than HDF5 files where handling sparse data
        np.savez_compressed(full_path_npz, image=image_result)

    print("NPZ file creation completed.")


if __name__ == "__main__":
    process_h5_files_and_create_npz(
        dir_h5,
        groundstations_info_path,
        dir_npz_preprocess,
        columns_measurements,
        columns_positions,
    )
