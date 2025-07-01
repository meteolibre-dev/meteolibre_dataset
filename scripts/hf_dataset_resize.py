"""
In this script we will create the dataset for the HF dataset

The goal of this script is to create a dataset from the ground stations data
and the radar data

The original dataset is in different format (h5 for radar and npz for ground stations)
and is big in term of images (3472x3472xnb_channels).
The idea is to create a smaller dataset (256x256) with the same number of channels

"""

import os
from tqdm import tqdm
import random
import datetime
import h5py

import numpy as np
import pandas as pd
import concurrent.futures
import threading
import json

# Create a lock for thread-safe file writing
index_file_lock = threading.Lock()


def max_pool_2x2(frames):
    """
    Downsamples frames by a factor of 2 using max pooling.
    Assumes input frames have shape (H, W, T) and so you want to reduce to (H/2, W/2, T).
    """
    H, W, T = frames.shape
    # Ensure dimensions are even for 2x2 pooling
    if H % 2 != 0 or W % 2 != 0:
        # Handle odd dimensions if necessary, e.g., by padding or cropping
        # For now, assuming even dimensions based on typical use cases
        raise ValueError("Input frame dimensions must be even for 2x2 max pooling.")

    # Reshape to group 2x2 blocks and apply max pooling
    # (H, W, T) -> (H/2, 2, W/2, 2, T) -> max over axes 1 and 3 -> (H/2, W/2, T)
    pooled_frames = frames.reshape(H // 2, 2, W // 2, 2, T).max(axis=(1, 3))

    return pooled_frames

def generate_data_point(
    index_dataframe,
    i,
    nb_back_steps,
    nb_future_steps,
    shape_image,
    ground_height_image,
):
    """
    Generate a data point for the HF dataset.

    Args:
        index (pd.DataFrame): The index dataframe containing file paths and datetime.
        i (int): The current index for the data point.
        nb_back_steps (int): Number of past steps to consider.
        nb_future_steps (int): Number of future steps to consider.
        shape_image (int): the initial shape of the image.
        ground_height_image (np.ndarray): Ground height image.
        save_hf_dataset (str): Path to save the HF dataset.

    Returns:
        None
    """
    # get the datetime
    index = int(i + nb_back_steps)

    current_date = index_dataframe.index[index]

    dict_return = {}

    # now for every image, we select only a random 256x256 patch
    x = random.randint(0, shape_image - shape_extrated_image * 2)
    y = random.randint(0, shape_image - shape_extrated_image * 2)

    array_future_list = []
    array_back_list = []

    array_back_groundstation_list = []
    array_future_groundstation_list = []

    array_back_list_time = []

    for future in range(nb_future_steps):
        path_file = os.path.join(
            MAIN_DIR, str(index_dataframe["radar_file_path"].iloc[index + 1 + future])
        )

        # take
        array = np.array(
            h5py.File(path_file, "r")["dataset1"]["data1"]["data"][
                x : (x + shape_extrated_image * 2) : 2,
                y : (y + shape_extrated_image * 2) : 2,
            ]
        )

        array = array.astype(np.int32)

        array[array == 65535] = DEFAULT_VALUE

        # if there is nothing > 0, we go on the next item
        if np.sum(array > 0.5) <= 10:
            # print("not enaught good point")
            return None

        array = np.float32(array) / RADAR_NORMALIZATION  # normalization

        array_future_list.append(array)

        # dict_return["radar_mask_future_" + str(future)] = array != (
        #     DEFAULT_VALUE / RADAR_NORMALIZATION
        # )

        array_ground_station = np.load(
            os.path.join(
                MAIN_DIR,
                str(index_dataframe["groundstation_file_path"].iloc[index + future]),
            )
        )["image"]

        # maxpool
        array_ground_station = array_ground_station[
            x : (x + shape_extrated_image * 2),
            y : (y + shape_extrated_image * 2),
            :
        ]

        array_ground_station = max_pool_2x2(array_ground_station)

        array_future_groundstation_list.append(array_ground_station)

    for back in range(-nb_back_steps, 1):
        path_file = os.path.join(
            MAIN_DIR,
            str(index_dataframe["radar_file_path"].iloc[index + back]),
        )

        # check if the delta with the current time is not too high
        time_back = index_dataframe.index[index + back]
        delta_time = current_date - time_back

        # convert delta time in minutes
        delta_time_minutes = delta_time.total_seconds() / 60

        if delta_time < datetime.timedelta(hours=(nb_back_steps//2 + 1)):
            array = np.array(
                h5py.File(path_file, "r")["dataset1"]["data1"]["data"][
                    x : (x + shape_extrated_image * 2) : 2,
                    y : (y + shape_extrated_image * 2) : 2,
                ]
            )
            array = array.astype(np.int32)
            array[array == 65535] = DEFAULT_VALUE

        else:
            # print("bad delta time", delta_time)
            array = (
                np.ones((shape_extrated_image, shape_extrated_image), dtype=np.float32)
                * DEFAULT_VALUE
            )

        array = np.float32(array) / RADAR_NORMALIZATION  # normalization

        # dict_return["radar_back_" + str(back)] = array
        array_back_list.append(array)
        array_back_list_time.append(delta_time_minutes / 60.0)

        ## groundstation setup
        array_ground_station = np.load(
            os.path.join(
                MAIN_DIR,
                str(index_dataframe["groundstation_file_path"].iloc[index + back]),
            )
        )["image"]

        array_ground_station = array_ground_station[
            x : (x + shape_extrated_image * 2),
            y : (y + shape_extrated_image * 2),
            :
        ]

        array_ground_station = max_pool_2x2(array_ground_station)

        array_back_groundstation_list.append(array_ground_station)

    dict_return["hour"] = np.int32(current_date.hour)
    dict_return["minute"] = np.int32(current_date.minute)

    # dd ground height image
    dict_return["ground_height_image"] = ground_height_image[
        x : (x + shape_extrated_image * 2) : 2, y : (y + shape_extrated_image * 2) : 2
    ]

    dict_return["radar_future"] = np.stack(array_future_list, axis=0)
    dict_return["radar_back"] = np.stack(array_back_list, axis=0)

    dict_return["time_radar_back"] = np.array(array_back_list_time, dtype=np.float32)

    dict_return["groundstation_future"] = np.stack(
        array_future_groundstation_list, axis=0
    )
    dict_return["groundstation_back"] = np.stack(array_back_groundstation_list, axis=0)

    # save the image and save an index
    return dict_return


def save_image(dict_return, save_hf_dataset, data_datetime_str, lock):
    """
    Save the image and update the index.

    Args:
        dict_return (dict): The dictionary containing the data point.
        save_hf_dataset (str): Path to save the HF dataset.
        data_datetime_str (str): The datetime string for the data point.
        lock (threading.Lock): The lock for thread-safe file writing.

    Returns:
        None
    """
    random_id = str(random.randint(0, 1000000000))  # generate a random id for the image

    def get_file_name(key, random_id, hour):
        return os.path.join(
            save_hf_dataset,
            key,
            str(key)
            + "_"
            + str(hour)
            + f"_"
            + random_id
            + ".npz",  # include random_id in the filename
        )

    hour = dict_return["hour"].item()

    for key in dict_return.keys():
        # if key in radar_future_* or key in radar_back_* we save the value somewhere
        if (
            key.startswith("radar_future")
            or key.startswith("radar_back")
            or key.startswith("groundstation_future")
            or key.startswith("groundstation_back")
            or key.startswith("ground_height_image")
        ):

            file_name = get_file_name(key, random_id, hour)
            # Ensure directory exists before saving
            os.makedirs(os.path.dirname(file_name), exist_ok=True)

            np.savez_compressed(file_name, dict_return[key])

    dict_data = {
            "radar_file_path_future": get_file_name("radar_future", random_id, hour),
            "radar_file_path_back": get_file_name("radar_back", random_id, hour),
            "groundstation_file_path_future": get_file_name(
                "groundstation_future", random_id, hour
            ),
            "groundstation_file_path_back": get_file_name("groundstation_back", random_id, hour),
            "ground_height_file_path": get_file_name("ground_height_image", random_id, hour),
            "hour": dict_return["hour"].item(),
            "minute": dict_return["minute"].item(),
            "time_radar_back": dict_return["time_radar_back"].tolist(),
            "datetime": data_datetime_str,
            "id": random_id,
        }

    # we want to append the dict_data to a json file
    # Use the lock for thread-safe writing
    with lock:
        with open(os.path.join(save_hf_dataset, "index.json"), "a") as f:
            
            json.dump(dict_data, f)
            f.write('\n')

# New worker function
def process_index(i, index_dataframe, nb_back_steps, nb_future_steps, shape_image, ground_height_image, save_hf_dataset, lock):
    """
    Processes a single index to generate and save a data point.
    """
    # Get the datetime for this data point
    data_datetime = index_dataframe.index[int(i + nb_back_steps)]
    data_datetime_str = data_datetime.strftime("%Y-%m-%d %H:%M:%S")

    dict_result = generate_data_point(
        index_dataframe,
        i,
        nb_back_steps,
        nb_future_steps,
        shape_image,
        ground_height_image,
    )

    if dict_result is not None:
        # Pass the lock and datetime string to save_image
        save_image(dict_result, save_hf_dataset, data_datetime_str, lock)


# --- 0. Prerequisite: load main variable ---
MAIN_DIR = "../data/"
ground_height_image = MAIN_DIR + "assets/reprojected_gebco_32630_500m_padded.npy"
index_file = MAIN_DIR + "index.parquet"
save_hf_dataset = "../data/hf_dataset/"

NB_BACK_STEPS = 5
NB_FUTURE_STEPS = 4
NB_PASS_PER_IMAGES = 8
RADAR_NORMALIZATION = 60.0
DEFAULT_VALUE = -1

shape_image = 3472
shape_extrated_image = 256

# we read the index file
index = pd.read_parquet(index_file)

# sort by datetime
index = index.sort_values(by="datetime")

# set datetime as index
index = index.set_index("datetime")

nb_back_steps = NB_BACK_STEPS
nb_future_steps = NB_FUTURE_STEPS
shape_image = shape_image

# manage ground height image (read the .npy file)
ground_height_image = np.load(ground_height_image)
ground_height_image = (ground_height_image - np.mean(ground_height_image)) / np.std(
    ground_height_image
)

# loop over the index and create the dataset
len_total = len(index) - nb_back_steps - nb_future_steps

# Create necessary directories for saving files
os.makedirs(save_hf_dataset, exist_ok=True)
# Create subdirectories for different data types
for data_type in ["radar_future", "radar_back", "groundstation_future", "groundstation_back", "ground_height_image"]:
    os.makedirs(os.path.join(save_hf_dataset, data_type), exist_ok=True)

# Initialize the index.json file (do not overwrite if exists)
if not os.path.exists(os.path.join(save_hf_dataset, "index.json")):
    with open(os.path.join(save_hf_dataset, "index.json"), "w") as f:
        pass # Just create an empty file or write a header if needed

# Use ThreadPoolExecutor for parallel processing
# Determine the number of workers, e.g., number of CPU cores
num_workers = 8 # Use number of cores, default to 4 if not available
print(f"Using {num_workers} worker threads.")

# create a random permutation of range(len_total)
index_permutation = np.random.permutation(range(len_total))

with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    futures = []
    # Use tqdm here to track the submission of tasks
    for _ in range(NB_PASS_PER_IMAGES):
        for i in tqdm(range(len_total), desc="Submitting tasks"):
            
            # shuffle the index ()
            new_i = index_permutation[i]

            # Submit the worker function to the executor
            future = executor.submit(
                process_index,
                new_i,
                index, # Pass index_dataframe
                nb_back_steps,
                nb_future_steps,
                shape_image,
                ground_height_image,
                save_hf_dataset,
                index_file_lock # Pass the lock
            )
            futures.append(future)

    # The executor context manager waits for all futures to complete automatically.
print("Dataset generation complete.")
