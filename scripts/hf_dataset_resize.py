"""
In this script we will create the dataset for the HF dataset

The goal of this script is to create a dataset from the ground stations data
and the radar data

The original dataset is in different format (h5 for radar and npz for ground stations)
and is big in term of images (3472x3472xnb_channels).
The idea is to create a smaller dataset (256x256) with the same number of channels

"""
import os
import random
import datetime
import h5py

import numpy as np
import pandas as pd


def generate_data_point(
    index_dataframe,
    i,
    nb_back_steps,
    nb_future_steps,
    shape_image,
    ground_height_image,
    save_hf_dataset
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
    x = random.randint(0, shape_image // 2 - shape_extrated_image)
    y = random.randint(0, shape_image // 2 - shape_extrated_image)

    for future in range(nb_future_steps):
        path_file = os.path.join(
            MAIN_DIR, str(index_dataframe["radar_file_path"].iloc[index + future])
        )

        # take
        array = np.array(
            h5py.File(path_file, "r")["dataset1"]["data1"]["data"][
                x : (x + shape_extrated_image*2) : 2, y : (y + shape_extrated_image*2) : 2
            ]
        )

        array = array.astype(np.int32)

        array[array == 65535] = -DEFAULT_VALUE

        # if there is nothing > 0, we go on the next item
        if np.sum(array > 0.1) <= 10:
            # print("not enaught good point")
            return None

        array = np.float32(array) / RADAR_NORMALIZATION  # normalization

        dict_return["future_" + str(future)] = array
        dict_return["mask_future_" + str(future)] = array != (
            -DEFAULT_VALUE / RADAR_NORMALIZATION
        )

    for back in range(nb_back_steps):
        path_file = os.path.join(
            MAIN_DIR,
            str(index_dataframe["radar_file_path"].iloc[index - back - 1]),
        )
        # check if the delta with the current time is not too high
        time_back = index_dataframe.index[index - back - 1]
        delta_time = current_date - time_back

        # convert delta time in minutes
        delta_time_minutes = delta_time.total_seconds() / 60

        if delta_time <= datetime.timedelta(hours=2):
            array = np.array(
                h5py.File(path_file, "r")["dataset1"]["data1"]["data"][
                    x : (x + shape_extrated_image*2) : 2, y : (y + shape_extrated_image*2) : 2
                ]
            )
            array = array.astype(np.int32)
            array[array == 65535] = -DEFAULT_VALUE

        else:
            # print("bad delta time", delta_time)
            array = np.ones((shape_extrated_image, shape_extrated_image), dtype=np.float32) * -DEFAULT_VALUE

        array = np.float32(array) / RADAR_NORMALIZATION  # normalization

        dict_return["back_" + str(back)] = array
        # dict_return["mask_back_" + str(back)] = array != (-DEFAULT_VALUE / RADAR_NORMALIZATION)
        dict_return["time_back_" + str(back)] = delta_time_minutes / 60.0

    dict_return["hour"] = np.int32(current_date.hour) / 24.0
    dict_return["minute"] = np.int32(current_date.minute) / 30.0

    # dd ground height image
    dict_return["ground_height_image"] = ground_height_image[
        x : (x + shape_extrated_image*2) : 2, y : (y + shape_extrated_image*2) : 2
    ]
    
    # save the image and save an index
    return dict_return

# --- 0. Prerequisite: load main variable ---
MAIN_DIR = "../data/"
ground_height_image = MAIN_DIR + "assets/reprojected_gebco_32630_500m_padded.npy"
index_file = MAIN_DIR + "index.parquet"
save_hf_dataset = "../data/hf_dataset/"

NB_BACK_STEPS = 5
NB_FUTURE_STEPS = 4
NB_PASS_PER_IMAGES = 20
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
ground_height_image = (
    ground_height_image - np.mean(ground_height_image)
) / np.std(ground_height_image)

# loop over the index and create the dataset
len_total = len(index) - nb_back_steps - nb_future_steps

for i in range(len_total):
    for _ in range(NB_PASS_PER_IMAGES):
        dict_result = generate_data_point(
            index,
            i,
            nb_back_steps,
            nb_future_steps,
            shape_image,
            ground_height_image,
            save_hf_dataset
        )
        breakpoint()
        
        # save the image and save an index TODO
