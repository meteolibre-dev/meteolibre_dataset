"""
In this script we will create the dataset for the HF dataset

We will use the current full dataset (the original one) to create a proper 256x256x(nb_time_channels + nb_measurements + nb_additional_parameters) dataset
This will be then a HF dataset used for training the model (in streaming format possibly)
"""
import os

import numpy as np
import pandas as pd

from datasets import Dataset, Features, Value, Array2D, Array3D, Array4D # Choose ArrayXD for flexibility


# --- 0. Prerequisite: load the index json dataframe OR CREATE IT ---
json_path = "../data/hf_dataset/index.json"
index_data = pd.read_json(json_path, orient='columns', lines=True)

print(index_data.head())  # Display the first few records for verification
print(index_data.columns)

# --- 1. Define the generator function ---
# It now takes the index_data as an argument
def npz_data_generator(index_records):

    for record in index_records.iterrows():
        record = record[1]  # Get the actual record from the tuple
        radar_back_file = record['radar_file_path_back']
        radar_back_future = record['radar_file_path_future']
        
        gs_future = record['groundstation_file_path_future']
        gs_back = record['groundstation_file_path_back']
        
        height = record['ground_height_file_path']
        
        hour = record['hour']
        minutes = record['minute']
        time_radar_back = record['time_radar_back']
        datetime = record['datetime']
        id = record['id']

        # Load the all the npz files
        npz_paths = [
            radar_back_file,
            radar_back_future,
            gs_future,
            gs_back,
            height
        ]
        # Check if all files exist
        for npz_path in npz_paths:
            if not os.path.exists(npz_path):
                print(f"File not found: {npz_path}")
                continue
        # Load the npz files
        try:
            radar_back_data = np.load(radar_back_file)['arr_0']  # Assuming the data is stored under 'arr_0'
            radar_future_data = np.load(radar_back_future)['arr_0']
            gs_future_data = np.load(gs_future)['arr_0']
            gs_back_data = np.load(gs_back)['arr_0']
            height_data = np.load(height)['arr_0']
        except Exception as e:
            print(f"Error loading data from {radar_back_file}, {radar_back_future}, {gs_future}, {gs_back}, {height}: {e}")
            continue

        yield {
            "radar_back": radar_back_data, # The NumPy array itself
            "radar_future": radar_future_data, # The NumPy array itself
            "groundstation_future": gs_future_data, # The NumPy array itself
            "groundstation_back": gs_back_data, # The NumPy array itself
            "ground_height": height_data, # The NumPy array itself
            "hour": hour,  # The scalar value
            "minutes": minutes,  # The scalar value
            #"time_radar_back": time_radar_back,  # The scalar value
            "datetime": datetime,  # The scalar value
            "id": id,  # The scalar value
        }


# --- 2. Define the features of your dataset (same as before) ---
# For our dummy data: arrays are 2D, first dimension varies, second is fixed at 5
# The dtype is float32.
array_feature_shape = 256
array_feature_dtype = 'float32' # Must match the dtype of arrays in your .npz files

features = Features({
    "radar_back": Array3D(shape=(5, array_feature_shape, array_feature_shape), dtype=array_feature_dtype),
    "radar_future": Array3D(shape=(4, array_feature_shape, array_feature_shape), dtype=array_feature_dtype),
    "groundstation_future": Array4D(shape=(4, array_feature_shape, array_feature_shape, 4), dtype=array_feature_dtype),
    "groundstation_back": Array4D(shape=(5, array_feature_shape, array_feature_shape, 4), dtype=array_feature_dtype),
    "ground_height": Array2D(shape=(array_feature_shape, array_feature_shape), dtype=array_feature_dtype),  # Assuming height is a 2D array with one column
    "hour": Value(dtype="int32"),
    "minutes": Value(dtype="int32"),
    #"time_radar_back": Array1D(shape=(5,), dtype=array_feature_dtype),  # 1D array with one element, dtype can be float32 or int32 depending on your data
    "datetime": Value(dtype="string"),  # Assuming this is a string representation of the datetime
    "id": Value(dtype="string")  # Assuming this is a unique identifier
})

# --- 3. Create the dataset ---
dataset = Dataset.from_generator(
    npz_data_generator,
    features=features,
    gen_kwargs={"index_records": index_data} # Pass your index here
)


# --- 4. (Optional) Inspect the dataset ---
print(f"\nDataset object: {dataset}")
if len(dataset) > 0:
    print(f"\nFirst example from the dataset:")
    first_example = dataset[0]
    id = first_example['id']
    print(f"ID: {id}")

    if len(dataset) > 1:
        print(f"\nSecond example from the dataset (if exists):")
        second_example = dataset[1]
        second_id = second_example['id']
        print(f"ID: {second_id}")

else:
    print("\nDataset is empty. Check your index data and file paths.")


# --- 5. (Optional) Save and load the dataset (Arrow format) ---
dataset_path = "./meteolibre_hf_dataset"
print(f"\nSaving dataset to {dataset_path}...")
dataset.save_to_disk(dataset_path)
print("Dataset saved.")

print("\nLoading dataset from disk...")
loaded_dataset = Dataset.load_from_disk(dataset_path)
print("Dataset loaded.")
print(loaded_dataset)
if len(loaded_dataset) > 0:
    print(np.array(loaded_dataset[0]['ground_height']))

# --- 6. (Optional) Push to Hub ---
from huggingface_hub import HfApi, HfFolder
dataset.push_to_hub("Forbu14/meteolibre_hf_dataset", private=True, token=HfFolder.get_token())



