"""
In this script we will create the dataset for the HF dataset

We will use the current full dataset (the original one) to create a proper 256x256x(nb_time_channels + nb_measurements + nb_additional_parameters) dataset
This will be then a HF dataset used for training the model (in streaming format possibly)
"""
import os

import numpy as np
import pandas as pd

from datasets import Dataset, Features, Value, Array2D, Array3D, ArrayXD # Choose ArrayXD for flexibility


# --- 0. Prerequisite: load the index parquet dataframe OR CREATE IT ---
index_data = pd.read_parquet(
    "../data/hf_dataset/index.parquet"
) # Load the index data from parquet

# --- 1. Define the generator function ---
# It now takes the index_data as an argument
def npz_data_generator(index_records):
    for record in index_records:
        npz_path = record['file_path']
        scalar_value = record['scalar_value']

        if not os.path.exists(npz_path):
            print(f"Warning: File not found {npz_path}, skipping.")
            continue

        # Load the .npz file and extract the specific array
        try:
            with np.load(npz_path) as npz_file_data:
                if NPZ_ARRAY_KEY not in npz_file_data:
                    raise KeyError(
                        f"Key '{NPZ_ARRAY_KEY}' not found in {npz_path}. "
                        f"Available keys: {list(npz_file_data.keys())}"
                    )
                numpy_array = npz_file_data[NPZ_ARRAY_KEY]
        except Exception as e:
            print(f"Error loading {npz_path}: {e}, skipping.")
            continue

        yield {
            "array_data": numpy_array, # The NumPy array itself
            "label": scalar_value      # The scalar value
        }


# --- 2. Define the features of your dataset (same as before) ---
# For our dummy data: arrays are 2D, first dimension varies, second is fixed at 5
# The dtype is float32.
array_feature_shape = (None, 5)
array_feature_dtype = 'float32' # Must match the dtype of arrays in your .npz files

features = Features({
    "array_data": ArrayXD(dtype=array_feature_dtype), # Or Array2D(dtype=array_feature_dtype, shape=array_feature_shape)
    "label": Value("float32") # Or "float64", "int32", "int64" etc.
})

# --- 3. Create the dataset ---
# Pass the index_data_list to the generator using gen_kwargs
dataset = Dataset.from_generator(
    npz_data_generator,
    features=features,
    gen_kwargs={"index_records": index_data_list} # Pass your index here
)


# --- 4. (Optional) Inspect the dataset ---
print(f"\nDataset object: {dataset}")
if len(dataset) > 0:
    print(f"\nFirst example from the dataset:")
    first_example = dataset[0]
    print(f"Label: {first_example['label']}")
    print(f"Array data type: {type(first_example['array_data'])}")
    print(f"Array data dtype: {first_example['array_data'].dtype}")
    print(f"Array data shape: {first_example['array_data'].shape}")

    if len(dataset) > 1:
        print(f"\nSecond example from the dataset (if exists):")
        second_example = dataset[1]
        print(f"Label: {second_example['label']}")
        print(f"Array data shape: {second_example['array_data'].shape}")
else:
    print("\nDataset is empty. Check your index data and file paths.")


# --- 5. (Optional) Save and load the dataset (Arrow format) ---
# dataset_path = "./my_indexed_npz_arrow_dataset"
# print(f"\nSaving dataset to {dataset_path}...")
# dataset.save_to_disk(dataset_path)
# print("Dataset saved.")

# print("\nLoading dataset from disk...")
# loaded_dataset = Dataset.load_from_disk(dataset_path)
# print("Dataset loaded.")
# print(loaded_dataset)
# if len(loaded_dataset) > 0:
#     print(loaded_dataset[0]['array_data'].shape)

# --- 6. (Optional) Push to Hub ---
# from huggingface_hub import HfApi, HfFolder
# HfFolder.save_token('YOUR_HF_WRITE_TOKEN') # If not already logged in via CLI
# dataset.push_to_hub("your_username/my_indexed_npz_dataset_name")



