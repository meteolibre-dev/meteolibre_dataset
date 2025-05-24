"""
Module to visualize the dataset created
"""
import os
import io
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

import imageio


import json

# load the index data from json
json_path = "../data/hf_dataset/index.json"
df_from_string = pd.read_json(json_path, orient='columns', lines=True)

print(df_from_string.columns)

# we read the 4 element:
# radar_file_path_future
# radar_file_path_back
# groundstation_file_path_future
# groundstation_file_path_back
# ground_height_file_path
index= 5
radar_file_path_future = df_from_string['radar_file_path_future'].values[index]
radar_file_path_back = df_from_string['radar_file_path_back'].values[index]
groundstation_file_path_future = df_from_string['groundstation_file_path_future'].values[index]
groundstation_file_path_back = df_from_string['groundstation_file_path_back'].values[index]
ground_height_file_path = df_from_string['ground_height_file_path'].values[index]

# load the npz files
def load_npz_file(file_path):
    try:
        with np.load(file_path) as data:
            # Assuming the data is stored under the key 'arr_0'
            return data['arr_0']
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None
# Load the data
radar_future = load_npz_file(radar_file_path_future)
radar_back = load_npz_file(radar_file_path_back)
groundstation_future = load_npz_file(groundstation_file_path_future)
groundstation_back = load_npz_file(groundstation_file_path_back)
ground_height = load_npz_file(ground_height_file_path)

# Check if the data is loaded correctly
if radar_future is None or radar_back is None or groundstation_future is None or groundstation_back is None or ground_height is None:
    print("Error loading one or more files.")

# now we save the images into a folder
output_folder = "../images"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# radar data are of shape (nb_time_step, 256, 256)
# groundstation data are of shape (nb_time_step, 256, 256, 4)
# ground height data are of shape (256, 256)
# we want to save a gif for the radar and groundstation data
# and a png for the ground height data

# Function to save the images
def save_image(data, title, filename):
    plt.figure(figsize=(10, 6))
    plt.imshow(data, cmap='viridis', aspect='auto', origin='lower')
    plt.colorbar(label='Intensity')
    plt.title(title)
    plt.xlabel('X-axis')
    plt.ylabel('Y-axis')
    plt.savefig(os.path.join(output_folder, filename))
    plt.close()

# Function to save the gif
def save_gif(data, title, filename):
    fig, ax = plt.subplots(figsize=(10, 6))
    images = []
    for i in range(data.shape[0]):
        plt.figure(figsize=(10, 10))
        im = plt.imshow(data[i], cmap='viridis', aspect='auto', origin='lower')
        plt.colorbar(im, label='Intensity')
        plt.title(f"{title} - Time step {i+1}")
        print(data[i].shape)

        plt.xlabel('X-axis')
        plt.ylabel('Y-axis')
        plt.savefig(os.path.join(output_folder, f"{title}_step_{i+1}.png"))
        plt.close(fig)

        image = imageio.imread(os.path.join(output_folder, f"{title}_step_{i+1}.png"))
        images.append(image)
    
    # Save the gif
    file = os.path.join(output_folder, filename)
    imageio.mimsave(file, images, duration=0.5)


# Save the radar data as a gif
save_gif(radar_future, "Radar Future Data", "radar_future.gif")
save_gif(radar_back, "Radar Back Data", "radar_back.gif")

# Save the groundstation data as a gif
save_gif(groundstation_future, "Groundstation Future Data", "groundstation_future.gif")
save_gif(groundstation_back, "Groundstation Back Data", "groundstation_back.gif")

# Save the ground height data as a png
save_image(ground_height, "Ground Height Data", "ground_height.png")