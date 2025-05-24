"""
### In this notebook we will make the data preprocessing for the MeteoLibre dataset.

1. First the h5 dataset (not a lot to do here the data is already processed and ready to be used).

The list of files already exist nothing to be done in reality

2. Second the ground station dataset

3. The ammping creation to get the correct position in EPSG

4. The two bufr file to be preprocess (and created via the bufr_preprocessing.py script)
"""

import pandas as pd
import os
import datetime

# 2. we first take a look at the data.gouv files to get the url of the historical data
file_ressources = "../data/datagouv/2ad89e9d0b014ad0fc3b605dc69b9d41.parquet"

ressources = pd.read_parquet(file_ressources)
print(ressources.head())

id_datastations = "6569b4473bedf2e7abad3b72"
ressources_meteofrance = ressources[ressources["dataset.id"] == id_datastations]

print(ressources_meteofrance["description"].dropna().values)

# drop false
values = ressources_meteofrance["description"].dropna().str.contains("2024-2025").values
idx = ressources_meteofrance["description"].dropna()[values].index

data_to_download = ressources_meteofrance.loc[idx]

data_to_download[["description", "url"]].head().values

# loop over data_to_download and download the data from dataset.url col
for url in data_to_download["url"]:
    # apply the command :
    # !wget {url} -O ../data/groundstations/{url.split("/")[-1]}
    os.system(f"wget {url} -O ../data/groundstations/{url.split('/')[-1]}")


## now for each files in data/groundstations we unzip the .gz files read it, create a parquet file
list_files = os.listdir("../data/groundstations")

# only .gz file
list_files = [f for f in list_files if f.endswith(".gz")]
#list_files = [f for f in list_files if f.endswith(".csv")]

# we loop over the file and unzip it read it save it into parquet format and delete the .gz file
for file in list_files:
    try:
        print(file)
        # command : !gzip -d ../data/groundstations/{file}
        os.system(f"gzip -d ../data/groundstations/{file}")

        # remove the last .gz
        file = file[:-3]

        # read the csv file
        df = pd.read_csv(f"../data/groundstations/{file}", sep=";")

        # save it into parquet format
        df.to_parquet(f"../data/groundstations_parquet/{file.split('.')[0]}.parquet")

        # delete the .gz file
        os.remove(f"../data/groundstations/{file}")
        
    except Exception as e:
        print(e)

