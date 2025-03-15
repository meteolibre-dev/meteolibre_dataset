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
# for url in data_to_download["url"]:
#     # apply the command :
#     # !wget {url} -O ../data/groundstations/{url.split("/")[-1]}
#     os.system(f"wget {url} -O ../data/groundstations/{url.split('/')[-1]}")


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
        # os.remove(f"../data/groundstations/{file}")
    except Exception as e:
        print(e)


# dir_ = "../data/groundstations_parquet"

# # we list all the parquet file in ../data/groundstations_parquet
# list_parquet = os.listdir(dir_)

# # we take only the parquet file that start with H
# list_parquet = [file for file in list_parquet if ".parquet" in file]

# list_df = []

# for file in list_parquet:
#     print(file)

#     data_parquet = pd.read_parquet(dir_ + "/" + file)
#     describe_info = data_parquet.describe()

#     # we keep only the columns with more that half non NaN values
#     idx_columns = describe_info.loc["count"] > len(data_parquet) / 2
#     columns_taken = idx_columns.index[idx_columns.values]

#     columns_taken = [
#         "NUM_POSTE",
#         "LAT",
#         "LON",
#         "ALTI",
#         "AAAAMMJJHH",
#         "RR1",
#         "QRR1",
#         "FF",
#         "QFF",
#         "DD",
#         "QDD",
#         "FXY",
#         "QFXY",
#         "DXY",
#         "QDXY",
#         "HXY",
#         "QHXY",
#         "FXI",
#         "QFXI",
#         "DXI",
#         "QDXI",
#         "HXI",
#         "QHXI",
#         "FXI3S",
#         "QFXI3S",
#         "HFXI3S",
#         "QHFXI3S",
#         "T",
#         "QT",
#         "TN",
#         "QTN",
#         "HTN",
#         "QHTN",
#         "TX",
#         "QTX",
#         "HTX",
#         "QHTX",
#         "DG",
#         "QDG",
#     ]

#     custom_df = data_parquet[list(columns_taken) + ["NOM_USUEL"]]

#     # save the custom_df
#     custom_df.to_parquet("../data/groundstations_filter/{}".format(file))

#     list_df.append(custom_df)

# df_total = pd.concat(list_df)

# # get proper format for AAAAMMJJHH
# df_total["datetime"] = pd.to_datetime(
#     df_total["AAAAMMJJHH"], format="%Y%m%d%H", errors="raise"
# )

# df_total.index = df_total["datetime"]

# df_total.to_parquet("../data/groundstations_filter/total.parquet")
