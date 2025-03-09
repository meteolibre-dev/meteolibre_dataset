"""
In this python module , we create an index (parquet file)
We simply read the list of files in data/h5 and create an index from it.
"""

import os

import pandas as pd

list_files = os.listdir("../data/h5")

# remove non h5 files
list_files = [f for f in list_files if f.endswith(".h5")]

df = pd.DataFrame(list_files, columns=["file_path"])
df["file_path_h5"] = "h5/" + df["file_path"]

print(df.head())

# data is of form T_IPRN20_C_LFPW_20250125033000.h5
# we want to extract the date from the filename
df["date"] = df["file_path"].str.extract(r"(\d{12})")

# now we want to sort the dataframe by date
df = df.sort_values(by="date")

# preprocess the date to get datetime format
df["datetime"] = pd.to_datetime(df["date"], format="%Y%m%d%H%M", errors="raise")

# now we want to keep on date that have a frequency of 1 hour or 30 minutes
df = df[df["datetime"].dt.minute.isin([0, 30])]

# print the number of rows
print(f"Number of rows: {len(df)}")

# now we want to save the dataframe into a parquet file
df.to_parquet("../data/index.parquet", index=False)


