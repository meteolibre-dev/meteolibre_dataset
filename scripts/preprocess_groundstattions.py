import polars as pl
import os
from pyproj import Transformer
import datetime


dir_ = "../data/groundstations_parquet"
output_dir = "../data/groundstations_filter"

# 1. Filter parquet files directly (more efficient than listing all and filtering in Python)
list_parquet = [file for file in os.listdir(dir_) if file.endswith(".parquet")]

# test on H-COMP_19_latest-2024-2025.parquet
# file = "H-COMP_19_latest-2024-2025.parquet"

# data = pl.read_parquet(os.path.join(dir_, file))

# print(data.head())
# exit()



# 2. Define columns and data types upfront for schema definition
columns_taken = [
    "NUM_POSTE",
    "LAT",
    "LON",
    "ALTI",
    "AAAAMMJJHH",
    "RR1",
    "QRR1",
    "FF",
    "QFF",
    "DD",
    "QDD",
    "FXY",
    "QFXY",
    "DXY",
    "QDXY",
    "HXY",
    "QHXY",
    "FXI",
    "QFXI",
    "DXI",
    "QDXI",
    "HXI",
    "QHXI",
    "FXI3S",
    "QFXI3S",
    "HFXI3S",
    "QHFXI3S",
    "T",
    "QT",
    "TN",
    "QTN",
    "HTN",
    "QHTN",
    "TX",
    "QTX",
    "HTX",
    "QHTX",
    "DG",
    "QDG",
    "NOM_USUEL" # Add NOM_USUEL here
]

# Define schema for reading Parquet (performance boost)
schema = {
    "NUM_POSTE": pl.Int64,
    "LAT": pl.Float32,
    "LON": pl.Float32,
    "ALTI": pl.Int64,
    "AAAAMMJJHH": pl.Int64,
    "RR1": pl.Float32,
    "QRR1": pl.Float32,
    "FF": pl.Float32,
    "QFF": pl.Float32,
    "DD": pl.Float32,
    "QDD": pl.Float32,
    "FXY": pl.Float32,
    "QFXY": pl.Float32,
    "DXY": pl.Float32,
    "QDXY": pl.Float32,
    "HXY": pl.Float32,
    "QHXY": pl.Float32,
    "FXI": pl.Float32,
    "QFXI": pl.Float32,
    "DXI": pl.Float32,
    "QDXI": pl.Float32,
    "HXI": pl.Float32,
    "QHXI": pl.Float32,
    "FXI3S": pl.Float32,
    "QFXI3S": pl.Float32,
    "HFXI3S": pl.Float32,
    "QHFXI3S": pl.Float32,
    "T": pl.Float32,
    "QT": pl.Float32,
    "TN": pl.Float32,
    "QTN": pl.Float32,
    "HTN": pl.Float32,
    "QHTN": pl.Float32,
    "TX": pl.Float32,
    "QTX": pl.Float32,
    "HTX": pl.Float32,
    "QHTX": pl.Float32,
    "DG": pl.Float32,
    "QDG": pl.Float32,
    "NOM_USUEL": pl.String  # Assuming NOM_USUEL is a string
}


list_df = []

for file in list_parquet:
    print(file)

    # 3. Read only the required columns with specified schema
    data_parquet = pl.read_parquet(
        os.path.join(dir_, file),
        columns=columns_taken,
    )

    for col in columns_taken:
        data_parquet = data_parquet.with_columns(pl.col(col).cast(schema[col]))


    # 4. Filter early
    data_parquet = data_parquet.filter(pl.col("AAAAMMJJHH") >= 2025010100)

    # 6. Write filtered data immediately.  Use streaming for large files.
    output_file = os.path.join(output_dir, file)
    data_parquet.write_parquet(output_file,  use_pyarrow=True,  row_group_size=50000)  #Streaming


    list_df.append(data_parquet)  # Collect for final concatenation.  Consider skipping if memory is tight.



# 7. Concatenate outside the loop (more efficient)
df_total = pl.concat(list_df)

print("save parquet")
df_total.write_parquet(os.path.join(output_dir, "total.parquet"), use_pyarrow=True, row_group_size=50000)

# delete the list_df and df_total from memory
del list_df
del df_total

# read the total.parquet file
df = pl.read_parquet("../data/groundstations_filter/total.parquet")

print("shape of the dataframe: ", df.shape)
print(df.head())

# create the datetime column
# add 00 at the end
df = df.with_columns(pl.concat_str([pl.col("AAAAMMJJHH"), pl.lit("00")]).alias("AAAAMMJJHH"))

# write the dataframe to a parquet file
df.write_parquet("../data/groundstations_filter/total.parquet", use_pyarrow=True, row_group_size=50000)
# read the total.parquet file


df = df.with_columns(
    pl.col("AAAAMMJJHH").str.strptime(pl.Date, format="%Y%m%d%H%M").alias("datetime")
)

EPSG = "32630"

# now we want to apply the transformer to the positions of the stations
transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{EPSG}")

# Extract LAT and LON as numpy arrays for the transformer
lat_array = df["LAT"].to_numpy()
lon_array = df["LON"].to_numpy()

print("start transformer")

positions_stations_transformed = transformer.transform(lat_array, lon_array)

print("end transformer")

# create two new columns in the dataframe
df = df.with_columns(
    pl.Series(name="LAT_transformed", values=positions_stations_transformed[0]),
    pl.Series(name="LON_transformed", values=positions_stations_transformed[1]),
)

# get the coordinates of the center of the map
center_map_x, center_map_y = transformer.transform(45.9, 3.2)

# now put the coordinates of the stations in the dataframe
df = df.with_columns(
    ((pl.col("LAT_transformed") - center_map_x) // 500 + 3472 // 2).alias("position_x"),
    (-(pl.col("LON_transformed") - center_map_y) // 500 + 3472 // 2).alias(
        "position_y"
    ),
)

print(df.select(["position_x", "position_y", "LAT", "LON"]).head())

# filter element not in [0, 3472]
df = df.filter(
    (pl.col("position_x") >= 0)
    & (pl.col("position_x") <= 3472)
    & (pl.col("position_y") >= 0)
    & (pl.col("position_y") <= 3472)
)

print( "end filter")

# save it somewhere
df.write_parquet("../data/groundstations_filter/total_transformed.parquet")
