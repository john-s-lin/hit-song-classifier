import requests
import os
import tarfile
import h5py
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import dask.dataframe as dd
import glob

# Define the download function
def download(download_url: str, local_file: str) -> str:
    """
    Downloads a file from a given URL to a local file path.
    Returns the local file path.
    """
    if os.path.exists(local_file):
        print(f"{local_file} already exists.")
        return local_file
    else:
        print(f"Downloading {download_url}...")
        response = requests.get(download_url, stream=True)
        with open(local_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return local_file

# Download and extract the subset
# download_link = "http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz"
# local_file = "C:/Users/jach_/git_Repos/hit-song-classifier/src/million_song.tar.gz"

# download(download_link, local_file)

# # Extract the tar file
# if not os.path.exists("C:/Users/jach_/git_Repos/hit-song-classifier/src/MillionSongSubset"):
#     with tarfile.open(local_file) as tar:
#         tar.extractall("C:/Users/jach_/git_Repos/hit-song-classifier/src/data")


spark = SparkSession.builder.appName("MillionSongSubset").getOrCreate()


parent_dir = r"C:\Users\jach_\git_Repos\hit-song-classifier\src"

h5_paths = list(glob.glob(parent_dir + "**/**/*.h5", recursive=True))



file = r"C:\Users\jach_\git_Repos\hit-song-classifier\src\data\MillionSongSubset\A\A\A\TRAAAAW128F429D538.h5"



features = ['artist_name', 'title', 'year', 'danceability', 'duration', 'energy', 'key', 'loudness', 'song_hotttnesss', 'tempo', 'time_signature']

h5_paths = h5_paths[:10]


# dfs = []

# for file_path in h5_paths:
#     with h5py.File(file_path, "r") as f:
#         h5_dfs = []
#         group_names = list(f.keys())  # get all group names in the file
#         for group_name in group_names:
#             if "songs" in f[group_name]:
#                 # Read data from the songs dataset in the current group and append to the list of DataFrames
#                 df = dd.from_array(f[group_name]["songs"][()])
#                 h5_dfs.append(df)

#         # Concatenate all DataFrames into a single Dask DataFrame for this h5 file
#         h5_df = dd.concat(h5_dfs)

#         # Add this h5 file's dataframe to the list of dataframes
#         dfs.append(h5_df)

# # Concatenate all DataFrames from all h5 files into a single Dask DataFrame
# df = dd.concat(dfs)

import dask.dataframe as dd
import pandas as pd
import tables



dfs = []

for h5_path in h5_paths:
    # open the H5 file
    with tables.open_file(h5_path, mode='r') as f:
        # iterate over each group in the H5 file
        for group in f.walk_groups():
            # check if the group has a songs table
            if 'songs' in group:
                # read the songs table into a pandas dataframe
                songs_df = pd.read_hdf(h5_path, key=group._v_pathname+'/songs')
                # create an empty dask dataframe with the correct column names
                columns = list(songs_df.columns)
                df = dd.from_pandas(pd.DataFrame(columns=columns), npartitions=1)
                # concatenate the single row of the songs table to the dask dataframe
                df = dd.concat([df, dd.from_pandas(songs_df, npartitions=1)])
                dfs.append(df)

# concatenate all the dask dataframes into one
final_df = dd.concat(dfs)
# persist the dask dataframe in memory for faster computations
final_df = final_df.persist()

df_filtered = final_df.loc[:, features]
head_df = df_filtered.head(10, npartitions=20)

print(head_df)
print(df_filtered.isnull().sum().compute())





# def process(df, columns_to_keep):
   
#     df_filtered = df.dropna(how = 'any')
#     df_filtered = df.loc[:, columns_to_keep]
#     return df_filtered

# features = ['artist_name', 'title', 'year', 'danceability', 'duration', 'energy', 'key', 'loudness', 'song_hotness', 'tempo', 'time_signature']

# process_df = process(df, features)

# # write the Dask DataFrame to a CSV file
# process_df.to_csv('million_song.csv', index=False)