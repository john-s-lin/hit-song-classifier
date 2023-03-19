import requests
import os
import tarfile
import h5py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import dask.dataframe as dd
import glob


#to do: make a data directory in the route directory and download the data there
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
        with open(local_file, "wb") as f:
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


dfs = []

# TODO try not to use for loops for this
# you can create a multi df from
# analysis_songs = dd.read_hdf('*/data/MillionSongSubset/**/**/*.h5', 'analysis/songs') # For one set
# meta_songs = dd.read_hdf('*/data/MillionSongSubset/**/**/*.h5', 'metadata/songs') # For another set
# music_songs = dd.read_hdf('*/data/MillionSongSubset/**/**/*.h5', 'musicbrainz/songs') # For another set


import os
import dask.dataframe as dd

# current_dir = os.path.dirname(os.path.abspath(__file__))
# data_dir = os.path.join(current_dir, 'data/MillionSongSubset')

# analysis_songs = dd.read_hdf('*/data/MillionSongSubset/*.h5', 'analysis/songs') # For one set
# meta_songs = dd.read_hdf('*/data/MillionSongSubset/*.h5', 'metadata/songs') # For another set
# music_songs = dd.read_hdf('*/data/MillionSongSubset/*.h5', 'musicbrainz/songs') # For another set





def process(df, columns_to_keep):

    df_filtered = df.dropna(how = 'any')
    df_filtered = df.loc[:, columns_to_keep]
    return df_filtered

# TODO make sure the feature names are correct

# features = ['artist_name', 'title', 'year', 'danceability', 'duration', 'energy', 'key', 'loudness', 'song_hotttness', 'tempo', 'time_signature']

# process_df = process(df, features)

# # write the Dask DataFrame to a CSV file
# process_df.to_csv('million_song.csv', index=False)


def main():
   
    features = [
        "artist_name",
        "title",
        "year",
        "danceability",
        "duration",
        "energy",
        "key",
        "loudness",
        "song_hotttnesss",
        "tempo",
        "time_signature",
    ]
    parent_dir = ".\src"
    h5_files = list(glob.glob(parent_dir + "**/**/*.h5", recursive=True))
    analysis_songs = dd.read_hdf(h5_files, key='analysis/songs')
    meta_songs = dd.read_hdf(h5_files, key='metadata/songs')
    music_songs = dd.read_hdf(h5_files, key='musicbrainz/songs')

   
    merged = dd.concat([analysis_songs, meta_songs, music_songs], axis=1)
    merged = merged.repartition(npartitions=10)
    filtered = process(merged,features)
    print(filtered.head(npartitions=5,n=5))
    filtered.to_csv('million_songs.csv', index=False)
    # print("merged:")
    # print(merged.head(n=10))


if __name__ == "__main__":
    main()
