import requests
import os
import tarfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from io import BytesIO

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
local_file = "C:/Users/jach_/git_Repos/hit-song-classifier/src/million_song.tar.gz"

# download(download_link, local_file)

# Extract the tar file
if not os.path.exists("C:/Users/jach_/git_Repos/hit-song-classifier/src/MillionSongSubset"):
    with tarfile.open(local_file) as tar:
        tar.extractall("C:/Users/jach_/git_Repos/hit-song-classifier/src/")

# Read data from extracted file into PySpark dataframe
spark = SparkSession.builder.appName("MyApp").getOrCreate()
subset_path = "C:/Users/jach_/git_Repos/hit-song-classifier/src/MillionSongSubset"
spark_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").csv(subset_path + "/**/*.csv")

# Preprocess the PySpark dataframe
keep_cols = ['artist_name', 'danceability', 'duration', 'energy', 'loudness', 'release', 'similar_artists', 'year', 'title', 'time_signature', 'tempo', 'song_hotttnesss']
preprocessed_spark_df = spark_df.select([col(c) for c in keep_cols])

# Save the preprocessed dataframe as a CSV file
preprocessed_spark_df.write.format("csv").option("header", "true").save("C:/Users/jach_/git_Repos/hit-song-classifier/src/preprocessed_data.csv")

# Show the first 5 rows of the preprocessed dataframe
preprocessed_spark_df.show(5)