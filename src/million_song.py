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
# local_file = "C:/Users/jach_/git_Repos/hit-song-classifier/src/million_song.tar.gz"

# download(download_link, local_file)

# # Extract the tar file
# if not os.path.exists("C:/Users/jach_/git_Repos/hit-song-classifier/src/MillionSongSubset"):
#     with tarfile.open(local_file) as tar:
#         tar.extractall("C:/Users/jach_/git_Repos/hit-song-classifier/src/data")


# Define function to get all h5 files in a directory and its subdirectories
def get_h5_files(path):
    h5_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".h5"):
                h5_files.append(os.path.join(root, file))
    return h5_files

# Set up Spark session
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Define path to MillionSongSubset directory
subset_path = "C:/Users/jach_/git_Repos/hit-song-classifier/src/data/MillionSongSubset"

# Get list of all h5 files in MillionSongSubset and its subdirectories
h5_files = get_h5_files(subset_path)

# Read all h5 files into a list of Spark dataframes
dfs = []
for h5_file in h5_files:
    df = spark.read.format("com.databricks.spark.avro").load(h5_file)
    dfs.append(df)

# Combine all dataframes into one dataframe
spark_df = dfs[0]
for df in dfs[1:]:
    spark_df = spark_df.union(df)

# Keep only selected features and drop rows with null values in any of the feature columns
selected_cols = ["artist_name", "title", "year", "tempo", "duration", "end_of_fade_in", "key", "loudness", "mode", "start_of_fade_out"]
spark_df = spark_df.select(selected_cols).dropna()

# Save the preprocessed dataframe as a CSV file
spark_df.write.format("csv").option("header", "true").save("C:/Users/jach_/git_Repos/hit-song-classifier/src/preprocessed_data.csv")

# Show the first 5 rows of the preprocessed dataframe
spark_df.show(5)


# import requests
# from pyspark.sql import SparkSession

# # Set up Spark session
# spark = SparkSession.builder.appName("Read MSD Subset Data").getOrCreate()

# # Set URL for subset_msd_summary_file.csv
# subset_url = "http://static.echonest.com/millionsongsubset_full/salami_subset/summary/salami_subset_msd_summary_file.csv"

# # Stream data from URL using requests library
# response = requests.get(subset_url, stream=True)

# # Read data into PySpark DataFrame
# spark_df = spark.read.option("header", "true").option("inferSchema", "true").csv(response.raw)

# # keep only required features
# subset_df = spark_df.select("year", "duration", "tempo", "loudness", "key", "mode", "end_of_fade_in", "start_of_fade_out", "energy")
# subset_df.show(5)
