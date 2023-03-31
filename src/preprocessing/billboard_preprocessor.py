import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, floor
from pyspark.sql.types import IntegerType, DateType

"""
Billboard 100 dataset kaggle info.
This dataset includes the hot 100 songs from 1958 to 2021
DOI citation:
    Dhruvil Dave. (2021). <i>Billboard "The Hot 100" Songs</i>
    [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DS/1211465
"""
kaggle_path = "dhruvildave/billboard-the-hot-100-songs"
kaggle_filename = "charts.csv"

# Paths
root_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", ".."))
temp_dir = os.path.join(root_dir, "data", "temp", "")


def download_kaggle_dataset(k_path: str, k_filename: str, directory: str) -> None:
    """kaggle API fetch and extract dataset"""
    api = KaggleApi()
    api.authenticate()

    print(f"Downloading {k_filename} from the Kaggle API at {k_path}.")
    api.dataset_download_file(k_path, file_name=k_filename, path=directory)
    zip_path = f"{os.path.join(directory, k_filename)}.zip"
    with ZipFile(zip_path, "r") as zip_object:
        zip_object.extractall(directory)


def spark_init() -> None:
    """initialize spark session"""
    spark = SparkSession.builder.appName("Hot-Song-classifier").getOrCreate()
    return spark


def process_csv(filename: str, year_limit: str) -> DataFrame:
    """Process the downloaded csv data"""
    spark = spark_init()

    df = spark.read.csv(filename, header=True, mode="DROPMALFORMED")
    df = df.withColumn("rank", df["rank"].cast(IntegerType()))
    df = df.withColumn("date", df["date"].cast(DateType()))
    max_rank_df = (
        df.where(col("date") < year_limit)
        .groupBy("song", "artist")
        .agg(min("rank").alias("top_rank"))
    )
    return max_rank_df


def classify_rankings(df: DataFrame) -> DataFrame:
    """classify top rankings into buckets ranging from 0 to 9"""
    classified_df = df.withColumn("class_f", ((df.top_rank - 1) / 10))
    classified_df = (
        classified_df.select("*", floor(col("class_f")).alias("class"))
        .select([col(c) for c in ["song", "artist", "class"]])
        .orderBy(col("song"))
    )
    return classified_df


def remove_directory(directory_path: str) -> None:
    """removes a directory and all content"""
    print(f"removing directory {directory_path}")
    shutil.rmtree(directory_path)


def write_csv(df: DataFrame, write_path: str, output_filename: str) -> None:
    """writes a dataframe to a csv file at the given path"""
    df.write.format("csv").mode("overwrite").option("header", "true").save(write_path)

    # rename file
    count = 1
    for file in os.listdir(write_path):
        if file.endswith(".csv"):
            filename = f"{output_filename}{count}.csv"
            path = os.path.join(root_dir, "data", filename)
            os.rename(os.path.join(write_path, file), path)
            count += 1
    # remove write_path directory and all contained files
    remove_directory(write_path)


download_kaggle_dataset(kaggle_path, kaggle_filename, temp_dir)

"""year_limit passed is 2011 or 2023 depending on the dataset required."""
processed_data = process_csv(os.path.join(temp_dir, kaggle_filename), "2023")
classified_rankings = classify_rankings(processed_data)

write_path = os.path.join(root_dir, "data", "classified_songs")

"""
output_filename is either "classified_billboard_songs"or "all_classified_billboard_songs"
depending on processed dataset.
"""
write_csv(classified_rankings, write_path, "all_classified_billboard_songs")

remove_directory(temp_dir)
