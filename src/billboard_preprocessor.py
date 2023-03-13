import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, floor
from pyspark.sql.types import IntegerType, DateType

'''
Billboard 100 dataset kaggle info.
This dataset includes the hot 100 songs from 1958 to 2021
DOI citation:
    Dhruvil Dave. (2021). <i>Billboard "The Hot 100" Songs</i>
    [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DS/1211465
'''
kaggle_path = 'dhruvildave/billboard-the-hot-100-songs'
kaggle_filename = 'charts.csv'

# Paths
root_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
temp_dir = os.path.join(root_dir, 'data', 'temp', '')


# kaggle API fetch and extract dataset
def download_kaggle_dataset(k_path: str, k_filename: str, directory: str):
    api = KaggleApi()
    api.authenticate()

    print(f'Downloading {k_filename} from the Kaggle API at {k_path}.')
    api.dataset_download_file(k_path, file_name=k_filename, path=directory)
    zip_path = f'{os.path.join(directory, k_filename)}.zip'
    with ZipFile(zip_path, 'r') as zip_object:
        zip_object.extractall(directory)


# process the downloaded csv data, taking the top ranking for all years before 2011.
def process_csv(filename: str) -> DataFrame:
    spark = (
        SparkSession.builder
        .appName('Hot-Song-classifier')
        .getOrCreate()
    )
    
    df = spark.read.csv(filename, header=True, mode='DROPMALFORMED')
    df = df.withColumn('rank', df['rank'].cast(IntegerType()))
    df = df.withColumn('date', df['date'].cast(DateType()))
    max_rank_df = (
        df.where(col('date') < '2011')
        .groupBy('song', 'artist')
        .agg(min('rank').alias('top_rank'))
    )
    return max_rank_df


# classify top rankings into buckets ranging from 0 to 9
def classify_rankings(df: DataFrame) -> DataFrame:
    classified_df = df.withColumn('class_f', ((df.top_rank-1)/10))
    classified_df = (
        classified_df.select('*', floor(col('class_f')).alias('class'))
        .select([col(c) for c in ['song', 'artist', 'class']])
    )
    return classified_df


# removes a directory and all content
def remove_directory(directory_path):
    print(f'removing directory {directory_path}')
    shutil.rmtree(directory_path)


# writes a dataframe to a csv file at the given path
def write_csv(df: DataFrame, write_path: str):
    df.write.format("csv") \
    .mode('overwrite') \
    .option("header", "true") \
    .save(write_path)

    # rename file
    count = 1
    for file in os.listdir(write_path):
        if file.endswith(".csv"):
            path = os.path.join(root_dir, 'data', f'charts{count}.csv')
            os.rename(os.path.join(write_path, file), path)
            count += 1
    # remove write_path directory and all contained files
    remove_directory(write_path)


download_kaggle_dataset(kaggle_path, kaggle_filename, temp_dir)

processed_data = process_csv(os.path.join(temp_dir, kaggle_filename))
classified_rankings = classify_rankings(processed_data)

write_path = os.path.join(root_dir, 'data', 'classified_songs')
write_csv(classified_rankings, write_path)

remove_directory(temp_dir)