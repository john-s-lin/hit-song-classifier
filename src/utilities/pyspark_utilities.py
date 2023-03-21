from pyspark.sql import SparkSession, DataFrame
import os
import shutil


def init_spark(app_name: str):
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()
    return spark


def write_csv(df: DataFrame, write_path: str):
    df.write.format("csv") \
        .mode('overwrite') \
        .option("header", "true") \
        .save(write_path)


def rename_csv(old_dir: str, new_dir, filename_prefix: str):
    # rename file
    count = 1
    for file in os.listdir(old_dir):
        if file.endswith(".csv"):
            # Create "new" file name, using the count to make files unique
            filename = f'{filename_prefix}{count}.csv'

            # Create complete file paths for the old and new files
            old_file_path = os.path.join(old_dir, file)
            new_file_path = os.path.join(new_dir, filename)

            # If "new" file already exists, remove it
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)

            # Use rename to move "old" file as "new" file
            os.rename(old_file_path, new_file_path)
            count += 1

    # remove old directory and all contained files
    remove_directory(old_dir)


def remove_directory(directory_path: str):
    print(f'removing directory {directory_path}')
    shutil.rmtree(directory_path)
