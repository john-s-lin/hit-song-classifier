from utilities.pyspark_utilities import write_csv, rename_csv, init_spark
from pyspark.sql.functions import col, when


def join_datasets():
    spark = init_spark("join-datasets")

    # Put Billboard CSV in dataframe
    billboard_df = spark.read.csv("./data/classified_billboard_songs1.csv",
                                  header=True, mode="DROPMALFORMED")

    # Put Million Song CSV in dataframe.  Rename common columns for join
    million_song_df = spark.read.csv("./data/million_songs.csv", header=True, mode="DROPMALFORMED")
    million_song_df = million_song_df.withColumnRenamed("title", "song") \
        .withColumnRenamed("artist_name", "artist")

    # Perform the join on the "right" side to get everything of Million Songs along with the joined info
    join_df = billboard_df.join(million_song_df, ['song', 'artist'], 'right').orderBy('song')

    # Replace empty "class" record with a class label of 10
    join_df = join_df.withColumn('class',
                                 when(col('class').isNull(), "10")
                                 .otherwise(col('class')))

    return join_df


df_join = join_datasets()
write_csv(df_join, ".\\data\\join_datasets.csv")
rename_csv(".\\data\\join_datasets.csv", ".\\data", "join_datasets")
