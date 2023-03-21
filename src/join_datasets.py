from utilities.pyspark_utilities import write_csv, rename_csv, init_spark


def join_datasets():
    spark = init_spark("join-datasets")

    df1 = spark.read.csv("./data/classified_billboard_songs1.csv",
                         header=True, mode="DROPMALFORMED")

    df2 = spark.read.csv("./data/million_songs.csv", header=True, mode="DROPMALFORMED")
    df2 = df2.withColumnRenamed("title", "song")  \
          .withColumnRenamed("artist_name", "artist") \

    df3 = df1.join(df2, ['song', 'artist'])
    # df3 = df1.join(df2, ['song']).orderBy('song')

    return df3


df_join = join_datasets()
write_csv(df_join, ".\\data\\join_datasets.csv")
rename_csv(".\\data\\join_datasets.csv", ".\\data", "join_datasets")
