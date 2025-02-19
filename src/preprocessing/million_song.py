import requests
import os
import tarfile
import dask.dataframe as dd
import glob

SUBSET_URL = "http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz"


def download(url: str) -> None:
    """Download file from url

    Args:
        url (str): url to download file from
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        filename = os.path.basename(url)
        filepath = f"data/{filename}"
        with open(filepath, "wb") as f:
            f.write(response.raw.read())


def extract_file(filepath: str) -> None:
    """Extract file from tar.gz"""
    print(f"Extracting {filepath}...")
    tar = tarfile.open(filepath, "r:gz")
    tar.extractall(path="data")
    tar.close()


def process(df: dd, columns_to_keep: list[str]) -> dd:
    """The filtered Dask DataFrame with specified columns and no NaN values."""
    df_filtered = df.loc[:, columns_to_keep]
    df_filtered = df_filtered.dropna(how="any")
    return df_filtered


def main():
    if not os.path.exists("data"):
        os.mkdir("data")
    if not os.path.exists("data/millionsongsubset.tar.gz"):
        download(SUBSET_URL)
    if not os.path.exists("data/MillionSongSubset"):
        extract_file("data/millionsongsubset.tar.gz")
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
    h5_files = list(glob.glob("../data/**/**/*.h5", recursive=True))
    analysis_songs = dd.read_hdf(h5_files, key="analysis/songs")
    meta_songs = dd.read_hdf(h5_files, key="metadata/songs")
    music_songs = dd.read_hdf(h5_files, key="musicbrainz/songs")

    merged = dd.concat([analysis_songs, meta_songs, music_songs], axis=1)
    merged = merged.repartition(npartitions=10)
    filtered = process(merged, features)

    filtered.to_csv("./data/million_songs.csv", index=False, single_file=True)


if __name__ == "__main__":
    main()
