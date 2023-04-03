import dotenv
import glob
import logging
import os
import pandas as pd
import re
import shutil
import sys
import time

# Set logging level
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from dask import dataframe as dd

# Load environment variables from .env
dotenv.load_dotenv()
SPOTIPY_CLIENT_ID = os.environ.get("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.environ.get("SPOTIPY_CLIENT_SECRET")

# Initialize target datasets
TARGET_FILE = "data/all_classified_billboard_songs1.csv"
TARGET_CLEANED_FILE = TARGET_FILE.split(".csv")[0] + "_clean.csv"
TEMP_DIR = "data/temp"
BB_SUBSET_PREFIX = f"{TEMP_DIR}/bb_subset_clean"
INTERMEDIATE_SUBSET_PREFIX = f"{TEMP_DIR}/bb_subset_id"
SPOTIFY_SUBSET_PREFIX = f"{TEMP_DIR}/spotify_enhanced_dataset"
SPOTIFY_DATASET_FILE = f"data/spotify_enhanced_dataset.csv"
MILLION_SONG_SUBSET_FILE = "data/million_songs_subset.csv"

RANDOM_SEED = 0


class SpotifyDatasetGenerator:
    def __init__(self) -> None:
        """Generate a new Spotify Client"""
        auth_manager = SpotifyClientCredentials(
            client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET
        )
        self.sp = spotipy.Spotify(
            auth_manager=auth_manager, requests_timeout=10, retries=10
        )

    def get_track_id_and_popularity(self, artist: str, title: str) -> tuple[str, int]:
        """Given artist and track title, return Spotify track ID and popularity
        Returns empty string if track does not exist

        Args:
            artist (str): artist name
            title (str): track title

        Returns:
            str: Spotify track ID
        """
        search_query = f"artist:{artist} track:{title}"
        response = self.sp.search(q=search_query, type="track")
        track_id = ""
        popularity = 0
        if len(response["tracks"]["items"]) > 0:
            track_id = response["tracks"]["items"][0]["id"]
            popularity = response["tracks"]["items"][0]["popularity"]
        # If it's a bad query, simplify the query
        else:
            search_query = f"{title} {artist}"
            response = self.sp.search(q=search_query, type="track")
            try:
                track_id = response["tracks"]["items"][0]["id"]
                popularity = response["tracks"]["items"][0]["popularity"]
            except IndexError:
                logging.error(f"No results returned for query:{search_query}")
                logging.error(response)
        return track_id, popularity

    def create_columns_track_id_popularity(self, ddf: dd) -> dd:
        """Creates column 'track_id' and 'popularity' given track name and artist

        Args:
            ddf (dd): _description_

        Returns:
            dd: _description_
        """
        df = ddf.compute()
        track_ids = {}
        song_popularities = {}
        for i, row in df.iterrows():
            track_id, popularity = self.get_track_id_and_popularity(
                row["artist"], row["song"]
            )
            track_ids[i] = track_id
            song_popularities[i] = popularity
            # Sleep to avoid rate limit
            time.sleep(0.5)
        track_ids_series = pd.Series(track_ids, dtype=str, name="track_id")
        song_popularities_series = pd.Series(
            song_popularities, dtype=int, name="popularity"
        )
        track_id_popularity_df = pd.concat(
            [track_ids_series, song_popularities_series], axis=1
        )
        return dd.from_pandas(df.join(track_id_popularity_df), chunksize=1000)

    def drop_rows_if_empty_track_id(self, ddf: dd) -> dd:
        """Drop rows from dask dataframe if track_id is empty
        Does this by selecting rows where track_id is not empty

        Args:
            ddf (dd): dask dataframe

        Returns:
            dd: dask dataframe
        """
        return ddf[(ddf["track_id"] != "")]

    def create_columns_audio_features(self, ddf: dd) -> dd:
        """Creates columns for audio features given track_id

        Args:
            ddf (dd): dask dataframe

        Returns:
            dd: dask dataframe
        """
        df = ddf.compute()
        audio_features = {}
        for i in range(0, len(df), 100):
            track_ids = df.iloc[i : i + 100]["track_id"].tolist()
            audio_features_list = self.sp.audio_features(track_ids)
            # Sleep to avoid rate limit
            time.sleep(0.5)
            for j, track_id in enumerate(track_ids):
                audio_features[track_id] = audio_features_list[j]

        features_list = []
        for id_, features in audio_features.items():
            if features is not None:
                features["id"] = id_
                features_list.append(features)

        audio_features_df = pd.DataFrame(
            features_list,
            columns=[
                "id",
                "danceability",
                "energy",
                "key",
                "loudness",
                "mode",
                "speechiness",
                "acousticness",
                "instrumentalness",
                "liveness",
                "valence",
                "tempo",
                "duration_ms",
                "time_signature",
            ],
        )

        return dd.from_pandas(
            df.merge(audio_features_df, left_on="track_id", right_on="id"), chunksize=1000
        )

    def get_cleaned_billboard_dataset(self, filename: str) -> dd:
        """Initialize classified Billboard dataset

        Args:
            filename (str): filename

        Returns:
            DataFrame: Billboard data with labels
        """
        ddf = dd.read_csv(filename, on_bad_lines="skip", dtype=str)
        return ddf.drop_duplicates(subset=["song", "artist"])


class BillboardDatasetCleaner:
    def __init__(self) -> None:
        pass

    def clean_raw_billboard_dataset(self, filename: str) -> None:
        """Cleans Billboard song names with '\' and '"' punctuation

        Args:
            filename (str): filename
        """
        csv_list = list()
        line_count = 0
        with open(filename, "r") as file:
            line = file.readline()
            while line:
                line_count += 1
                split_line = line.split(",")
                if len(split_line) == 3:
                    # Clean song name
                    split_line[0] = self.__clean_element_name(split_line[0])
                    # Clean artist name
                    split_line[1] = self.__clean_element_name(split_line[1])
                else:
                    # Clean song name
                    split_line[0] = self.__clean_element_name(split_line[:-2])
                    # Clean artist name
                    split_line[-2] = self.__clean_element_name(split_line[-2])
                    split_line = [split_line[0]] + split_line[-2:]
                csv_list.append(",".join(split_line))
                line = file.readline()

        # Add unclassified songs to csv_list
        class_10_songs = self.add_unclassified_songs(
            MILLION_SONG_SUBSET_FILE, line_count // 10
        )
        csv_list += class_10_songs

        output = "".join(csv_list)
        new_filename = filename.split(".csv")[0] + "_clean.csv"

        with open(new_filename, "w", newline="") as csvfile:
            csvfile.write(output)

    def add_unclassified_songs(self, filename: str, subset_size: int = 100) -> list:
        """Returns list of unclassified songs from million song subset as [song, artist, class]

        Args:
            filename (str): filename

        Returns:
            list: list of unclassified songs
        """
        df = pd.read_csv(filename)
        df_subset = df.sample(subset_size, random_state=RANDOM_SEED)
        df_subset["class"] = 10
        songs_list = [
            row
            for row in df_subset[["title", "artist.name", "class"]].values.tolist()
            if len(row) == 3
        ]
        return [",".join(map(str, row)) + "\n" for row in songs_list]

    def __clean_element_name(self, *args) -> str:
        """Cleans malformed CSV element names

        Returns:
            str: cleaned CSV element name
        """
        output = ""
        if isinstance(args[0], str):
            output = re.sub(r"([\\\"])", r"", args[0])
        else:
            output = '"' + re.sub(r"([\\\"])", r"", ",".join(args[0])) + '"'
        return output


def create_cleaned_billboard_dataset():
    """Creates cleaned Billboard dataset"""
    if not os.path.exists(TARGET_CLEANED_FILE):
        logging.info(f"Cleaning {TARGET_FILE}...")
        bb_cleaner = BillboardDatasetCleaner()
        bb_cleaner.clean_raw_billboard_dataset(TARGET_FILE)
    else:
        logging.info(f"{TARGET_CLEANED_FILE} already exists.")


def create_spotify_dataset():
    """Creates Spotify dataset"""
    if not os.path.exists(SPOTIFY_DATASET_FILE):
        logging.info(f"Creating {SPOTIFY_DATASET_FILE}...")

        split_billboard_cleaned_dataset()
        create_spotify_df_with_id_shards()
        create_spotify_df_with_audio_features_shards()
        combine_spotify_dataset_shards()

    else:
        logging.info(f"{SPOTIFY_DATASET_FILE} already exists.")


def split_billboard_cleaned_dataset():
    """Splits Billboard dataset into 1000 row shards"""
    if not os.path.exists(TEMP_DIR):
        os.mkdir(TEMP_DIR)

    # Check if shards exist
    shard_file_glob = f"{BB_SUBSET_PREFIX}_*.csv"
    bb_cleaned_shards = glob.glob(shard_file_glob)
    if len(bb_cleaned_shards) == 0:
        logging.info(f"Shards do not exist. Splitting...")
        df = pd.read_csv(TARGET_CLEANED_FILE, on_bad_lines="skip")
        split_dfs = [df[i : i + 1000] for i in range(0, df.shape[0], 1000)]
        for i, df in enumerate(split_dfs):
            df.to_csv(f"{BB_SUBSET_PREFIX}_{i}.csv", index=False)


def create_spotify_df_with_id_shards():
    """Creates intermediate datasets from shards with Spotify track ID"""
    # Get list of files which match data/bb_subset_clean_*.csv
    bb_subset = glob.glob(f"{BB_SUBSET_PREFIX}_*.csv")
    for i, bb_subset_file in enumerate(bb_subset):
        # Check if intermediate dataset exists
        intermediate_dataset_file = f"{INTERMEDIATE_SUBSET_PREFIX}_{i}.csv"
        if not os.path.exists(intermediate_dataset_file):
            logging.info(f"Creating {intermediate_dataset_file}...")
            sp_generator = SpotifyDatasetGenerator()
            df = sp_generator.get_cleaned_billboard_dataset(bb_subset_file)
            df_with_track_id = sp_generator.drop_rows_if_empty_track_id(
                sp_generator.create_columns_track_id_popularity(df)
            )
            df_with_track_id.to_csv(
                intermediate_dataset_file, single_file=True, index=False
            )
        else:
            logging.info(f"{intermediate_dataset_file} already exists.")


def create_spotify_df_with_audio_features_shards():
    """Creates Spotify dataset from shards with audio features"""
    # Get list of intermediate shards
    intermediate_subset = glob.glob(f"{INTERMEDIATE_SUBSET_PREFIX}_*.csv")
    for i, intermediate_subset_file in enumerate(intermediate_subset):
        # Check if Spotify dataset exists
        spotify_dataset_file = f"{SPOTIFY_SUBSET_PREFIX}_{i}.csv"
        if not os.path.exists(spotify_dataset_file):
            logging.info(f"Creating {spotify_dataset_file}...")
            sp_generator = SpotifyDatasetGenerator()
            df_with_track_id = dd.read_csv(intermediate_subset_file)
            df_with_audio_features = sp_generator.create_columns_audio_features(
                df_with_track_id
            )
            df_with_audio_features.to_csv(
                spotify_dataset_file, single_file=True, index=False
            )
        else:
            logging.info(f"{spotify_dataset_file} already exists.")


def combine_spotify_dataset_shards():
    """Combines Spotify dataset shards into one file"""
    # Check if shards exist
    shard_file_glob = f"{SPOTIFY_SUBSET_PREFIX}_*.csv"
    sp_enhanced_shards = glob.glob(shard_file_glob)
    if len(sp_enhanced_shards) > 0:
        logging.info(f"Shards already exist. Combining...")
        sp_enhanced_df_list = [
            pd.read_csv(file, on_bad_lines="skip") for file in sp_enhanced_shards
        ]
        sp_enhanced_df = pd.concat(sp_enhanced_df_list, axis=0, ignore_index=True)
        sp_enhanced_df.to_csv(SPOTIFY_DATASET_FILE, index=False)


def _delete_temp_files():
    """Deletes temporary files"""
    logging.info("Deleting temporary files...")
    shutil.rmtree(TEMP_DIR)


def main():
    create_cleaned_billboard_dataset()
    create_spotify_dataset()
    _delete_temp_files()


if __name__ == "__main__":
    main()
