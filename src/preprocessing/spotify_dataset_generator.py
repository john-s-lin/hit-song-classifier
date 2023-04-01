import dotenv
import logging
import os
import pandas as pd
import re
import sys

# Set logging level
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from dask import dataframe as dd

# Load environment variables from .env
dotenv.load_dotenv()
SPOTIPY_CLIENT_ID = os.environ.get("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.environ.get("SPOTIPY_CLIENT_SECRET")

# Initialize target Billboard dataset
TARGET_FILE = "data/classified_billboard_songs2.csv"
TARGET_CLEANED_FILE = TARGET_FILE.split(".csv")[0] + "_clean.csv"
SPOTIFY_DATASET_FILE = "data/spotify_enhanced_dataset.csv"


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
            search_query = title + artist
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
            for j, track_id in enumerate(track_ids):
                audio_features[track_id] = audio_features_list[j]

        features_list = []
        for id_, features in audio_features.items():
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
        return dd.read_csv(filename)


class BillboardDatasetCleaner:
    def __init__(self) -> None:
        pass

    def clean_raw_billboard_dataset(self, filename: str) -> None:
        """Cleans Billboard song names with '\' and '"' punctuation

        Args:
            filename (str): filename
        """
        csv_list = list()
        with open(filename, "r") as file:
            line = file.readline()
            while line:
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
        output = "".join(csv_list)
        new_filename = filename.split(".csv")[0] + "_clean.csv"

        with open(new_filename, "w", newline="") as csvfile:
            csvfile.write(output)

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


def main():
    if not os.path.exists(TARGET_CLEANED_FILE):
        logging.info(f"Cleaning {TARGET_FILE}...")
        bb_cleaner = BillboardDatasetCleaner()
        bb_cleaner.clean_raw_billboard_dataset(TARGET_FILE)

    sp_generator = SpotifyDatasetGenerator()
    df = sp_generator.get_cleaned_billboard_dataset(TARGET_CLEANED_FILE)
    df_with_track_id = sp_generator.drop_rows_if_empty_track_id(
        sp_generator.create_columns_track_id_popularity(df)
    )
    df_with_audio_features = sp_generator.create_columns_audio_features(df_with_track_id)
    df_with_audio_features.drop(columns=["id"]).to_csv(
        SPOTIFY_DATASET_FILE, single_file=True, index=False
    )


if __name__ == "__main__":
    main()
