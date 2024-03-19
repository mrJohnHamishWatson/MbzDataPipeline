from typing import Dict, Tuple, Any, List

import pandas as pd

from db.postgres import Postgres
from mbz.mbz import MbzClient
from transform import SONGS_TABLE, ARTIST_TABLE, ALBUMS_TABLE

LIST_MUSICIONS = [  # TODO: move it as argument from cli
    # "Led Zeppelin",
    # "Queen",
    # "Rush",
    # "AC/DC",
    # "Pink Floyd",
    "The Beatles",
    # "Black Sabbath",
]
CSV_PATH = "test.csv"

ARTIST_CSV = "artist.csv"
ALBUM_CSV = "album.csv"
SONGS_CSV = "songs.csv"


class DataTransform:
    def __init__(self):
        self.mbz_client = MbzClient()

    def _get_list_artists(self) -> List:
        my_list = []
        for artist in LIST_MUSICIONS:
            my_list.append(self.mbz_client.get_artist(artist_name=artist))
        return my_list

    def _get_album_dict(self, df: pd.DataFrame) -> Dict:
        result_dict = {}
        for index, row in df.iterrows():
            artist_id = row["id"]
            result_dict[artist_id] = self.mbz_client.get_album(artist_id)
        return result_dict

    def _get_songs_dict(
        self, melted_album_df: pd.DataFrame
    ) -> Dict[Tuple[Any, Any], List[str]]:
        album_songs_dict = {}
        for index, row in melted_album_df.iterrows():
            album = row["album_title"]
            songs = self.mbz_client.get_songs(album=album, artist_id=row["id"])
            if songs:
                album_songs_dict[(row["id"], row["album_id"])] = songs
        return album_songs_dict

    def _fix_artist_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={0: "id", 1: "artist"})  # artist, id

    def _fix_album_df(self, album_df: pd.DataFrame) -> pd.DataFrame:
        album_df = album_df.rename_axis("id")
        album_df = album_df.reset_index()

        melted_album_df = pd.melt(
            album_df, id_vars=["id"], var_name="album_num", value_name="album_title"
        )

        melted_album_df = melted_album_df.rename_axis("album_id")
        return melted_album_df.reset_index()

    def _fix_song_df(self, songs_df: pd.DataFrame) -> pd.DataFrame:
        songs_df = songs_df.rename_axis("id")
        songs_df = songs_df.reset_index()

        melted_df = pd.melt(
            songs_df, id_vars=["id"], var_name="song_num", value_name="song_title"
        )

        melted_df[["artist_id", "album_id"]] = melted_df["id"].apply(
            lambda x: pd.Series(x)
        )

        melted_df.drop(columns=["id"], inplace=True)
        melted_df = melted_df.dropna(subset=["song_title"])

        melted_df = melted_df.reindex(
            columns=["artist_id", "album_id", "song_num", "song_title"]
        )
        return melted_df

    @staticmethod
    def save_to_csv(
        songs_df: pd.DataFrame, artist_df: pd.DataFrame, albums_df: pd.DataFrame
    ):
        songs_df.to_csv(SONGS_CSV)
        artist_df.to_csv(ARTIST_CSV)
        albums_df.to_csv(ALBUM_CSV)

    @staticmethod
    def copy_csv_data():
        my_p = Postgres()
        my_p.copy_data(path=SONGS_CSV, table_name=SONGS_TABLE)
        my_p.copy_data(path=ARTIST_CSV, table_name=ARTIST_TABLE)
        my_p.copy_data(path=ALBUM_CSV, table_name=ALBUMS_TABLE)

    def process(self):
        my_list = [artist[0] for artist in self._get_list_artists()]
        df = pd.DataFrame(my_list)
        df = self._fix_artist_df(df)
        album_df = pd.DataFrame.from_dict(self._get_album_dict(df), orient="index")
        melted_album_df = self._fix_album_df(album_df)
        album_songs_dict = self._get_songs_dict(melted_album_df)
        songs_df = pd.DataFrame.from_dict(album_songs_dict, orient="index")
        melted_df = self._fix_song_df(songs_df)
        self.save_to_csv(songs_df=melted_df, albums_df=melted_album_df, artist_df=df)
        self.copy_csv_data()
