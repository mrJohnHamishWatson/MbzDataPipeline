import pandas as pd

from db.postgres import Postgres
from mbz.mbz import MbzClient
from transform import SONGS_TABLE, ARTIST_TABLE, ALBUMS_TABLE

LIST_MUSICIONS = [  # TODO: move it as argument from cli
    "Led Zeppelin",
    "Queen",
    #"Rush",
    #"AC/DC",
    #"Pink Floyd",
    #"The Beatles",
    #"Black Sabbath",

]
CSV_PATH = 'test.csv'

ARTIST_CSV = 'artist.csv'
ALBUM_CSV = 'album.csv'
SONGS_CSV = 'songs.csv'


class DataTransform:
    def process(self):

        mbz_client = MbzClient()

        my_list = []
        for artist in LIST_MUSICIONS:
            my_list.append(mbz_client.get_artist(artist_name=artist))

        my_list = [i[0] for i in my_list]

        df = pd.DataFrame(my_list)

        df = df.rename(columns={0: 'id', 1: "artist"})  # artist, id

        result_dict = {}
        for index, row in df.iterrows():
            artist_id = row['id']
            result_dict[artist_id] = mbz_client.get_album(artist_id)

        album_df = pd.DataFrame.from_dict(result_dict, orient='index')

        album_df = album_df.rename_axis('id')
        album_df = album_df.reset_index()

        melted_album_df = pd.melt(album_df, id_vars=['id'], var_name='album_num', value_name='album_title')

        melted_album_df = melted_album_df.rename_axis('album_id')
        melted_album_df = melted_album_df.reset_index()

        print('start process of getting song')
        album_songs_dict = {}
        # for index, row in album_df.iterrows():
        for index, row in melted_album_df.iterrows():
            album = row['album_title']
            songs = mbz_client.get_songs(album=album,
                                         artist_id=row['id'])
            if songs:
                album_songs_dict[(row['id'], row['album_id'])] = songs

        melted_album_df.to_csv(ALBUM_CSV)

        songs_df = pd.DataFrame.from_dict(album_songs_dict, orient='index')

        songs_df = songs_df.rename_axis('id')
        songs_df = songs_df.reset_index()

        melted_df = pd.melt(songs_df, id_vars=['id'], var_name='song_num', value_name='song_title')

        melted_df[['artist_id', 'album_id']] = melted_df['id'].apply(lambda x: pd.Series(x))

        melted_df.drop(columns=['id'], inplace=True)
        melted_df = melted_df.dropna(subset=['song_title'])

        melted_df = melted_df.reindex(columns=['artist_id', 'album_id', 'song_num', 'song_title'])
        melted_df.to_csv(SONGS_CSV)

        df.to_csv(path_or_buf=ARTIST_CSV)

        my_p = Postgres()
        my_p.copy_data(path=SONGS_CSV, table_name=SONGS_TABLE)
        my_p.copy_data(path=ARTIST_CSV, table_name=ARTIST_TABLE)
        my_p.copy_data(path=ALBUM_CSV, table_name=ALBUMS_TABLE)


