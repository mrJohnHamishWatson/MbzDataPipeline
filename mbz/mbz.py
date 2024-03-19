
from typing import List, Tuple, Union

import musicbrainzngs as mbz


class MbzClient:
    def __init__(self):
        self.mbz = mbz
        self.mbz.set_useragent('Example.app', '0.1')

    def get_artist(self, artist_name: str = "*", limit: int = 100) -> List[Tuple]:
        """
        Getting id and name about artist
        :param limit:
        :param artist_name:
        :return:
        """
        artist_list = mbz.search_artists(query=artist_name, limit=limit)['artist-list']
        return [(artist['id'], artist['name']) for artist in artist_list]

    def get_album(self, artist_id: str, limit: int = 100):
        if not artist_id:
            return None
        release_list = self.mbz.browse_releases(artist=artist_id, limit=limit)['release-list']
        return [release['title'] for release in release_list]

    def get_songs(self, album: str, artist_id: str) -> Union[List, None]:
        """
        Get songs by album
        :param artist_id:
        :param album:
        :return:
        """
        try:
            if not album or not artist_id:
                return None
            result = self.mbz.search_releases(artist=artist_id,
                                              release=album, limit=1)
            id = result["release-list"][0]["id"]
            res_list = []

            new_result = self.mbz.get_release_by_id(id, includes=["recordings"])
            t = (new_result["release"]["medium-list"][0]["track-list"])
            for x in range(len(t)):
                line = (t[x])
                res_list.append(line["recording"]["title"])

            return res_list
        except Exception as err:
            print(err)
            return []
