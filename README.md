# SpotifyPopularityCollector
Collecting popularity data from Spotify

Pulls Spotify track IDs from MongoDB and inserts their popularity. Aimed at daily popularity collection.

Usage:
```
popularity = SongPopularity(client_id=client_id,
                            client_secret=client_secret,
                            inserter_size=1e+8
                            )

popularity.start_collection(threads=6)
```
# Adding additional collectors
Create a class according to the following template:
```
from songs_db import SongsDb # An additional class is used to handle DB queries specific to this class. It is not required, though.
from collector import Collector
import datetime


class ClassName(SongsDb, Collector):
    ins_table = 'song_popularity' # Name of collection. Used to insert data
    threads = 0
    cons_amount = 50 # Batch size to be processed by the consumer

    def __init__(self, client_id, client_secret, inserter_size=1e+8):
        self.ins_size = inserter_size
        super(SongPopularity, self).__init__(client_id=client_id, client_secret=client_secret)

    def start_collection(self, threads=1):
        self.threads = threads
        self.start()

    def condition(self):
        return self.songs_for_update(count=True) # Call to a function that returns true if there are still items to be processed.

    def producer(self):
        return self.songs_for_update() # Retrieve data. Return a list

    def consumer(self, data):
        return tracks # Process data and return a list

    def inserter(self, data):
        # Return a dictionary of data to be inserted.
        return {'spotify_id': data['id'], 'popularity': data['popularity'], 'time': datetime.datetime.now()}

```
