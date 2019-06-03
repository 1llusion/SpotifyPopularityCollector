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
