from pymongo import MongoClient
from crawler import Crawler


class Db(Crawler):
    def __init__(self, **kwargs):
        self.client = MongoClient()
        self.db = self.client.audio_analysis
        super(Db, self).__init__(**kwargs)

    def insert_data(self, table, data):
        result = self.db[table].insert_many(data)
        return result.inserted_ids
