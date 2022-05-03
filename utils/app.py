import os
from dotenv import load_dotenv
from pymongo import MongoClient


class App:
    def __init__(self):
        load_dotenv()
        self.mongo_client = MongoClient(os.environ['MONGO_URI'])
        self.db = self.mongo_client.mtg
        self.TCG_URI = os.environ['TCG_URI']
        self.SCRYFALL_URI = os.environ['SCRYFALL_URI']
        self.THREAD_COUNT = int(os.environ['THREAD_COUNT'])
