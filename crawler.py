import os
import time
import asyncio
import requests
import threading, queue
from tqdm.asyncio import tqdm
from dotenv import load_dotenv
from pymongo import MongoClient
from typing import Callable, List
from datetime import datetime, timezone

q = queue.Queue()


def savePriceWorker():
    while True:
        item = q.get()
        print(q.qsize(), flush=True)
        card_price_obj = {}

        url = 'https://mpapi.tcgplayer.com/v2/product/' + str(item[0]) + '/pricepoints'

        try:
            tcg_card_pricing = requests.get(url).json()
            card_price_obj['tcgplayer_id'] = item[0]
            card_price_obj['id'] = item[1]
            card_price_obj['prices'] = tcg_card_pricing
            card_price_obj['created_at'] = datetime.now(timezone.utc)
            prices_collection.insert_one(card_price_obj)
        except:
            print('Error inserting price')
        
        q.task_done()


load_dotenv()
CACHE = os.environ.get('CACHE')
THREAD_COUNT = int(os.environ.get('THREAD_COUNT'))

sets_response = requests.get('https://api.scryfall.com/sets')

sets_response_json = sets_response.json()

client = MongoClient(os.environ['MONGO_URI'])
db = client.mtg

sets_collection = db.sets
cards_collection = db.cards
prices_collection = db.prices

if CACHE != 'True':
    for set_record in tqdm(sets_response_json['data'], desc='Saving sets'):
        sets_collection.insert_one(set_record)

    for set_record in tqdm(sets_response_json['data'], desc='Saving cards from sets'):
        if set_record['card_count'] > 0:
            cards_response = requests.get(set_record['search_uri'])
            cards_response_json = cards_response.json()
            for card_record in cards_response_json['data']:
                cards_collection.insert_one(card_record)
            time.sleep(0.05)

query = {"tcgplayer_id": {"$exists": True}}
cards = cards_collection.find(query)

start_time = datetime.now(timezone.utc)

for i in range(THREAD_COUNT):
    threading.Thread(target=savePriceWorker, daemon=True).start()

# print(datetime.now(timezone.utc))

for card in cards:
    card_tuple = (card['tcgplayer_id'], card['id'])
    q.put(card_tuple)

q.join()
end_time = datetime.now(timezone.utc)
print((end_time - start_time))
