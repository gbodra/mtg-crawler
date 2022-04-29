import os
import time
import asyncio
import requests
from tqdm.asyncio import tqdm
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from typing import Callable, List


async def savePrice(tcgplayer_id, id):
    card_price_obj = {}

    url = 'https://mpapi.tcgplayer.com/v2/product/' + str(tcgplayer_id) + '/pricepoints'
    tcg_card_pricing = requests.get(url).json()
    card_price_obj['tcgplayer_id'] = tcgplayer_id
    card_price_obj['id'] = id
    card_price_obj['prices'] = tcg_card_pricing

    prices_collection.insert_one(card_price_obj)

async def readPrices(cards_list: List, inner: Callable):
    tasks = []
    for card_item in cards_list:
        tasks.append(
            inner(card_item['tcgplayer_id'], card_item['id'])
        )
    
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    return responses


load_dotenv()
CACHE = os.environ.get('CACHE')

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
cards_list = []

for card in tqdm(cards, desc='Downloading cards info...'):
    cards_list.append(card)

print(datetime.now())
responses = asyncio.get_event_loop().run_until_complete(readPrices(cards_list, savePrice))
