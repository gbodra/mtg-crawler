import queue
import requests
import threading
from datetime import datetime, timezone


class Price:
    def __init__(self, app):
        self.app = app
        self.db = app.db
        self.prices_collection = self.db.prices
        self.cards_collection = self.db.cards
        self.q = queue.Queue()

    def save_price_worker(self):
        while True:
            item = self.q.get()
            print(self.q.qsize(), flush=True)
            card_price_obj = {}

            url = 'https://mpapi.tcgplayer.com/v2/product/' + str(item[0]) + '/pricepoints'

            try:
                tcg_card_pricing = requests.get(url).json()
                card_price_obj['tcgplayer_id'] = item[0]
                card_price_obj['id'] = item[1]
                card_price_obj['prices'] = tcg_card_pricing
                card_price_obj['created_at'] = datetime.now(timezone.utc)
                self.prices_collection.insert_one(card_price_obj)
            except:
                print('Error inserting price')

            self.q.task_done()

    def run(self):
        query = {"tcgplayer_id": {"$exists": True}}
        cards = self.cards_collection.find(query)

        start_time = datetime.now(timezone.utc)

        for i in range(self.app.THREAD_COUNT):
            threading.Thread(target=self.save_price_worker, daemon=True).start()

        for card in cards:
            card_tuple = (card['tcgplayer_id'], card['id'])
            self.q.put(card_tuple)

        self.q.join()
        end_time = datetime.now(timezone.utc)
        print('Tempo de carga:', (end_time - start_time))
