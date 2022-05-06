from tqdm import tqdm
from datetime import datetime, timezone, timedelta


class Price:
    def __init__(self, app):
        self.db = app.db
        self.price_dict = {}

    def aggregate_prices(self):
        prices_collection = self.db.prices
        prices_aggregated_collection = self.db.prices_aggregated
        price_list = list(prices_collection.find())

        for price in tqdm(price_list):
            price.pop('_id', None)
            price.pop('tcgplayer_id', None)

            for price_detail in price['prices']:
                price_detail.update({'created_at': price['created_at']})

            price.pop('created_at', None)
            query = {"id": price['id']}

            upsert_data = {
                '$push': {'price_history': {'$each': price['prices']}},
                '$setOnInsert': {'id': price['id']}
            }

            prices_aggregated_collection.update_one(query, upsert_data, upsert=True)

        prices_collection.drop()

    def load_prices(self):
        today = datetime.now()
        DD = timedelta(days=2)
        earlier = today - DD
        pipeline = [
            {'$project': {
                'price_history': {'$filter': {
                    'input': '$price_history',
                    'as': 'item',
                    'cond': {'$and': [
                        {'$eq': ['$$item.printingType', 'Normal']},
                        {'$gt': ['$$item.created_at', earlier]}
                    ]}
                }},
                'id': 1,
                '_id': 0,
            }}
        ]
        prices_aggregated_collection = self.db.prices_aggregated
        price_list = list(prices_aggregated_collection.aggregate(pipeline))
        self.price_dict = {item['id']: {'price_history': item['price_history']} for item in price_list}

    def calculate_price_movements(self):
        self.load_prices()
        for card_price in tqdm(self.price_dict.items()):
            list_normal_price = []
            for price_history in card_price[1]['price_history']:
                if price_history['printingType'] == 'Normal':
                    list_normal_price.append(price_history)

            normal_money, normal_percentage, normal_last_price = self._get_last_movement(list_normal_price)
            self.price_dict[card_price[0]]['normal_last_price'] = normal_last_price
            self.price_dict[card_price[0]]['normal_movement_money'] = normal_money
            self.price_dict[card_price[0]]['normal_movement_percentage'] = normal_percentage

    def get_card_name(self, id):
        cards_collection = self.db.cards
        query = {"id": id}
        card = cards_collection.find_one(query)
        return card['name']

    def save_alert(self):
        THRESHOLD = 0.5
        dict_alert = {
            'threshold': THRESHOLD,
            'created_at': datetime.now(timezone.utc),
            'cards': []
        }

        for item in self.price_dict.items():
            # Percentage has to be over the threshold but also the money movement has to be over 1 USD
            if item[1]['normal_movement_percentage'] > THRESHOLD and item[1]['normal_movement_money'] > 1.0:
                dict_alert['cards'].append({
                    'id': item[0],
                    'name': self.get_card_name(item[0]),
                    'last_price': item[1]['normal_last_price'],
                    'normal_movement_money': item[1]['normal_movement_money'],
                    'normal_movement_percentage': item[1]['normal_movement_percentage']
                })

        alerts_collection = self.db.alerts
        alerts_collection.insert_one(dict_alert)

    def _get_last_movement(self, list_price):
        if len(list_price) < 2:
            return 0, 0, 0

        last_price = list_price[-1]['marketPrice']
        day_before_last_price = list_price[-2]['marketPrice']

        if last_price is None or day_before_last_price is None:
            return 0, 0, 0

        movement_money = last_price - day_before_last_price
        movement_percentage = movement_money / day_before_last_price
        return movement_money, movement_percentage, last_price
