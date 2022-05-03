import pprint
from tqdm import tqdm
from datetime import datetime, timezone


class Price:
    def __init__(self, app):
        self.db = app.db
        self.price_dict = {}

    def aggregate_prices(self):
        prices_collection = self.db.prices
        price_cursor = prices_collection.find()

        for price in tqdm(price_cursor):
            price_history = []
            for price_detail in price['prices']:
                price_item = {
                    'printingType': price_detail['printingType'],
                    'marketPrice': price_detail['marketPrice'],
                    'buylistMarketPrice': price_detail['buylistMarketPrice'],
                    'listedMedianPrice': price_detail['listedMedianPrice'],
                    'created_at': price['created_at']
                }
                price_history.append(price_item)

            if price['id'] not in self.price_dict.keys():
                self.price_dict[price['id']] = {'price_history': price_history}
            else:
                self.price_dict[price['id']]['price_history'].extend(price_history)

    def calculate_price_movements(self):
        for card_price in tqdm(self.price_dict.items()):
            list_normal_price = []
            list_foil_price = []
            for price_history in card_price[1]['price_history']:
                if price_history['printingType'] == 'Normal':
                    list_normal_price.append(price_history)
                else:
                    list_foil_price.append(price_history)

            normal_money, normal_percentage, normal_last_price = self._get_last_movement(list_normal_price)
            self.price_dict[card_price[0]]['normal_last_price'] = normal_last_price
            self.price_dict[card_price[0]]['normal_movement_money'] = normal_money
            self.price_dict[card_price[0]]['normal_movement_percentage'] = normal_percentage

            foil_money, foil_percentage, foil_last_price = self._get_last_movement(list_foil_price)
            self.price_dict[card_price[0]]['foil_last_price'] = foil_last_price
            self.price_dict[card_price[0]]['foil_movement_money'] = foil_money
            self.price_dict[card_price[0]]['foil_movement_percentage'] = foil_percentage

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
            if item[1]['normal_movement_percentage'] > THRESHOLD:
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
