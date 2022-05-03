from utils import App, Price


if __name__ == '__main__':
    app = App()
    price = Price(app)

    price.aggregate_prices()
    price.calculate_price_movements()
    price.save_alert()
