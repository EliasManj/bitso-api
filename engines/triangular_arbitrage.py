from datetime import datetime, timedelta
import unittest
import grequests
import json
import time
import logging
import logging.handlers
import pyfiglet
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler

from engines.bitso import ExchangeEngine

# title
title = "Bitso API Bot"
ascii_art = pyfiglet.figlet_format(title, font="slant")

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler and set level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create timed rotating file handler and set level to DEBUG
file_handler = TimedRotatingFileHandler(
    'app.log', when='midnight', interval=1, backupCount=7, encoding='utf-8', delay=False, utc=False
)
file_handler.setLevel(logging.DEBUG)

# Create Logstash handler
# Create Logstash handler
logstash_handler = AsynchronousLogstashHandler(
    host='logstash',  # Use the service name as defined in docker-compose.yml
    port=5000,
    database_path=None  # Use None if you don't need a persistent queue
)
logstash_handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Set the formatter
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logstash_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.addHandler(logstash_handler)

def printwt(msg):
    logger.info(f'{datetime.utcnow()} :: {msg}')

def _send_requests(requests):
    responses = grequests.map(requests)
    for response in responses:
        if not response:
            logger.error(response.json())
            raise Exception
    return responses


class CryptoEngineTriArbitrage(object):
    def __init__(self, config, engine, mock=False):
        self.mock = False
        self.config = config
        self.tickerPairA = config['tickerPairA']
        self.tickerPairB = config['tickerPairB']
        self.tickerPairC = config['tickerPairC']
        self.tickerPairs = [self.tickerPairA, self.tickerPairB, self.tickerPairC]
        self.tickerA = config['tickerA']
        self.tickerB = config['tickerB']
        self.tickerC = config['tickerC']
        self.tickers = [self.tickerA, self.tickerB, self.tickerC]
        self.open_orders = True
        self.openOrderCheckCount = 0
        self.engine = engine
        self.engine.load_key(config['keyFile'])
        self.book_info = grequests.map([self.engine.get_available_books(books=[self.tickerPairA, self.tickerPairB, self.tickerPairC])])[0].parsed
        self.trade_limit = 10
        self.balance_log = None

    def main_loop(self):
        printwt(ascii_art)
        n_of_trades = 0
        printwt("------- Balance -------")
        printwt(self.get_balances())
        while True:
            if n_of_trades >= self.trade_limit:
                break
            if self.open_orders:
                self.check_open_orders()
            else:
                opportunities = self.check_order_book()
                if opportunities:
                    printwt("------- Opportunities -------")
                    printwt(opportunities)
                    if not self.mock:
                        orders = self.place_orders(opportunities)
                        printwt("------- Placed Orders -------")
                        printwt(orders)
                        self.open_orders = True
                        n_of_trades += 1
                        time.sleep(300)
                    else:
                        printwt("------- No orders placed for Mock mode -------")
                    printwt("------- Balance after trade -------")
                    printwt(self.get_balances())
                    time.sleep(5)

    def get_balances(self):
        return grequests.map([self.engine.get_balance(tickers=[self.tickerA, self.tickerB, self.tickerC])])[
            0].parsed

    def check_open_orders(self):
        orders = grequests.map([self.engine.list_open_orders()])[0].json()['payload']
        # too many orders, something went wrong, cancel all orders
        if len(orders) > 3:
            self.engine.cancel_all_orders()
            self.open_orders = False
            return
        # no orders
        if len(orders) == 0:
            self.open_orders = False
            return
        # some orders have been filled but orders still pending
        if 0 < len(orders) < 3:
            self.open_orders = True
            self.print_open_orders(orders)
            time.sleep(300)
            return

    def print_open_orders(self, orders):
        current_datetime = datetime.now()
        if self.balance_log is None:
            printwt("Open Orders")
            printwt(orders)
            self.balance_log = current_datetime
        else:
            if current_datetime >= self.balance_log + timedelta(minutes=30):
                self.balance_log = current_datetime
                printwt("Open Orders")
                printwt(orders)

    def check_order_book(self):
        rs = [
            self.engine.get_order_book_innermost(book=self.tickerPairA),
            self.engine.get_order_book_innermost(book=self.tickerPairB),
            self.engine.get_order_book_innermost(book=self.tickerPairC)
        ]
        books = [res.parsed for res in _send_requests(rs)]
        fees = grequests.map([self.engine.list_fees(books=[self.tickerPairA, self.tickerPairB, self.tickerPairC])])[0].parsed
        # bid route
        fee_factor1 = 1 - float(fees[self.tickerPairA]['taker_fee_decimal']) / 100
        fee_factor2 = 1 - float(fees[self.tickerPairB]['taker_fee_decimal']) / 100
        fee_factor3 = 1 - float(fees[self.tickerPairC]['taker_fee_decimal']) / 100
        bid_route = (1 / books[0]['ask']['price'])*fee_factor1 / books[1]['ask']['price']*fee_factor2 * books[2]['bid']['price']*fee_factor3
        ask_route = (1 * books[0]['bid']['price'])*fee_factor1 / books[2]['ask']['price']*fee_factor3 * books[1]['bid']['price']*fee_factor2
        printwt(f'Bid route: {bid_route}; Ask route: {ask_route}')
        if bid_route > 1 or ask_route > 1:
            if bid_route > ask_route:
                max_amounts = self.get_max_amounts_bid_route(books)
                if not self.validate_max_amounts(max_amounts):
                    printwt("Can't make trade, amounts too low")
                    return None
                printwt("------- Route -------")
                printwt(
                    f'Sell {max_amounts[2]} of {self.tickerA} for {max_amounts[2] * books[2]["bid"]["price"]} of {self.tickerC}')
                printwt(
                    f'Then buy {max_amounts[1]} of {self.tickerB} with {max_amounts[1] * books[1]["ask"]["price"]} of {self.tickerC}')
                printwt(
                    f'Then buy {max_amounts[0]} of {self.tickerA} with {max_amounts[0] * books[0]["ask"]["price"]} of {self.tickerB}')
                orders = [
                    {
                        'book': self.tickerPairA,
                        'major': max_amounts[0],
                        'side': 'buy',
                        'price': books[0]['ask']['price'],
                        'type': 'limit'
                    },
                    {
                        'book': self.tickerPairB,
                        'major': max_amounts[1],
                        'side': 'buy',
                        'price': books[1]['ask']['price'],
                        'type': 'limit'
                    },
                    {
                        'book': self.tickerPairC,
                        'major': max_amounts[2],
                        'side': 'sell',
                        'price': books[2]['bid']['price'],
                        'type': 'limit'
                    }
                ]
                return orders
            else:
                printwt("------- Route -------")
                max_amounts = self.get_max_amounts_ask_route(books)
                if not self.validate_max_amounts(max_amounts):
                    printwt("Can't make trade, amounts too low")
                    return None
                printwt(
                    f'Sell {max_amounts[0]} of {self.tickerA} for {max_amounts[0] * books[0]["bid"]["price"]} of {self.tickerB}')
                printwt(f'Then sell {max_amounts[1]} of {self.tickerB} for {self.tickerC}')
                printwt(f'Then buy {max_amounts[2]} of {self.tickerA} with {self.tickerC}')
                orders = [
                    {
                        'book': self.tickerPairA,
                        'major': max_amounts[0],
                        'side': 'sell',
                        'price': books[0]['bid']['price'],
                        'type': 'limit'
                    },
                    {
                        'book': self.tickerPairB,
                        'major': max_amounts[1],
                        'side': 'sell',
                        'price': books[1]['bid']['price'],
                        'type': 'limit'
                    },
                    {
                        'book': self.tickerPairC,
                        'major': max_amounts[2],
                        'side': 'buy',
                        'price': books[2]['ask']['price'],
                        'type': 'limit'
                    }
                ]
                return orders
        else:
            return None

    def validate_max_amounts(self, amounts):
        ticketPairA_amnt = amounts[0]
        ticketPairB_amnt = amounts[1]
        ticketPairC_amnt = amounts[2]
        minAmountA = self.book_info[self.tickerPairA]['minimum_amount']
        minAmountB = self.book_info[self.tickerPairB]['minimum_amount']
        minAmountC = self.book_info[self.tickerPairC]['minimum_amount']
        if ticketPairA_amnt < minAmountA:
            printwt(f"Min amount for trading {self.tickerPairA} is {minAmountA}")
            return False
        elif ticketPairB_amnt < minAmountB:
            printwt(f"Min amount for trading {self.tickerPairB} is {minAmountB}")
            return False
        elif ticketPairC_amnt < minAmountC:
            printwt(f"Min amount for trading {self.tickerPairC} is {minAmountC}")
            return False
        return True

    def get_max_amounts_ask_route(self, books):
        # sell eth for btc -> sell btc for mxn -> buy eth with mxn
        # get balances
        balances = grequests.map([self.engine.get_balance(tickers=[self.tickerA, self.tickerB, self.tickerC])])[
            0].parsed
        max_amount_eth_btc = self.calculate_max_amount(books[0], balances, 'bid')
        max_amount_btc_mxn = self.calculate_max_amount(books[1], balances, 'bid', prev_order=books[0]['bid'])
        max_amount_eth_mxn = self.calculate_max_amount(books[2], balances, 'ask', prev_order=books[1]['bid'])
        printwt("Maximum amount for bid eth_btc: " + str(max_amount_eth_btc))
        printwt("Maximum amount for bid btc_mxn: " + str(max_amount_btc_mxn))
        printwt("Maximum amount for ask eth_mxn: " + str(max_amount_eth_mxn))
        return [max_amount_eth_btc, max_amount_btc_mxn, max_amount_eth_mxn]

    def get_max_amounts_bid_route(self, books):
        # sell eth for mx -> buy btc with mxn -> buy eth with btc
        balances = grequests.map([self.engine.get_balance(tickers=[self.tickerA, self.tickerB, self.tickerC])])[
            0].parsed
        max_amount_eth_btc = self.calculate_max_amount(books[0], balances, 'ask')
        max_amount_btc_mxn = self.calculate_max_amount(books[1], balances, 'ask', prev_order=books[0]['ask'])
        max_amount_eth_mxn = self.calculate_max_amount(books[2], balances, 'bid', prev_order=books[1]['ask'])
        printwt("Maximum amount for ask eth_btc:" + str(max_amount_eth_btc))
        printwt("Maximum amount for ask btc_mxn:" + str(max_amount_btc_mxn))
        printwt("Maximum amount for bid eth_mxn:" + str(max_amount_eth_mxn))
        return [max_amount_eth_btc, max_amount_btc_mxn, max_amount_eth_mxn]

    def calculate_max_amount(self, order, balance, order_type, prev_order=None):
        ticker_from, ticker_to = order['book'].split('_')
        amount_to_trade = min(balance[ticker_from], balance[ticker_to] / order[order_type]['price'],
                              order[order_type]['amount'])
        if prev_order:
            amount_from_prev_trade = prev_order['amount']
            #amount_to_trade += amount_from_prev_trade
        return round(amount_to_trade, 8)

    def place_orders(self, orders):
        orders = [
            self.engine.place_order(orders[0]),
            self.engine.place_order(orders[1]),
            self.engine.place_order(orders[2])
        ]
        order_responses = [res.json() for res in _send_requests(orders)]
        self.open_orders = True
        return order_responses


class TestTriangularArbitrage(unittest.TestCase):

    def setUp(self) -> None:
        f = open('arbitrage_config.json')
        arbitrage_config = json.load(f)
        f.close()
        self.engine = ExchangeEngine(arbitrage_config['url'])
        self.triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, self.engine, mock=True)
        return super().setUp()

    def test_triangular_arbitrage(self):
        self.triangular_arb.main_loop()


if __name__ == '__main__':
    # run all tests
    # unittest.main()
    # run a specific test
    suite = unittest.TestSuite()
    suite.addTest(TestTriangularArbitrage('test_triangular_arbitrage'))  # Include only the test you want to run
    unittest.TextTestRunner().run(suite)
