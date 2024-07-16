from datetime import datetime, timedelta
import unittest
import grequests
import json
import time
import logging.handlers
import pyfiglet
from logging.handlers import TimedRotatingFileHandler
import os
import traceback
from engines.alerts import Alerts
from engines.bitso import ExchangeEngine

# Title
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
    "app.log",
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
    delay=False,
    utc=False,
)
file_handler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Set the formatter
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def printwt(msg):
    logger.info(f"{datetime.utcnow()} :: {msg}")


def _send_requests(requests):
    responses = grequests.map(requests)
    for response in responses:
        if not response:
            logger.error(response.json())
    return responses


class CryptoEngineTriArbitrage(object):
    def __init__(self, config, engine):
        self.mock = False
        self.config = config
        self.tickerPairA = config["tickerPairA"]
        self.tickerPairB = config["tickerPairB"]
        self.tickerPairC = config["tickerPairC"]
        self.tickerPairs = [self.tickerPairA, self.tickerPairB, self.tickerPairC]
        self.tickerA = config["tickerA"]
        self.tickerB = config["tickerB"]
        self.tickerC = config["tickerC"]
        self.tickers = [self.tickerA, self.tickerB, self.tickerC]
        self.open_orders = True
        self.openOrderCheckCount = 0
        self.engine = engine
        self.book_info = grequests.map(
            [
                self.engine.get_available_books(
                    books=[self.tickerPairA, self.tickerPairB, self.tickerPairC]
                )
            ]
        )[0].parsed
        self.trade_limit = 10
        self.balance_log = None
        # email alerts
        load_dotenv()
        self.emailuser = os.getenv("EMAIL_USR")
        self.emailpwd = os.getenv("EMAIL_PWD")
        self.emailto = os.getenv("EMAIL_TO")
        self.alertsservice = Alerts(self.emailuser, self.emailpwd)

    def main(self):
        try:
            self.main_loop()
        except Exception as e:
            logging.exception("An error occurred: %s", e)
            traceback.print_exc()

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
                        orders, responses = self.place_orders(opportunities)
                        printwt("------- Placed Orders -------")
                        printwt(opportunities)
                        body = f"""
                        Orders: {json.dumps(opportunities, indent=4)},
                        Responses: {json.dumps(responses, indent=4)}
                        """
                        self.alertsservice.email_alert(
                            self.emailto, "Order Placed", body
                        )
                        self.open_orders = True
                        n_of_trades += 1
                        time.sleep(300)
                    else:
                        printwt("------- No orders placed for Mock mode -------")
                    printwt("------- Balance after trade -------")
                    printwt(self.get_balances())
                    time.sleep(5)

    def get_balances(self):
        return grequests.map(
            [
                self.engine.get_balance(
                    tickers=[self.tickerA, self.tickerB, self.tickerC]
                )
            ]
        )[0].parsed

    def check_open_orders(self):
        orders = grequests.map([self.engine.list_open_orders()])[0].json()["payload"]
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

    def get_bid_route(self, books, fees):
        fee_factor1 = 1 - float(fees[self.tickerPairA]["taker_fee_decimal"]) / 100
        fee_factor2 = 1 - float(fees[self.tickerPairB]["taker_fee_decimal"]) / 100
        fee_factor3 = 1 - float(fees[self.tickerPairC]["taker_fee_decimal"]) / 100
        # bid route
        bid_route = (
            (1 / books[0]["ask"]["price"])
            * books[1]["bid"]["price"]
            * books[2]["bid"]["price"]
        )
        bid_route = bid_route * fee_factor1 * fee_factor2 * fee_factor3
        return bid_route

    def get_ask_route(self, books, fees):
        fee_factor1 = 1 - float(fees[self.tickerPairA]["taker_fee_decimal"]) / 100
        fee_factor2 = 1 - float(fees[self.tickerPairB]["taker_fee_decimal"]) / 100
        fee_factor3 = 1 - float(fees[self.tickerPairC]["taker_fee_decimal"]) / 100
        # bid route
        ask_route = (
            (1 / books[2]["ask"]["price"])
            / books[1]["ask"]["price"]
            * books[0]["bid"]["price"]
        )
        ask_route = ask_route * fee_factor1 * fee_factor2 * fee_factor3
        return ask_route

    def check_order_book(self):
        rs = [
            self.engine.get_order_book_innermost(book=self.tickerPairA),
            self.engine.get_order_book_innermost(book=self.tickerPairB),
            self.engine.get_order_book_innermost(book=self.tickerPairC),
        ]
        books = [res.parsed for res in _send_requests(rs)]
        # check that there a re bids and asks
        for book in books:
            if "bid" not in book or "ask" not in book:
                return None
        fees = grequests.map(
            [
                self.engine.list_fees(
                    books=[self.tickerPairA, self.tickerPairB, self.tickerPairC]
                )
            ]
        )[0].parsed
        # bid route
        bid_route = self.get_bid_route(books, fees)
        ask_route = self.get_ask_route(books, fees)
        printwt(f"Bid route: {bid_route}; Ask route: {ask_route}")
        if bid_route > 1 or ask_route > 1:
            if bid_route > ask_route:
                max_amounts = self.get_max_amounts_bid_route(books)
                if not self.validate_max_amounts(max_amounts):
                    printwt("Can't make trade, amounts too low")
                    return None
                max_amounts = self.top_max_amounts(max_amounts)
                orders = [
                    {
                        "book": self.tickerPairA,
                        "major": max_amounts[0],
                        "side": "buy",
                        "price": books[0]["ask"]["price"],
                        "type": "limit",
                    },
                    {
                        "book": self.tickerPairB,
                        "major": max_amounts[1],
                        "side": "sell",
                        "price": books[1]["bid"]["price"],
                        "type": "limit",
                    },
                    {
                        "book": self.tickerPairC,
                        "major": max_amounts[2],
                        "side": "sell",
                        "price": books[2]["bid"]["price"],
                        "type": "limit",
                    },
                ]
                return orders
            else:
                printwt("------- Route -------")
                max_amounts = self.get_max_amounts_ask_route(books)
                if not self.validate_max_amounts(max_amounts):
                    printwt("Can't make trade, amounts too low")
                    return None
                max_amounts = self.top_max_amounts(max_amounts)
                orders = [
                    {
                        "book": self.tickerPairA,
                        "major": max_amounts[0],
                        "side": "sell",
                        "price": books[0]["bid"]["price"],
                        "type": "limit",
                    },
                    {
                        "book": self.tickerPairB,
                        "major": max_amounts[1],
                        "side": "buy",
                        "price": books[1]["ask"]["price"],
                        "type": "limit",
                    },
                    {
                        "book": self.tickerPairC,
                        "major": max_amounts[2],
                        "side": "buy",
                        "price": books[2]["ask"]["price"],
                        "type": "limit",
                    },
                ]
                return orders
        else:
            return None

    def validate_max_amounts(self, amounts):
        ticketPairA_amnt = amounts[0]
        ticketPairB_amnt = amounts[1]
        ticketPairC_amnt = amounts[2]
        minAmountA = self.book_info[self.tickerPairA]["minimum_amount"]
        minAmountB = self.book_info[self.tickerPairB]["minimum_amount"]
        minAmountC = self.book_info[self.tickerPairC]["minimum_amount"]
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

    def top_max_amounts(self, amounts):
        maxAmountA = self.book_info[self.tickerPairA]["maximum_amount"]
        maxAmountB = self.book_info[self.tickerPairB]["maximum_amount"]
        maxAmountC = self.book_info[self.tickerPairC]["maximum_amount"]
        amounts[0] = min(amounts[0], maxAmountA)
        amounts[1] = min(amounts[1], maxAmountB)
        amounts[2] = min(amounts[2], maxAmountC)
        return amounts

    def get_max_amounts_ask_route(self, books):
        # sell eth for btc -> sell btc for mxn -> buy eth with mxn
        # get balances
        balances = grequests.map(
            [
                self.engine.get_balance(
                    tickers=[self.tickerA, self.tickerB, self.tickerC]
                )
            ]
        )[0].parsed
        max_amount_eth_mxn = self.calculate_max_amount(
            books[0], balances, "bid", "sell"
        )
        max_amount_eth_btc = self.calculate_max_amount(books[1], balances, "ask", "buy")
        max_amount_btc_mxn = self.calculate_max_amount(books[2], balances, "ask", "buy")
        printwt("Maximum amount for bid eth_btc: " + str(max_amount_eth_btc))
        printwt("Maximum amount for bid btc_mxn: " + str(max_amount_btc_mxn))
        printwt("Maximum amount for ask eth_mxn: " + str(max_amount_eth_mxn))
        return [max_amount_eth_mxn, max_amount_eth_btc, max_amount_btc_mxn]

    def get_max_amounts_bid_route(self, books):
        # sell eth for mx -> buy btc with mxn -> buy eth with btc
        balances = grequests.map(
            [
                self.engine.get_balance(
                    tickers=[self.tickerA, self.tickerB, self.tickerC]
                )
            ]
        )[0].parsed
        max_amount_eth_mxn = self.calculate_max_amount(books[0], balances, "ask", "buy")
        max_amount_eth_btc = self.calculate_max_amount(
            books[1], balances, "bid", "sell"
        )
        max_amount_btc_mxn = self.calculate_max_amount(
            books[2], balances, "bid", "sell"
        )
        printwt("Maximum amount for ask eth_btc:" + str(max_amount_eth_btc))
        printwt("Maximum amount for bid btc_mxn:" + str(max_amount_btc_mxn))
        printwt("Maximum amount for bid eth_mxn:" + str(max_amount_eth_mxn))
        return [max_amount_eth_mxn, max_amount_eth_btc, max_amount_btc_mxn]

    def calculate_max_amount(self, order, balance, order_type, action):
        ticker_left, ticker_right = order["book"].split("_")
        if action == "buy":
            balance_amount_major = (
                balance[ticker_right] * 0.8 / order[order_type]["price"]
            )
            amount_to_trade = min(balance_amount_major, order[order_type]["amount"])
        else:
            balance_amount_major = balance[ticker_left] * 0.8
            amount_to_trade = min(balance_amount_major, order[order_type]["amount"])
        return round(amount_to_trade, 8)

    def place_orders(self, orders):
        orders = [
            self.engine.place_order(orders[0]),
            self.engine.place_order(orders[1]),
            self.engine.place_order(orders[2]),
        ]
        order_responses = [res.json() for res in _send_requests(orders)]
        self.open_orders = True
        return orders, order_responses


class TestTriangularArbitrage(unittest.TestCase):

    def setUp(self) -> None:
        f = open("arbitrage_config.json")
        arbitrage_config = json.load(f)
        f.close()
        self.engine = ExchangeEngine(arbitrage_config["test_url"])
        self.engine.load_key(arbitrage_config["test_keyFile"])
        self.triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, self.engine)
        return super().setUp()

    def test_triangular_arbitrage(self):
        self.triangular_arb.main_loop()


if __name__ == "__main__":
    # run all tests
    # unittest.main()
    # run a specific test
    suite = unittest.TestSuite()
    suite.addTest(
        TestTriangularArbitrage("test_triangular_arbitrage")
    )  # Include only the test you want to run
    unittest.TextTestRunner().run(suite)
