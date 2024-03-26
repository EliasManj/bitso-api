from datetime import datetime
import unittest
import grequests
import json

from exchanges.bitso import ExchangeEngine


def printwt(msg):
    print(f'{datetime.utcnow()} :: {msg}')


def _send_requests(requests):
    responses = grequests.map(requests)
    for response in responses:
        if not response:
            printwt(responses)
            raise Exception
    return responses


class CryptoEngineTriArbitrage(object):
    def __init__(self, config, engine, mock=False):
        self.mock = mock
        self.config = config
        self.tickerPairA = config['tickerPairA']
        self.tickerPairB = config['tickerPairB']
        self.tickerPairC = config['tickerPairC']
        self.tickerA = config['tickerA']
        self.tickerB = config['tickerB']
        self.tickerC = config['tickerC']
        self.tickers = [self.tickerA, self.tickerB, self.tickerC]
        self.minProfitUSDT = 0.3
        self.hasOpenOrder = True  # always assume there are open orders first
        self.openOrderCheckCount = 0
        self.engine = engine
        self.engine.load_key('keys/bitso_stage.key')

    def start(self):
        printwt("Starting Triangular Arbitrage")
        print(self.config)
        if self.mock:
            print("------------Mock Mode----------")
        while True:
            try:
                if not self.mock and self.hasOpenOrder:
                    self.check_order_book()
                else:
                    self.check_order_book()
            except Exception as e:
                print(e)

    def get_last_prices(self):
        requests = [self.engine.get_ticker_last_price(self.tickerPairA),
                    self.engine.get_ticker_last_price(self.tickerPairB),
                    self.engine.get_ticker_last_price(self.tickerPairC)]
        last_prices = []
        printwt('Last Prices')
        for response in _send_requests(requests):
            last_prices.append(response.parsed)
            print(response.parsed)
        return last_prices

    def get_order_book_innermost(self):
        printwt('Innermost prices in order book')
        requests = [self.engine.get_order_book_innermost(self.tickerPairA),
                    self.engine.get_order_book_innermost(self.tickerPairB),
                    self.engine.get_order_book_innermost(self.tickerPairC)]
        res = [response.parsed for response in _send_requests(requests)]
        for response in res:
            print(response)
        return res

    def get_balances_and_price_in_mxn(self):
        res = grequests.map([self.engine.get_balance(tickers=[self.tickerA,self.tickerB,self.tickerC])])[0].parsed
        for asset in res:
            res[asset] = float(res[asset])
        for asset in res:
            if asset != 'mxn':
                price = grequests.map([self.engine.get_ticker_last_price(book=f'{asset}_mxn')])[0].parsed
                res[asset] = {
                    'ticker': asset,
                    'balance_in_mx': res[asset] * float(price[next(iter(price))]),
                    'balance': res[asset],
                    'price': float(price[next(iter(price))])
                }
            else:
                res[asset] = {
                    'ticker': asset,
                    'balance_in_mx': res[asset],
                    'balance': res[asset],
                    'price': 1
                }
        return res

    def get_fees(self):
        res = grequests.map([self.engine.list_fees(books=[self.tickerPairA,self.tickerPairB,self.tickerPairC])])
        return res[0].parsed

    def get_balances(self):
        res = grequests.map([self.engine.get_balance(tickers=[self.tickerA,self.tickerB,self.tickerC])])
        return res[0].parsed


    def check_order_book(self):
        # Get last prices
        last_prices = self.get_last_prices()
        # Get innermost from orderbook
        innermost = self.get_order_book_innermost()
        # get fees
        fees = self.get_fees()
        # calculate bid and ask routes based on innermost order book info
        # bid route
        bid_route_result = (1 / innermost[0]['ask']['price']) / innermost[1]['ask']['price'] * innermost[2]['bid']['price']
        # ask route
        ask_route_result = (1 * innermost[0]['bid']['price']) / innermost[2]['ask']['price'] * innermost[1]['bid']['price']
        print(f'Bid Route Result: {bid_route_result}')
        print(f'Ask Route Result: {ask_route_result}')
        # status = 1 is bid, status = 2 is ask, status = 0 means no opportunity available
        cond = (bid_route_result > 1 and ask_route_result > 1 and (bid_route_result - 1) * last_prices[0] > (ask_route_result - 1) *
         last_prices[1])

        if bid_route_result > ask_route_result or cond:
            status = 1
        elif ask_route_result > 1:
            status = 2
        else:
            status = 0

        # if there is an oportunity, check the max amout that can be traded of each ticker considering our balance


    def get_max_amounts(self, innermost, fees, status):
        max = []
        balances = self.get_balances_and_price_in_mxn()
        for index, ticker in enumerate([self.tickerA, self.tickerB, self.tickerC]):

            # for a bid route:
            # -- first ticker is an ask
            # -- second ticker is an ask
            # -- third ticker is a bid
            bid_ask = 1 if index == 2 else -1

            # for an ask route we reverse it
            bid_ask = -1*bid_ask if status == 2 else bid_ask

            # convert the number to either 'bid' or 'ask'
            bid_ask = 'bid' if bid_ask == 1 else 'ask'

            # get max amount based on the innermost orderbook
            max_amnt_book = innermost[index][bid_ask]['amount']*balances[ticker]['price']
            max_amnt_balance = balances[ticker]['balance_in_mx']
            max_balance = min(max_amnt_balance, max_amnt_book)

            fee_percent = float(fees[innermost[index]['book']]['taker_fee_percent'])/100

            max_balance = max_balance * (1 - fee_percent)


            if not max or max_balance > max:
                max = max_balance

        max_amounts = []
        for index, ticker in enumerate([self.tickerA, self.tickerB, self.tickerC]):
            max_amounts.append(max/last_price)



class TestTriangularArbitrage(unittest.TestCase):

    def setUp(self) -> None:
        f = open('arbitrage_config.json')
        arbitrage_config = json.load(f)
        f.close()
        self.engine = ExchangeEngine()
        self.triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, self.engine, mock=True)
        return super().setUp()

    def test_triangular_arbitrage(self):
        self.triangular_arb.start()

    def test_get_max_amounts(self):
        last_prices = self.triangular_arb.get_last_prices()
        innermost = self.triangular_arb.get_order_book_innermost()
        fees = self.triangular_arb.get_fees()
        # bid route
        status = 1
        # Get max amount
        self.triangular_arb.get_max_amounts(innermost, fees, status)


if __name__ == '__main__':
    # run all tests
    # unittest.main()
    # run a specific test
    suite = unittest.TestSuite()
    suite.addTest(TestTriangularArbitrage('test_get_max_amounts'))  # Include only the test you want to run
    unittest.TextTestRunner().run(suite)
