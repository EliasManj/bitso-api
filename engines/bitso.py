import hashlib
import json
import hmac
import time
import unittest
import grequests
from engines.base import ExchangeEngineBase
from urllib.parse import urlparse, urlencode
import requests


class ExchangeEngine(ExchangeEngineBase):
    def __init__(self, url):
        self.API_URL = url
        self.apiVersion = 'v3'
        self.feeRatio = 0.0026
        self.sleepTime = 5
        self.async_ = True  # 'async' is a keyword in Python 3, so rename it to 'async_'
        self.debug = False

    def _sign_request(self, url, httpMethod, body={}, params={}):
        public = self.key['public']
        private = self.key['private']
        nonce = str(int(round(time.time() * 1000000)))
        parsed_url = urlparse(url)
        path = parsed_url.path + ('?' + urlencode(params) if params else '')
        body = json.dumps(body) if body else ''

        data = nonce + httpMethod + path + body

        hash_obj = hmac.new(private.encode('utf-8'), data.encode('utf-8'), hashlib.sha256)
        signature = hash_obj.hexdigest()

        return {
            'Authorization': f'Bitso {public}:{nonce}:{signature}',
        }

    def _debug_request(self, url, method, **args):
        debug = requests.Request(method, url, **args)
        prepared_request = debug.prepare()
        print("-------------------")
        print("Request Method:", prepared_request.method)
        print("Request URL:", prepared_request.url)
        print("Request Headers:")
        for header, value in prepared_request.headers.items():
            print(header + ":", value)
        print("Request Body:", prepared_request.body)
        print("-------------------")

    def _send_request(self, command, httpMethod, body={}, params={}, hook=None):
        command = f'/{self.apiVersion}/{command}/'

        url = self.API_URL + command
        headers = {}
        if httpMethod == "GET":
            R = grequests.get
        elif httpMethod == "POST":
            R = grequests.post
        elif httpMethod == "DELETE":
            R = grequests.delete

        headers.update(self._sign_request(url, httpMethod, body, params))

        args = {'params': params, 'headers': headers}
        if body:
            args.update({'json': body})
        if hook:
            args['hooks'] = dict(response=hook)

        if self.debug:
            self._debug_request(url, httpMethod, **args)
        req = R(url, **args)
        if self.async_:
            return req
        else:
            response = grequests.map([req])[0].json()

        if 'error' in response:
            print(response)
        return response

    def get_ticker_last_price(self, book):
        return self._send_request('ticker', 'GET', {}, {'book' : book},
                                  [self.hook_tickerlastprice(book=book)])

    def hook_tickerlastprice(self, *factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()
            r.parsed = {}
            last_price = json_data['payload']['last']
            r.parsed[factory_kwargs['book']] = float(last_price)

        return res_hook

    def place_order(self, body):
        return self._send_request('orders', 'POST', body)

    def get_balance(self, tickers=[]):
        return self._send_request('balance', 'GET', {}, {},
                                  [self.hook_getBalance(tickers=[ticker.lower() for ticker in tickers])])

    def hook_getBalance(self, *factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()
            r.parsed = {}
            balances = json_data['payload']['balances']
            if factory_kwargs['tickers']:
                filtered = filter(lambda balance: balance['currency'] in factory_kwargs['tickers'], balances)
            else:
                filtered = balances

            for ticker in filtered:
                r.parsed[ticker['currency']] = float(ticker['available'])

        return res_hook

    def list_order_book(self, book):
        return self._send_request('order_book', 'GET', {}, {'book': book, 'aggregate': True})

    def get_order_book_innermost(self, book):
        return self._send_request('order_book', 'GET', {}, {'book': book, 'aggregate': True},
                                  [self.hook_order_book_innermost(book=book)])

    def hook_order_book_innermost(self, *factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()
            r.parsed = {}
            book = json_data['payload']
            r.parsed = {
                'book': factory_kwargs['book'],
                'bid': {
                    'price': float(book['bids'][0]['price']),
                    'amount': float(book['bids'][0]['amount'])
                },
                'ask': {
                    'price': float(book['asks'][0]['price']),
                    'amount': float(book['asks'][0]['amount'])
                }
            }
        return res_hook

    def get_ticker(self, symbol):
        return self._send_request('ticker', 'GET', {}, {'book': symbol})

    def get_available_books(self, books=[]):
        return self._send_request('available_books', 'GET', {}, {}, [self.hook_get_available_books(books=books)])

    def hook_get_available_books(self, *factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()
            r.parsed = {}
            books = json_data['payload']
            if factory_kwargs['books']:
                filtered = list(filter(lambda pair: pair['book'] in factory_kwargs['books'], books))
            else:
                filtered = books
            for book in filtered:
                r.parsed[book['book']] = {
                    'fees': book['fees']['flat_rate'],
                    'minimum_price': book['minimum_price'],
                    'default_chart': book['default_chart'],
                    'minimum_amount': float(book['minimum_amount']),
                }

        return res_hook

    def cancel_all_orders(self):
        return self._send_request('orders/all', 'DELETE')

    def cancel_order(self, oid):
        return self._send_request(f'orders/{oid}', 'DELETE')

    def list_open_orders(self, book=None):
        return self._send_request('open_orders', 'GET', {}, {'book': book}) if book else self._send_request('open_orders', 'GET')

    def lookup_order(self, oid):
        return self._send_request(f'/orders/{oid}', 'GET')

    def list_fees(self, books=[]):
        return self._send_request('fees', 'GET', {}, {}, [self.list_fees_hook(books=books)])

    def list_fees_hook(*factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()['payload']['fees']
            r.parsed = {}
            if factory_kwargs['books']:
                filtered = list(filter(lambda book: book['book'] in factory_kwargs['books'], json_data))
            else:
                filtered = json_data
            for book in filtered:
                r.parsed[book['book']] = book

        return res_hook

class TestBitsoApi(unittest.TestCase):

    def setUp(self) -> None:
        self.engine = ExchangeEngine('https://stage.bitso.com/api')
        self.engine.load_key('keys/bitso_stage.key')
        return super().setUp()

    def validate_api_response(self, res):
        self.assertIsNotNone(res)
        self.assertGreater(len(res), 0)
        self.assertTrue(res)
        response = res[0]
        self.assertEqual(response.status_code, 200)
        return response

    def test_get_balance_all(self):
        for res in grequests.map([self.engine.get_balance()]):
            self.assertIsNotNone(res.parsed)
            self.assertGreater(len(res.parsed), 0)
            self.assertTrue(res.parsed)
            # print(json.dumps(res.parsed, indent=4))

    def test_get_balance_tickers(self):
        for res in grequests.map([self.engine.get_balance(tickers=["usd", "eth", "btc"])]):
            self.assertIsNotNone(res.parsed)
            self.assertGreater(len(res.parsed), 0)
            self.assertTrue(res.parsed)
            # print(json.dumps(res.parsed, indent=4))

    def test_list_available_books(self):
        self.get_books()

    def test_list_available_books_symbol(self):
        self.get_books(books=['eth_mxn', 'eth_btc', 'btc_mxn'])

    def get_books(self, books=None):
        res = grequests.map([self.engine.get_available_books(books=books)])
        response = self.validate_api_response(res)
        parsed = response.parsed
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed)
        if books:
            for book in books:
                self.assertIn(book, parsed)
        return parsed

    def get_ticker(self, symbol):
        res = grequests.map([self.engine.get_ticker(symbol=symbol)])
        response = self.validate_api_response(res)
        ticker_data = response.json()['payload']
        self.assertIsNotNone(ticker_data)
        self.assertIn('book', ticker_data)
        self.assertEqual(ticker_data['book'], symbol)
        return ticker_data

    def test_get_all_tickers(self):
        books = self.get_books()
        for index, symbol in enumerate(books):
            if index >= 10:
                break
            self.get_ticker(symbol=symbol)

    def test_get_specific_tickers(self):
        books = self.get_books(books=['eth_mxn', 'eth_btc', 'btc_mxn'])
        for symbol in books:
            self.get_ticker(symbol=symbol)

    def test_get_order_book(self):
        self.get_order_book('btc_mxn')

    def get_order_book(self, book):
        res = grequests.map([self.engine.list_order_book(book=book)])
        response = self.validate_api_response(res)
        order_book = response.json()['payload']
        self.assertTrue(order_book)
        self.assertIn('bids', order_book)
        self.assertIn('asks', order_book)
        return order_book

    def test_get_orderbook_innermost(self):
        self.get_orderbook_innermost('btc_mxn')

    def get_orderbook_innermost(self, book):
        res = grequests.map([self.engine.get_order_book_innermost(book)])
        response = self.validate_api_response(res).parsed
        self.assertIn('bid', response)
        self.assertIn('ask', response)
        self.assertGreater(response['ask']['price'], response['bid']['price'])

    def test_market_order(self):
        order = {
            'book': 'eth_mxn',
            'minor': 5000,
            'type': 'market',
            'side': 'buy'
        }
        res = grequests.map([self.engine.place_order(order)])
        response = self.validate_api_response(res)

    def test_buy_then_sell_market_order(self):
        buy_order = {
            'book': 'btc_mxn',
            'minor': 200,
            'type': 'market',
            'side': 'buy'
        }
        sell_order = {
            'book': 'btc_mxn',
            'minor': 200,
            'type': 'market',
            'side': 'sell'
        }
        r = grequests.map([self.engine.get_balance(tickers=['BTC', 'MXN'])])
        mxn_balance_1 = self.validate_api_response(r).parsed['mxn']

        res = grequests.map([self.engine.place_order(buy_order)])
        self.validate_api_response(res)

        r = grequests.map([self.engine.get_balance(tickers=['BTC', 'MXN'])])
        mxn_balance_2 = self.validate_api_response(r).parsed['mxn']
        self.assertEqual(float(mxn_balance_1), float(mxn_balance_2) + 200)

        res = grequests.map([self.engine.place_order(sell_order)])
        self.validate_api_response(res)

        r = grequests.map([self.engine.get_balance(tickers=['BTC', 'MXN'])])
        mxn_balance_3 = self.validate_api_response(r).parsed['mxn']
        self.assertEqual(float(mxn_balance_1), float(mxn_balance_3))

    def test_cancell_all_orders(self):
        self.validate_api_response(grequests.map([self.engine.cancel_all_orders()]))

    def test_limit_order(self):
        # get ticker
        res = grequests.map([self.engine.get_ticker(symbol='btc_mxn')])
        response = self.validate_api_response(res).json()['payload']
        bid = float(response['bid'])
        otm_bid = bid - (bid * 0.20)
        otm_bid = round(otm_bid / 10) * 10
        mxn_amount = 500
        btc_amount = mxn_amount / otm_bid
        # do a limit order OTM
        order = {
            'book': 'btc_mxn',
            'major': btc_amount,
            'type': 'limit',
            'side': 'buy',
            'price': otm_bid
        }
        res = grequests.map([self.engine.place_order(order)])
        oid = self.validate_api_response(res).json()['payload']['oid']
        # check that limit order is open
        open_order = self.validate_api_response(grequests.map([self.engine.lookup_order(oid)])).json()['payload'][0]
        self.assertEqual(open_order['oid'], oid)
        self.assertEqual(open_order['price'], str(otm_bid))
        # close the order
        r = grequests.map([self.engine.cancel_order(open_order['oid'])])
        cancel_response = self.validate_api_response(r).json()['payload'][0]
        self.assertEqual(cancel_response, oid)

    def test_list_fees(self):
        books = ['btc_mxn', 'eth_btc']
        r = grequests.map([self.engine.list_fees(books=books)])
        response = self.validate_api_response(r).parsed
        for book in books:
            self.assertIn(book, response)
            fee = response[book]
            self.assertIn('book', fee)
            self.assertIn('fee_decimal', fee)
            self.assertIn('fee_percent', fee)

    def test_get_ticker_last_price(self):
        book = 'btc_mxn'
        r = grequests.map([self.engine.get_ticker_last_price(book=book)])
        response = self.validate_api_response(r).parsed
        self.assertTrue(response)


if __name__ == '__main__':
    # run all tests
    unittest.main()
    # run a specific test
    #suite = unittest.TestSuite()
    #suite.addTest(TestBitsoApi('test_limit_order'))  # Include only the test you want to run
    #unittest.TextTestRunner().run(suite)
