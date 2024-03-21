import hashlib
import json
import hmac
import time
import unittest
from datetime import datetime, timedelta
import calendar
import grequests
from base import ExchangeEngineBase
from urllib.parse import urlparse, urlencode
import requests

class ExchangeEngine(ExchangeEngineBase):
    def __init__(self):
        self.API_URL = 'https://stage.bitso.com/api'
        self.apiVersion = 'v3'
        self.feeRatio = 0.0026
        self.sleepTime = 5
        self.async_ = True  # 'async' is a keyword in Python 3, so rename it to 'async_'
        self.debug = True

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
            'Authorization' : f'Bitso {public}:{nonce}:{signature}',
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
        
        headers.update(self._sign_request(url, httpMethod, body, params))
        
        args = {'params' : params, 'headers': headers}
        if body:
            args.update({'json':body})
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
    
    def place_order(self, body):
        return self._send_request('orders','POST', body)

    def get_balance(self, tickers=[]):
        return self._send_request('balance', 'GET', {}, {}, [self.hook_getBalance(tickers=tickers)])
    
    def list_order_book(self, book):
        return self._send_request('order_book','GET', {}, {'book' : book, 'aggregate': True})

    def get_ticker(self, symbol):
        return self._send_request('ticker', 'GET', {}, {'book' : symbol})

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
                    'fees' :  book['fees']['flat_rate'] ,
                    'minimum_price' : book['minimum_price'],
                    'default_chart' : book['default_chart']
                }
        return res_hook

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
                r.parsed[ticker['currency']] = ticker['available']

        return res_hook

    def get_ticker_orderBook_innermost(self, ticker):
        return self._send_request(f'public/Depth?pair={ticker}&count=1', 'GET', {}, self.hook_orderBook)

    def hook_orderBook(self, r, *r_args, **r_kwargs):
        json_data = r.json()
        ticker = next(iter(json_data['result']))
        result = json_data['result'][ticker]
        r.parsed = {
            'bid': {
                'price': float(result['bids'][0][0]),
                'amount': float(result['bids'][0][1])
            },
            'ask': {
                'price': float(result['asks'][0][0]),
                'amount': float(result['asks'][0][1])
            }
        }

    def get_open_order(self):
        return self._send_request('private/OpenOrders', 'POST', {}, self.hook_openOrder)

    def hook_openOrder(self, r, *r_args, **r_kwargs):
        json_data = r.json()
        r.parsed = []
        for order in json_data['result']:
            r.parsed.append({'orderId': str(order['OrderUuid']), 'created': order['Opened']})

    def cancel_order(self, orderID):
        return self._send_request('private/CancelOrder', 'POST', {'txid': orderID})

    def withdraw(self, ticker, withdrawalKey, amount):
        return self._send_request('private/Withdraw', 'POST', {'asset': ticker, 'key': withdrawalKey, 'amount': amount})

    def get_ticker_lastPrice(self, ticker):
        return self._send_request(f'public/Ticker?pair={ticker}ZUSD', 'GET', {}, [self.hook_lastPrice(ticker=ticker)])

    def hook_lastPrice(self, *factory_args, **factory_kwargs):
        def res_hook(r, *r_args, **r_kwargs):
            json_data = r.json()
            r.parsed = {}
            r.parsed[factory_kwargs['ticker']] = float(next(iter(json_data['result'].values()))['c'][0])

        return res_hook

    def get_ticker_history(self, ticker, timeframe='1'):
        since = calendar.timegm((datetime.utcnow() - timedelta(hours=1)).timetuple())
        return self._send_request(f'public/OHLC?pair={ticker}&interval={timeframe}&since={since}', 'GET')

    def parseTickerData(self, ticker, tickerData):
        vwapIndex = 5
        for key in tickerData['result'].keys():
            if isinstance(tickerData['result'][key], list):
                return {'exchange': self.key['exchange'], 'ticker': ticker,
                        'data': list(map(lambda x: {'price': x[vwapIndex]}, tickerData['result'][key]))}


class TestBitsoApi(unittest.TestCase):

    def setUp(self) -> None:
        self.engine = ExchangeEngine()
        self.engine.load_key('keys/bitso_stage.key')
        return super().setUp()

    
    def test_get_balance_all(self):
        for res in grequests.map([self.engine.get_balance()]):
            self.assertIsNotNone(res.parsed)
            self.assertGreater(len(res.parsed), 0)
            self.assertTrue(res.parsed)
            #print(json.dumps(res.parsed, indent=4))

    
    def test_get_balance_tickers(self):
        for res in grequests.map([self.engine.get_balance(tickers=["usd","eth","btc"])]):
            self.assertIsNotNone(res.parsed)
            self.assertGreater(len(res.parsed), 0)
            self.assertTrue(res.parsed)
            #print(json.dumps(res.parsed, indent=4))

    
    def test_list_available_books(self):
        self.get_books()

    
    def test_list_available_books_symbol(self):
        self.get_books(books = ['eth_mxn', 'eth_btc', 'btc_mxn'])

    def get_books(self, books = None):
        res = grequests.map([self.engine.get_available_books(books=books)])
        self.assertIsNotNone(res)
        self.assertGreater(len(res), 0)
        self.assertTrue(res)
        parsed = res[0].parsed
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed)  
        if books:
            for book in books:
                self.assertIn(book, parsed)      
        return parsed
    
    def get_ticker(self, symbol):
        ticker_response = grequests.map([self.engine.get_ticker(symbol=symbol)])
        self.assertIsNotNone(ticker_response)
        self.assertGreater(len(ticker_response), 0)
        ticker_data = ticker_response[0].json()['payload']
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
        order_book_response = grequests.map([self.engine.list_order_book(book=book)])
        self.assertIsNotNone(order_book_response)
        self.assertGreater(len(order_book_response), 0)
        response = order_book_response[0]
        self.assertEqual(response.status_code, 200)
        order_book = response.json()['payload']
        self.assertTrue(order_book)
        self.assertIn('bids', order_book)
        self.assertIn('asks', order_book)
        return order_book
    
    def test_place_order(self):
        order = {
            'book':'btc_mxn',
            'minor':200,
            'type':'market',
            'side':'buy'
        }
        response = grequests.map([self.engine.place_order(order)])
        self.assertIsNotNone(response)
        self.assertGreater(len(response), 0)
        response = response[0]
        self.assertEqual(response.status_code, 200)
        print(response)

if __name__ == '__main__':
    unittest.main()