import json
from engines.triangular_arbitrage import CryptoEngineTriArbitrage
from engines.bitso import ExchangeEngine
import argparse

configFile = 'arbitrage_config.json'

with open(configFile) as f:
    config = json.load(f)


parser = argparse.ArgumentParser(description="Run functions based on the command line arguments.")
parser.add_argument('--prod', action='store_true', help="Run in production mode")
args = parser.parse_args()

f = open('arbitrage_config.json')
arbitrage_config = json.load(f)
f.close()
if args.prod:
    print("ENV: prod")
    engine = ExchangeEngine(arbitrage_config['url'])
    engine.load_key(config['keyFile'])
    triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, engine)
else:
    print("ENV: test")
    engine = ExchangeEngine(arbitrage_config['test_url'])
    engine.load_key(config['test_keyFile'])
    triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, engine)
triangular_arb.main_loop()