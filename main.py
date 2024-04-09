import json
from engines.triangular_arbitrage import CryptoEngineTriArbitrage
from engines.bitso import ExchangeEngine

configFile = 'arbitrage_config.json'

with open(configFile) as f:
    config = json.load(f)

f = open('arbitrage_config.json')
arbitrage_config = json.load(f)
f.close()
engine = ExchangeEngine(arbitrage_config['url'])
triangular_arb = CryptoEngineTriArbitrage(arbitrage_config, engine, mock=False)
triangular_arb.main_loop()