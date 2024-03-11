import argparse
import json

configFile = 'arbitrage_config.json'

with open(configFile) as f:
    config = json.load(f)

parser = argparse.ArgumentParser(description='Crypto Arbitrage')
parser.add_argument('-m', '--mode', help='Arbitrage mode: triangular or exchange', required=True)
parser.add_argument('-p', '--production', help='Production mode', action='store_true')
args = parser.parse_args()

engine = None
isMockMode = not args.production

if args.mode == 'triangular':
    from engines.triangular_arbitrage import CryptoEngineTriArbitrage
    engine = CryptoEngineTriArbitrage(config['triangular'], isMockMode)
else:
    print(f'Mode {args.mode} is not recognized')

if engine:
    engine.run()
