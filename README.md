# Bitso API triangular arbitrage

Automated trading bot that detects triangular arbitrage opportunities on the Bitso exchange for BTC/ETH/MXN routes

### Configuration

Add your keys in `keys` folder

```
{
  "public": "publickey",
  "private": "privatekey"
}
```

Edit the configuration `json` file to specify key file and to either use Bitso stage url or Bitso prod url

```
{
  "keyFile": "keys/bitso.key",
  "url":"https://bitso.com/api"
}
```

### Run

To run the bot:

```
python main.py
```