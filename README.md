# coinbasewebsocket-scraper

This is a simple websocket scraper for Coinbase Market Data. 
It will scrape the websocket for all orders (Full channel) and save them to a MongoDB.

## Installation

1. Clone the repository

```bash
git clone https://github.com/kkuette/coinbasewebsocket-scraper.git
```

2. Install the requirements

```bash
pip3 install -r requirements.txt
```

3. Create a conf.json file with the following variables:
    - database: *you can also use a mongodb uri*
        - host: "localhost"
        - port: 27017
    - database_name: "coinbase"  (*Optionnal*)
    - products: ["BTC-USD", "ETH-USD"]

You can also directly use the docker image: kkuette/coinbasewebsocket-scraper
It need a conf.json file to be mounted in the container.

```bash
docker run --rm -v /path/to/conf.json:/secrets kkuette/coinbasewebsocket-scraper:latest
```

## Usage

```bash
python3 coinbasewebsocket-scraper/scraper.py path/to/conf.json
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.





