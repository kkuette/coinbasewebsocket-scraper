import json, time

from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException

import logging

class WebsocketClient:
    def __init__(
            self,
            url="wss://ws-feed.exchange.coinbase.com",
            products=None
        ):
        self.url = url
        self.products = products
        self.channels = ["full"]
        self.stop = True
        self.error = None
        self.ws = None
        self.thread = None

    def start(self):
        def _go():
            self._connect()
            self._listen()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.keepalive = Thread(target=self._keepalive)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        sub_params = {'type': 'subscribe', 'channels': self.channels, 'product_ids': self.products}

        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(sub_params))

    def _keepalive(self, interval=30):
        sleep = 0.1
        interval_counter = 0
        while not self.stop:
            interval_counter += sleep
            if interval_counter >= interval:
                self.ws.ping("keepalive")
                logging.debug("keepalive sent")
                interval_counter = 0
            time.sleep(sleep)

    def _listen(self):
        self.keepalive.start()
        while not self.stop:
            try:
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        self.ws.close()
        #self.keepalive.join()

    def close(self):
        self.stop = True   # will only disconnect after next msg recv
        logging.debug("WebsocketClient disconnecting...")
        self._disconnect() # force disconnect so threads can join
        self.on_close()

    def on_open(self):
        logging.info("-- Subscribed! --")

    def on_close(self):
        logging.info("-- Socket Closed --")

    def on_message(self, msg):
        logging.info(msg)

    def on_error(self, e, data=None):
        self.error = e
        logging.error('{} - data: {}'.format(e, data))
        logging.debug("WebsocketClient error, closing...")
        self.close()