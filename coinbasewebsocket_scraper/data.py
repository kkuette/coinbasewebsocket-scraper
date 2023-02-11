from datetime import datetime
from pymongo import MongoClient
from threading import Thread, Event
from copy import deepcopy
import logging

class Data:

    def __init__(self, pipe, db_conf, database, collections, batch_size):
        self.pipe = pipe
        if type(db_conf) == dict:
            self.client = MongoClient(**db_conf)
        elif type(db_conf) == str:
            self.client = MongoClient(db_conf)
        self.database = database
        self.collections = collections
        self.batch_size = batch_size
        self.data = { collection:[] for collection in self.collections}
        self.listen()

    def create_threads(self):
        self.threads = {}
        self.events = {}
        for collection in self.collections:
            logging.debug("Creating thread for collection: {}".format(collection))
            if collection not in self.client[self.database].list_collection_names():
                logging.debug("Creating collection: {}".format(collection))
                self.client[self.database].create_collection(collection)
            self.events[collection] = Event()
            self.threads[collection] = Thread(target=self._send, args=(collection, ))
            self.threads[collection].start()
            
    def clean_data(self, data):
        product_id = data['product_id']
        del data['product_id']
        data['time'] = datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
        if 'price' in data:
            data['price'] = float(data['price'])
        if 'size' in data:
            data['size'] = float(data['size'])
        if 'remaining_size' in data:
            data['remaining_size'] = float(data['remaining_size'])
        if 'funds' in data:
            data['funds'] = float(data['funds'])
        return data, product_id

    def _send(self, product_id):
        while True:
            self.events[product_id].wait()
            self.events[product_id].clear()
            data = deepcopy(self.data[product_id])
            if self.batch_size == 1:
                self.client[self.database][product_id].insert_one(data)
            else:
                self.data[product_id] = []
                self.client[self.database][product_id].insert_many(data)
                

    def listen(self):
        self.create_threads()
        while True:
            data = self.pipe.recv()
            data, product_id = self.clean_data(data)
            if self.batch_size == 1:
                self.data[product_id] = data
                if not self.events[product_id].is_set():
                    self.events[product_id].set()
            else:
                self.data[product_id].append(data)
                if len(self.data[product_id]) >= self.batch_size:
                    if not self.events[product_id].is_set():
                        self.events[product_id].set()
    