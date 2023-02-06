from datetime import datetime
from pymongo import MongoClient

class Data:

    def __init__(self, pipe, db_conf, database, collection, batch_size):
        self.pipe = pipe
        if type(db_conf) == dict:
            self.client = MongoClient(**db_conf)
        elif type(db_conf) == str:
            self.client = MongoClient(db_conf)
        self.database = database
        self.collection = collection
        self.batch_size = batch_size
        self.data = []
        self.listen()

    def clean_data(self, data):
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
        return data

    def listen(self):
        while True:
            data = self.pipe.recv()
            data = self.clean_data(data)
            if self.batch_size == 1:
                self.client[self.database][self.collection].insert_one(data)
            else:
                self.data.append(data)
                if len(self.data) == self.batch_size:
                    self.client[self.database][self.collection].insert_many(self.data)
                    self.data = []
    