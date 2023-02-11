from multiprocessing import Pipe, Process, cpu_count

from data import Data
from websocketclient import WebsocketClient
from collections import deque

class Scraper(WebsocketClient):

    def __init__(self, db_conf, database, batch_size=100, products=None):
        self.db_conf = db_conf
        self.database = database
        self.products = products
        self.pipes = {}
        self.batch_size = batch_size
        self.messages = {}
        self.average_messages = {}
        self.processes = []
        self.max_processes = cpu_count() if cpu_count() < len(self.products) else len(self.products)
        super().__init__(products=self.products)

    def create_processes(self):
        logging.debug("Creating {} processes...".format(self.max_processes))
        for process in range(self.max_processes):
            snd, rcv = Pipe()
            self.pipes.update(dict.fromkeys(self.products[process::self.max_processes], snd))
            self.messages.update(dict.fromkeys(self.products[process::self.max_processes], deque([0], maxlen=60)))
            self.average_messages.update(dict.fromkeys(self.products[process::self.max_processes], 0))
            self.processes.append(Process(target=Data, args=(rcv, self.db_conf, self.database, self.products[process::self.max_processes], self.batch_size)))
            self.processes[-1].start()

    def on_message(self, msg):
        if self.stop:
            return
        if 'product_id' in msg:
            self.messages[msg['product_id']][-1] += 1
            self.pipes[msg['product_id']].send(msg)
            if time.time() - self.time > 1:
                logging.info("Total messages received: {}".format(sum([value[-1] for value in self.messages.values()])))
                for product in self.products:
                    self.average_messages[product] = int(sum(self.messages[product]) / len(self.messages[product]))
                    self.messages[product].append(0)
                self.time = time.time()

    def close(self):
        logging.debug("Terminating processes...")
        for process in self.processes:
            process.terminate()
        super().close()
        logging.debug("Scraper closed...")

    def start(self):
        logging.debug("Starting scraper...")
        self.create_processes()
        logging.debug("Processes created...")
        self.time = time.time()
        super().start()
        logging.debug("Scraper started...")
        while not self.stop:
            time.sleep(1)
        if self.error:
            logging.debug("Restarting scraper...")
            self.start()
            self.error = None

if __name__ == "__main__":
    import logging, json, sys, time
    conf = 'secrets/conf.json' if len(sys.argv) == 1 else sys.argv[1]
    with open(conf) as fp:
        full_conf = json.load(fp)
        database_conf = full_conf['database']
        db_name = full_conf['database_name'] if 'database_name' in full_conf else 'coinbase'
        product_conf = full_conf['products']
        fp.close()

    # Set logging configuration

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8', level=logging.DEBUG)

    scraper = Scraper(database_conf, db_name, products=product_conf)
    
    try:
        scraper.start()
        scraper.close()
        sys.exit(0)
    except KeyboardInterrupt:
        scraper.close()
        sys.exit(1)
    
    