import logging

import pymongo

from common import parse_config

logger = logging.getLogger('pastehunter')

config = parse_config()


class MongoOutput:
    def __init__(self):
        self.mongo_cli = pymongo.MongoClient()['pasteHunter']  # FIXME: should read from config file.

    def store_paste(self, paste_data):
        if not config['outputs']['json_output']['store_raw']:
            del paste_data['raw_paste']
        self.mongo_cli['data'].insert_one(paste_data)
        del paste_data['_id']  # insert_one will add a _id here.
