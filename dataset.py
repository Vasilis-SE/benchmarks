import os
import json

class Dataset:
    
    def __init__(self, ds_name):
        self.ds = self.fetch_ds(ds_name);

    def fetch_ds(self, ds_name):
        ds_path = './datasets/{}.json'.format(ds_name)
        
        if not os.path.exists(ds_path):
            raise Exception("[Error] No dataset in with the given name...")

        if not os.path.isfile(ds_path):
            raise Exception("[Error] Dataset provided is not a file...")

        ds_string = open(ds_path)
        ds = json.load(ds_string)
        return ds