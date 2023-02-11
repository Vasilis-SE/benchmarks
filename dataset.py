import os
import json
from types import NoneType

class Dataset:
    
    def __init__(self):
        self.ds = None
        self.name = None
        self.path = None

    def generate_path(self):
        self.set_path('./datasets/{}.json'.format(self.name))
        
    def fetch_ds(self):        
        if not os.path.exists(self.path):
            raise Exception("[Error] No dataset in with the given name...")

        if not os.path.isfile(self.path):
            raise Exception("[Error] Dataset provided is not a file...")

        ds_string = open(self.path)
        self.set_dataset(json.load(ds_string))
    
    # Getters / Setters
    def get_dataset(self) -> json:
        return self.ds
    
    def get_name(self) -> str:
        return self.name
    
    def get_path(self) -> str:
        return self.path
    
    def set_dataset(self, ds) -> NoneType:
        self.ds = ds
    
    def set_name(self, name) -> NoneType:
        self.name = name
    
    def set_path(self, path) -> NoneType:
        self.path = path