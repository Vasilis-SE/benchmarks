import requests
import json

class DragonflyBenchmarkClass: 
    
    def __init__(self):
        self.base_url = "http://localhost:6379/v1/kv/" 
        
    def add_data(self, key, value):
        data = {"value": value}
        headers = {"Content-type": "application/json"}
        url = self.base_url + key
                
        print(url)
        response = requests.request('PUT', url, headers=headers, data=data)
        return response
    
    def retrieve_data(self, key):
        url = self.base_url + key
        response = requests.get(url)
        return response

    def update_data(self, key, value):
        data = {"value": value}
        headers = {"Content-type": "application/json"}
        url = self.base_url + key
        response = requests.put(url, json=data, headers=headers)
        return response

    def delete_data(self, key):
        url = self.base_url + key
        response = requests.delete(url)
        return response