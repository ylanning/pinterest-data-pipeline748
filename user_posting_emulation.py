import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import ast
import yaml

random.seed(100)
db_cred_file = './db_cred.yaml'

class AWSDBConnector:

    def __init__(self):

        def __init__(self):
            self.cred = self.read_db_creds(db_cred_file)
            self.HOST = self.cred['HOST']
            self.USER = self.cred['USER']
            self.PASSWORD = self.cred['PASSWORD']
            self.DATABASE = self.cred['DATABASE']
            self.PORT = self.cred['PORT']
            self.pin_result = {}
            self.geo_result = {}
            self.user_result = {}
        
    def read_db_creds(self, db_cred_file):
        try:
            with open(db_cred_file, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File {db_cred_file} not found.")
            # Handle the exception as needed
        except yaml.YAMLError:
            print(f"Error: Invalid YAML format in {db_cred_file}.")
            # Handle the exception as needed
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

invoke_url =  "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/pinterest/topics"
pin_invoke_url = f"{invoke_url}/0a2f66c3e41f.pin"
geo_invoke_url = f"{invoke_url}/0a2f66c3e41f.geo"
user_invoke_url = f"{invoke_url}/0a2f66c3e41f.user"

# Function to send data to Kafka topic via API Gateway

def datetime_serializer(obj):
    if isinstance(obj,datetime):
        return obj.isoformat()
    raise TypeError("Type not serializeable")
    
def send_data_to_kafka_topic(url,datas):    
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}    
    response = requests.request("POST", url, data=datas, headers=headers)
    
    if response.status_code == 200:
        print(f"Data sent successfully to {url}")
    else:
        print(f"Failed to send data to {url}. Status code: {response.status_code}, Response: {response.text}")

def run_infinite_post_data_loop():
    while True:
        try:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = new_connector.create_db_connector()

            with engine.connect() as connection:
                ######### PIN ##########
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)
          
                payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas
                    "value": {
                            "index": pin_result["index"], 
                            "category": pin_result["category"], 
                            "downloaded": pin_result["downloaded"], 
                            "follower_count": pin_result["follower_count"],
                            "image_src": pin_result["image_src"],
                            "is_image_or_video": pin_result["is_image_or_video"],
                            "poster_name": pin_result["poster_name"],
                            "save_location": pin_result["save_location"],
                            "tag_list": pin_result["tag_list"],
                            "title": pin_result["title"],
                            "unique_id": pin_result["unique_id"]}
                    }]})

                # Send data to Kafka topics via API Gateway
                send_data_to_kafka_topic(pin_invoke_url,payload)

                ######### GEO ##########
                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)

                payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas
                    "value": {"index": geo_result["ind"], 
                            "timestamp": geo_result["timestamp"].isoformat(), 
                            "latitude": geo_result["latitude"], 
                            "longitude": geo_result["longitude"],
                            "country": geo_result["country"]}
                    }]})
                    
                # Send data to Kafka topics via API Gateway
                send_data_to_kafka_topic(geo_invoke_url,payload)

                ######### USER ##########
                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                
                for row in user_selected_row:
                    user_result = dict(row._mapping)
                                    
                payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas
                    "value": {"index": user_result["ind"], 
                            "first_name": user_result["first_name"], 
                            "last_name": user_result["last_name"], 
                            "age": user_result["age"],
                            "date_joined": user_result["date_joined"].isoformat()}
                    }]})

                # Send data to Kafka topics via API Gateway
                send_data_to_kafka_topic(user_invoke_url,payload)
                
        except KeyboardInterrupt:
             # Handle a keyboard interrupt (the user presses Ctrl+C)
                print("\nExiting the loop.")
                break  # Exit the loop
                

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
