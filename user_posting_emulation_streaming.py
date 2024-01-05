import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import yaml

random.seed(100)
db_cred_file = './db_cred.yaml'

class AWSDBConnector:

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

invoke_url = "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/pinterest_datas/streams"
pin_invoke_url = f"{invoke_url}/streaming-0a2f66c3e41f-pin/record"
geo_invoke_url = f"{invoke_url}/streaming-0a2f66c3e41f-geo/record"
user_invoke_url = f"{invoke_url}/streaming-0a2f66c3e41f-user/record"

# Function to send data to Kafka topic via API Gateway    
def send_data_to_kafka_topic(url,datas):    
    headers = {'Content-Type': 'application/json'}    
    response = requests.request("PUT", url, headers=headers, data=datas,)
   
    if response.status_code == 200:
        print(response.status_code)
        data = response.json()
        print(data)
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
           
                payload_pin = json.dumps({
                "StreamName": "streaming-0a2f66c3e41f-pin",
                "Data": {
                        "index": pin_result['index'], 
                        "unique_id": pin_result["unique_id"], 
                        "title": pin_result["title"], 
                        "description": pin_result["description"], 
                        "poster_name": pin_result["poster_name"], 
                        "follower_count": pin_result["follower_count"], 
                        "tag_list": pin_result["tag_list"], 
                        "is_image_or_video": pin_result["is_image_or_video"], 
                        "image_src": pin_result["image_src"], 
                        "downloaded": pin_result["downloaded"], 
                        "save_location": pin_result["save_location"], 
                        "category": pin_result["category"]
                        },
                "PartitionKey": "1"})
                
                ######### GEO ##########
                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)
         
                payload_geo = json.dumps({
                "StreamName": "streaming-0a2f66c3e41f-geo",
                "Data":{
                        "index": geo_result["ind"], 
                        "timestamp": geo_result["timestamp"].isoformat(), 
                        "latitude": geo_result["latitude"], 
                        "longitude": geo_result["longitude"],
                        "country": geo_result["country"]
                        },
                "PartitionKey": "2"})

                # Send data to Kafka topics via API Gateway
                send_data_to_kafka_topic(geo_invoke_url, payload_geo)

                ######### USER ##########
                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                
                for row in user_selected_row:
                    user_result = dict(row._mapping)
    
                payload_user = json.dumps({
                "StreamName": "streaming-0a2f66c3e41f-user",
                "Data":  {
                        "index": user_result["ind"], 
                        "first_name": user_result["first_name"], 
                        "last_name": user_result["last_name"], 
                        "age": user_result["age"],
                        "date_joined": user_result["date_joined"].isoformat()},
                "PartitionKey": "3"})
                            
                # Send data to Kafka topics via API Gateway
                send_data_to_kafka_topic(pin_invoke_url,payload_pin)
                send_data_to_kafka_topic(geo_invoke_url,payload_geo)
                send_data_to_kafka_topic(user_invoke_url,payload_user)
                
        except KeyboardInterrupt:
             # Handle a keyboard interrupt (the user presses Ctrl+C)
                print("\nExiting the loop.")
                break  # Exit the loop
                
if __name__ == "__main__":
    try:
        run_infinite_post_data_loop()
        print('Working')
    except Exception as e:
        print('Failed')
        print(e)
