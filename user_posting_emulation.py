import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from pprint import pprint


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

invoke_url =  "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/topics"
# pin_invoke_url = f"{invoke_url}/0a2f66c3e41f.pin"
# geo_invoke_url = f"{invoke_url}/0a2f66c3e41f.geo"
# user_invoke_url = f"{invoke_url}/0a2f66c3e41f.user"

# Function to send data to Kafka topic via API Gateway
def send_data_to_kafka_topic(url,data):
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    response = requests.request("GET", invoke_url, params=data, headers=headers) 
    
    if response.status_code == 200:
        print(f"Data sent successfully to {url}")
    else:
        print(f"Failed to send data to {url}. Status code: {response.status_code}, Response: {response.text}")

    print(response.status_code)

def run_infinite_post_data_loop():
    print('inside run infinite post data loop')
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            # Send data to Kafka topics via API Gateway
            send_data_to_kafka_topic(invoke_url,pin_result)

            # geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            # geo_selected_row = connection.execute(geo_string)
            
            # for row in geo_selected_row:
            #     geo_result = dict(row._mapping)

            # user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            # user_selected_row = connection.execute(user_string)
            
            # for row in user_selected_row:
            #     user_result = dict(row._mapping)
            
            # pprint(pin_result)
            # print("\n")
            # print("\n")

            # {'category': 'mens-fashion',
            # 'description': 'No description available Story format',
            # 'downloaded': 0,
            # 'follower_count': 'User Info Error',
            # 'image_src': 'Image src error.',
            # 'index': 7528,
            # 'is_image_or_video': 'multi-video(story page format)',
            # 'poster_name': 'User Info Error',
            # 'save_location': 'Local save in /data/mens-fashion',
            # 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e',
            # 'title': 'No Title Data Available',
            # 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf'}

            # pprint(geo_result)
            # print("\n")
            # print("\n")

            # {'country': "Cote d'Ivoire",
            # 'ind': 2923,
            # 'latitude': -84.6302,
            # 'longitude': -164.507,
            # 'timestamp': datetime.datetime(2019, 9, 8, 22, 53, 9)}

            # pprint(user_result)
            # print("\n")
            # print("\n")
            
            # {'age': 36,
            # 'date_joined': datetime.datetime(2015, 12, 8, 20, 2, 43),
            # 'first_name': 'Rachel',
            # 'ind': 5730,
            # 'last_name': 'Davis'}


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


