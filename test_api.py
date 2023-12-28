import requests
import json

example_df = {"index": 1, "name": "Maya", "age": 25, "role": "engineer"}

# invoke_url = "https://YourAPIInvokeURL/YourDeploymentStage/topics/YourTopicName"
# invoke_url = "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/topics/0a2f66c3e41f.pin"
# invoke_url = "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/test/topics/0a2f66c3e41f.pin"
invoke_url = "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/pinterest/topics/0a2f66c3e41f.pin"
invoke_url = "https://uf6d6cgu5e.execute-api.us-east-1.amazonaws.com/pinterest/topics//0a2f66c3e41f.pin"

#To send JSON messages you need to follow this structure
payload = json.dumps({
    "records": [
        {
        #Data should be send as pairs of column_name:value, with different columns separated by commas
        "value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
        }
    ]
})

headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
response = requests.request("POST", invoke_url, headers=headers, data=payload)


if response.status_code == 200:
    print(f"Data sent successfully to {invoke_url}")
else:
    print(f"Failed to send data to {invoke_url}. Status code: {response.status_code}, Response: {response.text}")
