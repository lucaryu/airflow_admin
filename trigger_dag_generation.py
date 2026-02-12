import requests

url = 'http://127.0.0.1:5000/api/dags/generate'
data = {'template_id': '1', 'mapping_ids': [3]}

try:
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"Error: {e}")
