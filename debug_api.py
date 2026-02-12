import requests
import json

try:
    response = requests.get('http://127.0.0.1:5000/api/template-variables')
    print(f"Status Code: {response.status_code}")
    try:
        data = response.json()
        print("Response JSON:")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print("Failed to parse JSON")
        print("Response Text:")
        print(response.text)
except Exception as e:
    print(f"Request failed: {e}")
