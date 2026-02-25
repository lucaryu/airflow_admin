import requests
import json
import time

url = "http://127.0.0.1:5000/api/run_code"
payload = {
    "code": "print('hello jupyter')\nx=10"
}

print("Testing simple execution...")
# Add some sleep to let the Kernel spin up completely if it's the first hit
response = requests.post(url, json=payload)
print(response.json())

print("Testing statefulness...")
payload2 = {
    "code": "print(x*2)"
}
response2 = requests.post(url, json=payload2)
print(response2.json())

print("Testing Airflow import...")
payload3 = {
    "code": "from airflow.hooks.base import BaseHook\ntry:\n  c = BaseHook.get_connection('fake')\nexcept ValueError as e:\n  print(f'Expected error: {e}')"
}
response3 = requests.post(url, json=payload3)
print(response3.json())
