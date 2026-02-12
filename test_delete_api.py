
from app import app, db
from models import GeneratedDAG
import json

def test_delete_dag():
    with app.app_context():
        # Test deleting a non-existent DAG
        fake_id = 99999
        print(f"Testing Delete for non-existent DAG ID: {fake_id}")
        
        with app.test_client() as client:
            response = client.delete(f'/api/dags/{fake_id}')
            print(f"Status Code: {response.status_code}")
            
            try:
                data = response.get_json()
                print(f"Response: {data}")
            except:
                print(f"Raw Response: {response.data[:200]}...") # Print start of response to see if HTML

if __name__ == "__main__":
    test_delete_dag()
