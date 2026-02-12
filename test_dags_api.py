
from app import app, db
from models import GeneratedDAG
import json

def test_view_code():
    with app.app_context():
        dag = GeneratedDAG.query.first()
        if not dag:
            print("No DAGs found to test.")
            return

        print(f"Testing View Code for DAG ID: {dag.id}")
        
        with app.test_client() as client:
            response = client.get(f'/api/dags/{dag.id}/code')
            print(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                data = response.get_json()
                print(f"Filename: {data.get('filename')}")
                print(f"Code Length: {len(data.get('code', ''))}")
                print("Success!")
            else:
                print(f"Failed: {response.data}")

if __name__ == "__main__":
    test_view_code()
