import requests
import json
from requests.auth import HTTPBasicAuth
import time

# Airflow API configuration
AIRFLOW_BASE_URL = "http://localhost:8080/api/v1"
USERNAME = "airflow"
PASSWORD = "airflow"

def test_airflow_api():
    """Test basic Airflow API connectivity and authentication"""
    
    # Test 1: Health check endpoint
    print("Test 1: Checking Airflow health...")
    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/health",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=10
        )
        if response.status_code == 200:
            print("[OK] Health check passed")
            print(f"  Response: {response.json()}")
        else:
            print(f"[ERROR] Health check failed with status {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except Exception as e:
        print(f"[ERROR] Health check failed with exception: {e}")
        return False
    
    # Test 2: Get DAGs
    print("\nTest 2: Getting DAG list...")
    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/dags",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=10
        )
        if response.status_code == 200:
            print("[OK] DAG list retrieved successfully")
            dags = response.json()
            print(f"  Found {dags.get('total_entries', 0)} DAGs")
            if dags.get('dags'):
                print(f"  Sample DAG: {dags['dags'][0]['dag_id'] if dags['dags'] else 'None'}")
        else:
            print(f"[ERROR] Failed to get DAGs with status {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except Exception as e:
        print(f"[ERROR] DAG list failed with exception: {e}")
        return False
        
    # Test 3: Get specific DAG
    print("\nTest 3: Getting specific DAG...")
    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/dags/test_api_dag",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=10
        )
        if response.status_code == 200:
            print("[OK] Specific DAG retrieved successfully")
            dag = response.json()
            print(f"  DAG ID: {dag.get('dag_id')}")
            print(f"  Is Paused: {dag.get('is_paused')}")
        else:
            print(f"[ERROR] Failed to get specific DAG with status {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except Exception as e:
        print(f"[ERROR] Specific DAG failed with exception: {e}")
        return False
    
    print("\n[OK] All tests passed! Airflow API is working correctly.")
    return True

def wait_for_airflow(max_retries=30, delay=5):
    """Wait for Airflow to be ready"""
    print("Waiting for Airflow to be ready...")
    
    for i in range(max_retries):
        try:
            response = requests.get(
                f"{AIRFLOW_BASE_URL}/health",
                timeout=5
            )
            if response.status_code == 200:
                health = response.json()
                if health.get('metadatabase', {}).get('status') == 'healthy':
                    print("[OK] Airflow is ready!")
                    return True
        except Exception:
            pass
            
        print(f"Airflow not ready yet, retrying in {delay}s... ({i+1}/{max_retries})")
        time.sleep(delay)
    
    print("[ERROR] Airflow did not become ready in time")
    return False

if __name__ == "__main__":
    print("Airflow API Test Script")
    print("==============================")
    
    # Wait for Airflow to be ready
    if not wait_for_airflow():
        exit(1)
    
    # Give Airflow time to fully start
    print("Waiting for Airflow to fully start...")
    time.sleep(10)
    
    # Run API tests
    success = test_airflow_api()
    
    if success:
        print("\n[SUCCESS] All tests completed successfully!")
    else:
        print("\n[FAILURE] Some tests failed. Please check your Airflow setup.")
        exit(1)