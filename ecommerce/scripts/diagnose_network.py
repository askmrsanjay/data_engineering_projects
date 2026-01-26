import socket
import urllib.request
import os

def check_connection(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"SUCCESS: Connected to {host}:{port}")
    except Exception as e:
        print(f"FAILED: Could not connect to {host}:{port} - {e}")

def check_http(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            print(f"SUCCESS: HTTP {url} returned {response.status}")
    except Exception as e:
        print(f"FAILED: HTTP {url} error - {e}")

print("--- Internal Connectivity Check ---")
check_connection("kafka", 9092)
check_connection("ecommerce-minio", 9000)
check_http("http://ecommerce-minio:9001") # Console

print("\n--- Volume Check ---")
path = "/opt/bitnami/spark/app/src/streaming/bronze_streaming.py"
if os.path.exists(path):
    print(f"SUCCESS: {path} exists")
else:
    print(f"FAILED: {path} not found")
