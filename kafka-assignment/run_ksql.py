import requests
import json
import time

KSQL_ENDPOINT = "http://dataeng-ksql:8088/ksql"

def execute_ksql(query):
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}
    data = {
        "ksql": query,
        "streamsProperties": {}
    }
    
    response = requests.post(KSQL_ENDPOINT, headers=headers, json=data)
    return response.json()

def main():
    # Read KSQL queries
    with open('device_ksql.sql', 'r') as file:
        queries = file.read().split(';')
    
    # Execute each query
    for query in queries:
        if query.strip():
            print(f"Executing query:\n{query}")
            result = execute_ksql(query)
            print(f"Result: {json.dumps(result, indent=2)}")
            time.sleep(2)

if __name__ == "__main__":
    main()