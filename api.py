import boto3
import time
from fastapi import FastAPI, HTTPException

app = FastAPI()

AWS_REGION = "us-east-1"
S3_OUTPUT_LOCATION = "s3://target-bucket-valentin/queries_result/"
DATABASE_NAME = "salesdata"

athena_client = boto3.client('athena', region_name=AWS_REGION)


def execute_athena_query(query: str):
    """Exécuter une requête sur Athena et récupérer les résultats."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE_NAME},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
        )
        query_execution_id = response['QueryExecutionId']

        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status_state = status['QueryExecution']['Status']['State']
            if status_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if status_state != 'SUCCEEDED':
            raise Exception(f"Query failed with state: {status_state}")

        # Récupérer les résultats
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def format_results(athena_results):
    """Formater les résultats Athena en une liste propre."""
    rows = athena_results['ResultSet']['Rows']
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = [
        {headers[i]: col.get('VarCharValue', None) for i, col in enumerate(row['Data'])}
        for row in rows[1:]
    ]
    return data


@app.get("/query")
def run_query(query: str):
    """Endpoint pour exécuter une requête Athena."""
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter is required")

    try:
        raw_results = execute_athena_query(query)
        formatted_results = format_results(raw_results)
        return {"query": query, "results": formatted_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))