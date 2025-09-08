from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
# import logging
import pandas as pd
from msal import ConfidentialClientApplication
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
# logging.basicConfig(level=logging.INFO)

# Create a FastAPI app instance
# This is the equivalent of `app = func.FunctionApp(...)`
app = FastAPI(
    title="Conversational Bot API",
    description="An API to interact with a conversational agent.",
    version="1.0.0"
)

# ~/Documents/SCALER/Azure Projects/ai_for_bi_webapp_local/.venv/bin/python

@app.get("/conv_bot_v1/{question}", response_class=PlainTextResponse)
def conv_bot_v1(question: str):
    
    # logging.info('FastAPI GET endpoint processed a request.')

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    
    final_return = ""

    try:
        final_return += f"Received : {question}"

        TENANT_ID = os.getenv("TENANT_ID")
        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")
        XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/AgentPOCWorkspace" 
        DATASET_NAME = "conv_data" 

        # --- 2. Authenticate and Get Access Token ---
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        app = ConfidentialClientApplication(
            client_id=CLIENT_ID,
            authority=authority,
            client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=scope)
        if "access_token" not in result:
            raise Exception("Could not get access token: " + result.get("error_description", "Unknown error"))
        access_token = result['access_token']
        # print("✅ Successfully acquired access token.")
        final_return += "\n✅ Successfully acquired access token."

        WORKSPACE_ID = "ea2a7d27-23bb-47ed-8dd2-4256a9089c16"
        DATASET_ID = "a5c03290-f848-47bc-b2ab-ebb02a1e8e4d"

        # --- 3. Execute DAX Query via REST API (The New Part) ---
        api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries"

        # DAX query remains the same
        dax_query = "EVALUATE TOPN(10, 'CustomerDimension')" # Replace 'YourTable' with your actual table name

        # The request body must be a JSON payload
        request_body = {
            "queries": [
                {
                    "query": dax_query
                }
            ]
        }

        # Set up the headers for the API request
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Make the POST request
        # print("Sending DAX query to Power BI REST API...")
        final_return += f"\nSending DAX query to Power BI REST API..."
        response = requests.get(api_url, json=request_body, headers=headers)

        # --- 4. Process the Response ---
        if response.status_code == 200:
            # print("✅ DAX query executed successfully!")
            final_return += f"\n✅ DAX query executed successfully!"
            response_json = response.json()
            
            # The data is nested in the response JSON
            rows = response_json['results'][0]['tables'][0]['rows']
            
            # Convert the list of dictionaries (rows) into a pandas DataFrame
            df = pd.DataFrame(rows)
            # print("Here is your data:")
            final_return += f"\nHere is your data:"
            # print(df)
            final_return += f"\n{df}"
        else:
            # print(f"❌ Error executing query. Status code: {response.status_code}")
            # print(f"Error details: {response.text}")
            final_return += f"\n❌ Error executing query. Status code: {response.status_code}"
            final_return += f"\nError details: {response.text}"

        return final_return

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {str(e)}")

# Add a root endpoint for basic health check/info
@app.get("/")
def read_root():
    return {"message": "Welcome to the Conversational Bot API. Use the /conv_bot_v1/{question} endpoint."}
