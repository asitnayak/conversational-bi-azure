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
        # api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries"

        # DAX query remains the same
        # dax_query = "EVALUATE TOPN(10, 'CustomerDimension')" # Replace 'YourTable' with your actual table name

        # The request body must be a JSON payload
        # request_body = {
        #     "queries": [
        #         {
        #             "query": dax_query
        #         }
        #     ]
        # }

        # Set up the headers for the API request
        # headers = {
        #     "Authorization": f"Bearer {access_token}",
        #     "Content-Type": "application/json"
        # }

        # Make the POST request
        # print("Sending DAX query to Power BI REST API...")
        # final_return += f"\nSending DAX query to Power BI REST API..."
        # response = requests.post(api_url, json=request_body, headers=headers)

        res = list_tables_dax_tool()
        final_return += f"\n\nLIST TABLE result :\n{res}"

        res = get_schema_dax_tool('ProductDimension')
        final_return += f"\n\nGET SCHEMA result :\n{res}"

        # --- 4. Process the Response ---
        # if response.status_code == 200:
            # print("✅ DAX query executed successfully!")
            # final_return += f"\n✅ DAX query executed successfully!"
            # response_json = response.json()
            
            # The data is nested in the response JSON
            # rows = response_json['results'][0]['tables'][0]['rows']
            
            # Convert the list of dictionaries (rows) into a pandas DataFrame
            # df = pd.DataFrame(rows)
            # print("Here is your data:")
            # final_return += f"\nHere is your data:"
            # print(df)
            # final_return += f"\n{df}"
        # else:
            # print(f"❌ Error executing query. Status code: {response.status_code}")
            # print(f"Error details: {response.text}")
            # final_return += f"\n❌ Error executing query. Status code: {response.status_code}"
            # final_return += f"\nError details: {response.text}"

        return final_return

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {str(e)}")

# Add a root endpoint for basic health check/info
@app.get("/")
def read_root():
    return {"message": "Welcome to the Conversational Bot API. Use the /conv_bot_v1/{question} endpoint."}


def run_dax_query(dax_query: str) -> pd.DataFrame:
    """
    Securely connects to Power BI, executes a DAX query via the REST API,
    and returns the result as a pandas DataFrame.
    """
    # logging.info(f"Executing DAX query...")
    
    # Securely get credentials from the Function App's Application Settings
    TENANT_ID = os.environ.get("TENANT_ID")
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
    WORKSPACE_ID = "ea2a7d27-23bb-47ed-8dd2-4256a9089c16"
    DATASET_ID = "a5c03290-f848-47bc-b2ab-ebb02a1e8e4d"

    if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_ID, DATASET_ID]):
        raise ValueError("One or more Power BI environment variables are not set.")

    # Authenticate and get an access token using MSAL
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" not in result:
        raise ConnectionError("Failed to acquire access token for Power BI.")
    access_token = result['access_token']

    # Execute the DAX query using the Power BI REST API
    api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/executeQueries"
    request_body = {"queries": [{"query": dax_query}]}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    response = requests.post(api_url, json=request_body, headers=headers)
    response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx or 5xx)

    # Process the response and convert it to a DataFrame
    rows = response.json()['results'][0]['tables'][0]['rows']
    return pd.DataFrame(rows)


def list_tables_dax_tool() -> str:
    """
    Lists all visible tables in the connected Power BI semantic model.
    """
    try:
        # DAX Dynamic Management Views (DMVs) are used to query model metadata.
        # TMSCHEMA_TABLES lists all tables.
        dax_query = "EVALUATE TMSCHEMA_TABLES"
        df = run_dax_query(dax_query)
        # We only care about the table name and filter out system tables
        tables = df[df['Type'] == 'Table']['Name'].tolist()
        return "\n".join(tables) if tables else "No tables found in the model."
    except Exception as e:
        return f"Error retrieving tables: {str(e)}"


def get_schema_dax_tool(table_name: str) -> str:
    """
    Returns the schema (column names and types) for a specified table,
    along with a few sample rows to provide context.
    """
    try:
        # Use TMSCHEMA_COLUMNS DMV to get the schema for a specific table
        dax_schema_query = f"EVALUATE FILTER(TMSCHEMA_COLUMNS, [TableID] IN SELECTCOLUMNS(FILTER(TMSCHEMA_TABLES, [Name] = '{table_name}'), \"ID\", [ID]))"
        schema_df = run_dax_query(dax_schema_query)
        
        if schema_df.empty:
            return f"Table '{table_name}' not found or has no columns."

        schema_lines = [f" {row['Name']} ({row['DataType']})" for index, row in schema_df.iterrows()]
        schema_text = f"Schema for '{table_name}':\n" + "\n".join(schema_lines)

        # Get top 3 sample rows using TOPN
        dax_sample_query = f"EVALUATE TOPN(3, '{table_name}')"
        sample_df = run_dax_query(dax_sample_query)
        
        if not sample_df.empty:
            sample_text = f"\n\nSample rows:\n{sample_df.to_string()}"
        else:
            sample_text = "\n\n(No sample rows found - table may be empty.)"
            
        return schema_text + sample_text
    except Exception as e:
        return f"Error retrieving schema for table '{table_name}': {str(e)}"
