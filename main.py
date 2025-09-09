from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
# import logging
import pandas as pd
from msal import ConfidentialClientApplication
import requests
import os
from dotenv import load_dotenv
import json
import io
import base64
import plotly.express as px
from typing import Literal
import time

from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from langgraph.graph import MessagesState
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from langgraph.checkpoint.memory import MemorySaver

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
    final_return = ""

    try:
        question = req.route_params.get('question')
        if not question:
            return func.HttpResponse("Did not receive any question.", status_code=400)

        my_bot = get_bot()

        config = {"configurable": {"user_id": "user_123", "thread_id": str(time.time())}}
        response = my_bot.invoke({"messages": [HumanMessage(content=question)]}, config=config)
        
        final_message = response['messages'][-1].content

        try:
            # If it's a plot, return as JSON
            json_response = json.loads(final_message)
            return func.HttpResponse(json.dumps(json_response), status_code=200, mimetype="application/json")
        except json.JSONDecodeError:
            # If it's plain text, return as plain text
            return func.HttpResponse(final_message, status_code=200, mimetype="text/plain")
  
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"conv_bot_v1 - An internal error occurred: {str(e)}")

# Add a root endpoint for basic health check/info
@app.get("/")
def read_root():
    return {"message": "Welcome to the Conversational Bot API. Use the /conv_bot_v1/{question} endpoint."}


def get_llm():
    # IMPORTANT: Replace with your actual LLM credentials from environment variables
    CHAT_KEY = os.environ.get("OPENAI_API_KEY")
    return ChatOpenAI(model="gpt-5-mini", temperature=0, max_retries=3, api_key=CHAT_KEY)


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


@tool
def list_tables_dax_tool() -> str:
    """
    Lists all visible tables in the connected Power BI semantic model.
    """
    try:
        # DAX Dynamic Management Views (DMVs) are used to query model metadata.
        # TMSCHEMA_TABLES lists all tables.
        dax_query = '''EVALUATE
    SELECTCOLUMNS(
        FILTER(
            INFO.VIEW.TABLES(),
            [IsHidden] = FALSE
        ),
        "Table Name", [Name],
        "Description", [Description]
    )'''
        df = run_dax_query(dax_query)
        df = df.to_string(index=False)
        # We only care about the table name and filter out system tables
        # tables = df[df['Type'] == 'Table']['Name'].tolist()
        return df
    except Exception as e:
        return f"list_tables_dax_tool - Error retrieving tables - from EXCEPT block"


@tool
def get_schema_dax_tool(table_name: str) -> str:
    """
    Returns the schema (column names and types) for a specified table,
    along with a few sample rows to provide context.
    """
    try:
        # Use TMSCHEMA_COLUMNS DMV to get the schema for a specific table
        dax_schema_query = f'''EVALUATE
                            SELECTCOLUMNS(
                                FILTER(
                                    INFO.VIEW.COLUMNS(),
                                    [Table] = "{table_name}" && [IsHidden] = FALSE
                                ),
                                "Column Name", [Name],
                                "Data Type", [DataType],
                                "Description", [Description],
                                "Is Key", [IsKey],
                                "Is Nullable", [IsNullable]
                            )'''
        schema_df = run_dax_query(dax_schema_query)

        if schema_df.empty:
            return f"Table '{table_name}' not found or has no columns."

        # schema_lines = [f" {row['Name']} ({row['DataType']})" for index, row in schema_df.iterrows()]
        schema_text = f"Schema for '{table_name}':\n" + schema_df.to_string(index=False)

        # Get top 3 sample rows using TOPN
        dax_sample_query = f"EVALUATE TOPN(3, '{table_name}')"
        sample_df = run_dax_query(dax_sample_query)

        if not sample_df.empty:
            sample_text = f"\n\nSample rows:\n{sample_df.to_string(index=False)}"
        else:
            sample_text = "\n\n(No sample rows found - table may be empty.)"

        return schema_text + sample_text
    except Exception as e:
        print(f"get_schema_dax_tool - Error retrieving schema for table '{table_name}'")


@tool
def dax_query_checker_tool(dax_query: str) -> str:
    """
    Checks if the DAX query is syntactically valid and uses best practices.
    This does not execute the query—it only asks an LLM to review it.
    """
    llm = get_llm()
    query_check_prompt = """You are an expert DAX analyst and Power BI performance specialist with deep knowledge of DAX syntax, optimization, and best practices. You will *not* execute the query; you will only analyze it statically and fix issues if found.

    Thoroughly analyze the provided DAX query across these critical areas:

## SYNTAX VALIDATION
- Verify query starts with EVALUATE statement
- Check table/column references use correct syntax: 'Table Name'[Column Name] (single quotes for table names with spaces)
- Validate function names, parentheses matching, and parameter syntax
- Ensure proper use of VAR/RETURN statements and variable declarations
- Check for missing commas, semicolons, or incorrect operators

## FUNCTION USAGE & BEST PRACTICES
- CALCULATE vs FILTER: Ensure CALCULATE is used for context modification, FILTER for table filtering
- Use SELECTEDVALUE() instead of VALUES() when expecting single values
- Use ISBLANK() instead of = BLANK() for blank checks
- Use DIVIDE() instead of "/" for division to handle divide-by-zero
- Use COUNTROWS() instead of COUNT() for better performance
- Prefer KEEPFILTERS() over FILTER() for maintaining filter context
- Use DISTINCT() vs VALUES() consistently based on blank handling needs
- When you use SUMMARIZECOLUMNS(), you cannot reference the original table columns in the TOPN function, because SUMMARIZECOLUMNS() creates a new table and hence you need to reference the columns from the new table structure created by SUMMARIZECOLUMNS().

## PERFORMANCE OPTIMIZATION
- Identify inefficient patterns like nested CALCULATE functions
- Check for unnecessary ALL() functions on entire tables (use specific columns instead)
- Flag expensive operations in iterators (SUMX, FILTER, etc.)
- Recommend variables for repeated expressions to avoid recalculation
- Suggest TREATAS over INTERSECT/FILTER for virtual relationships

## CODE QUALITY & READABILITY
- Verify meaningful variable names and proper indentation
- Check for reusable measure patterns vs redundant calculations
- Ensure fully qualified column references: Table[Column]
- Validate logical structure and flow of complex calculations

## COMMON PITFALLS
- Context transition issues in calculated columns vs measures
- Incorrect aggregation functions for the intended calculation
- Missing error handling for edge cases (empty tables, no data scenarios)
- Improper use of time intelligence functions without proper date tables

## SECURITY & DATA INTEGRITY
- Check for potential injection vulnerabilities in dynamic expressions
- Validate proper handling of sensitive data filtering

**INSTRUCTIONS:**
1. If you find ANY issues, provide the corrected query with clear explanations of changes made.
2. If the query is correct, return the original query itself.
3. Always explain your reasoning for any modifications.
4. Prioritize critical syntax errors, then performance issues, then style improvements.
5. For complex queries, break down your analysis by section.

    If there are any mistakes, rewrite the query to be correct. If there are no mistakes, just return the original query.
    """
    messages = [
        SystemMessage(content=query_check_prompt),
        HumanMessage(content=f"Please review the following DAX query:\n\n{dax_query}")
    ]
    response = llm.invoke(messages)
    return response.content


@tool
def run_dax_query_tool(query: str) -> str:
    """
    Executes a DAX query against the Power BI semantic model and returns the result.
    The query MUST start with 'EVALUATE'.
    """
    if not query.strip().upper().startswith("EVALUATE"):
        return "Error: DAX queries must start with the 'EVALUATE' keyword."
    try:
        df = run_dax_query(query)
        return df.to_string(index=False)
    except Exception as e:
        return f"DAX query failed: {str(e)}. Please check the query and try again."


@tool
def query_and_plot_dax_tool(dax_query: str, chart_type: Literal["bar", "pie", "line", "scatter", "histogram"], title: str) -> str:
    """
    Executes a DAX query, then generates an interactive HTML plot from the data.
    Returns a JSON object containing a message and the path to the saved plot file.
    Use this when a user explicitly asks for a 'plot', 'chart', 'graph' or 'visualization'.
    """
    try:
        df = run_dax_query(dax_query)
        if df.empty or len(df.columns) < 1:
            return json.dumps({"message": "Query returned no data or insufficient columns for a plot."})

        # Define the path for saving plots. Create the directory if it doesn't exist.
        plots_dir = "plots"
        os.makedirs(plots_dir, exist_ok=True)
        
        timestamp = int(time.time())
        file_name = f"plot_{timestamp}.html"
        file_path = os.path.join(plots_dir, file_name)

        fig = None
        if chart_type == "bar":
            fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=title, template="plotly_white")
        elif chart_type == "pie":
            fig = px.pie(df, names=df.columns[0], values=df.columns[1], title=title)
        elif chart_type == "line":
            fig = px.line(df, x=df.columns[0], y=df.columns[1], title=title, template="plotly_white")
        elif chart_type == "scatter":
            fig = px.scatter(df, x=df.columns[0], y=df.columns[1], title=title, template="plotly_white")
        elif chart_type == "histogram":
            fig = px.histogram(df, x=df.columns[0], title=title, template="plotly_white")
        else:
            return json.dumps({"message": f"Chart type '{chart_type}' is not supported."})
            
        # Save the interactive plot as an HTML file
        if fig:
            fig.write_html(file_path)
            
            # The front-end will need to construct the full URL to this file
            # based on the web app's domain. We return the relative path.
            response = {
                "message": f"Successfully generated an interactive {chart_type} chart titled '{title}'.",
                "plot_path": file_path,
            }
            return json.dumps(response)
        else:
            return json.dumps({"message": "Figure could not be generated."})

    except Exception as e:
        return json.dumps({"message": f"An error occurred during plotting: {str(e)}"})


def get_tools():
    return [list_tables_dax_tool, get_schema_dax_tool, dax_query_checker_tool, run_dax_query_tool, query_and_plot_dax_tool]

class RouteQuery(BaseModel):
    """Route the user question to the appropriate tool."""
    destination: Literal['data_query', 'general_conversation'] = Field(
        ...,
        description="Given the user's question, decide whether it requires data from the database ('data_query') or if it is general conversation ('general_conversation')."
    )

def route_question(state: MessagesState):
    llm = get_llm()
    route_decision = llm.with_structured_output(RouteQuery).invoke(state['messages'][-1].content)
    return {"messages": [AIMessage(content=route_decision.destination)]}

def general_response(state: MessagesState):
    llm = get_llm()
    # Simplified implementation
    return {"messages": [llm.invoke(state['messages'])]}


def call_model(state: MessagesState):
    """The core agent node that decides what to do."""
    system_prompt = """You are a helpful AI assistant expert in querying Power BI semantic models with DAX (Data Analysis Expressions) queries.
    Your goal is to answer the user's question accurately.

    You have access to the following tools:
    1. list_tables_dax_tool: To see what tables are in the data model.
    2. get_schema_dax_tool: To get the schema of a specific table.
    3. dax_query_checker_tool: To check if a DAX query is syntactically valid.
    4. run_dax_query_tool: To run a DAX query against the semantic model.
    5. query_and_plot_dax_tool: To execute a DAX query and create a plot from the results.

    Follow these steps:
    1. First, use `list_tables_dax_tool` and `get_schema_dax_tool` to understand the available data.
    2. Construct a syntactically correct DAX query to find the answer. ALL queries must begin with 'EVALUATE'.
    3. Use the `dax_query_checker_tool` to verify your query.
    4. If the user's request explicitly asks for a 'plot', 'chart', 'graph', or 'visualization', you MUST use the 'query_and_plot_dax_tool'.
    5. For all other questions that require data, use the `run_dax_query_tool`.
    6. Analyze the result of the query and provide a final, natural language response.
    7. If a query fails, analyze the error, revise your plan, and try again.

    INTERNAL REASONING TO GENERATE THE DAX QUERY(use these steps **internally only**; do NOT output internal thoughts):
    1. Decide whether the output should be a table expression or a single-row summary (measure).
    2. Select correct DAX constructs (EVALUATE, SUMMARIZECOLUMNS, SELECTCOLUMNS, VAR/RETURN, CALCULATE, FILTER, iterators).
    3. Validate quoting and identifier usage against schema (use `'Table Name'[Column]` when schema shows spaces/special chars; otherwise `Table[Column]` is acceptable).
    4. When you use SUMMARIZECOLUMNS(), you cannot reference the original table columns in the TOPN function, because SUMMARIZECOLUMNS() creates a new table and hence you need to reference the columns from the new table structure created by SUMMARIZECOLUMNS().
    5. Check types and aggregations (avoid aggregating non-numeric columns).
    6. Verify syntax (balanced parentheses, correct VAR/RETURN placement, EVALUATE present).
    7. Simplify/optimize (use VAR to avoid duplication, prefer set-based patterns).

    ADDITIONAL GUIDANCE:
    - If a referenced table/column is missing from schema, do NOT invent names silently.
    - Prefer `SUMMARIZECOLUMNS` / `SELECTCOLUMNS` for query output; avoid returning raw calculated-column expressions unless requested.
    - But make sure, when you use SUMMARIZECOLUMNS(), you cannot reference the original table columns in the TOPN function, because SUMMARIZECOLUMNS() creates a new table and hence you need to reference the columns from the new table structure created by SUMMARIZECOLUMNS().
    - Use VAR for repeated expensive expressions.

    **CRITICAL RULE: To prevent fetching too much data, always use the `TOPN` function in your DAX queries to limit results unless the user asks for everything. A limit of 23 (e.g., `TOPN(23, ...)` is a good default.**
    
    Example DAX for getting the top 10 customers by sales:
    EVALUATE
    TOPN(
        10,
        SUMMARIZECOLUMNS(
            'Customer'[Customer Name],
            "Total Sales", [Total Sales Amount]
        ),
        [Total Sales Amount],
        DESC
    )
    """
    llm = get_llm()
    tools = get_tools()
    cleaned_messages = [msg for msg in state['messages'] if msg.content != 'data_query']
    messages = [SystemMessage(content=system_prompt)] + cleaned_messages
    llm_with_tools = llm.bind_tools(tools)
    response = llm_with_tools.invoke(messages)
    return {'messages': [response]}

def decide_path(state: MessagesState) -> Literal['data_path', 'general_path']:
    if state['messages'][-1].content == "data_query":
        return 'data_path'
    return 'general_path'

def after_model_action(state: MessagesState):
    if state['messages'][-1].tool_calls:
        return 'call_tool'
    return END


def get_bot():
    builder = StateGraph(MessagesState)
    builder.add_node('router', route_question)
    builder.add_node('general_response', general_response)
    builder.add_node('agent', call_model)
    builder.add_node('call_tool', ToolNode(get_tools()))

    builder.add_edge(START, 'router')
    builder.add_conditional_edges(
        'router',
        decide_path,
        {'data_path': 'agent', 'general_path': 'general_response'}
    )
    builder.add_edge('general_response', END)
    builder.add_conditional_edges('agent', after_model_action, {'call_tool': 'call_tool', END: END})
    builder.add_edge('call_tool', 'agent')

    return builder.compile(checkpointer=MemorySaver())
