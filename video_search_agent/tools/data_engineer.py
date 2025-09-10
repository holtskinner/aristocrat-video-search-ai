# video_search_agent/tools/data_engineer.py

import uuid
from functools import cache

from google.adk.tools import ToolContext
from google.cloud.bigquery import Client, QueryJobConfig
from google.cloud.exceptions import BadRequest, NotFound
from google.genai.types import Content, GenerateContentConfig, Part
from pydantic import BaseModel, Field

from ..config import (
    BIGQUERY_DATASET_ID,
    DATA_ENGINEER_MODEL_ID,
    GOOGLE_CLOUD_LOCATION,
    GOOGLE_CLOUD_PROJECT,
)
from ..prompts.data_engineer import (
    SYSTEM_INSTRUCTION as data_engineer_instruction_template,
)
from .utils import get_genai_client

# --- Configuration for SQL Correction ---
SQL_CORRECTOR_MODEL_ID = DATA_ENGINEER_MODEL_ID
sql_correction_instruction_template = """

You are an expert BigQuery SQL troubleshooter. Your task is to fix a broken SQL query.
You will be given a non-working SQL query, the error message it produced, and the available table metadata.
Based on the error and the provided schemas, correct the query. Pay close attention to the table descriptions for join logic.
Your entire output must be a JSON object containing only the corrected SQL query.

**Available Table Metadata (in dataset `{dataset_id}`):**
{table_metadata}
"""


# --- Pydantic Model for Structured Output ---
class SQLResult(BaseModel):
    sql_query: str = Field(description="The final, valid BigQuery SQL query.")
    error: str = Field(
        default="", description="Any errors encountered during the process."
    )


# --- Helper Functions ---
@cache
def _get_bigquery_client():
    return Client(project=GOOGLE_CLOUD_PROJECT, location=GOOGLE_CLOUD_LOCATION)


def get_bigquery_schema() -> str:
    """Fetches the schema for all tables in a BigQuery dataset."""
    print(f"Fetching schema for dataset: {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET_ID}")
    client = _get_bigquery_client()
    query = f"""
        SELECT table_name, column_name, data_type
        FROM `{GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_name, ordinal_position;
    """
    try:
        query_job = client.query(query)
        results = query_job.result()

        tables = {}
        for row in results:
            if row.table_name not in tables:
                tables[row.table_name] = []
            tables[row.table_name].append(f"{row.column_name} ({row.data_type})")

        schema_string = ""
        for table_name, columns in tables.items():
            schema_string += f"Table: {table_name}\n"
            schema_string += "\n".join(columns)
            schema_string += "\n\n"

        return schema_string
    except Exception as e:
        print(f"Error fetching BigQuery schema: {e}")
        return ""


def _sql_validator(sql_code: str) -> tuple[str, str]:
    print(
        f"--- Running SQL Validator on: ---\n{sql_code}\n---------------------------------"
    )
    try:
        client = _get_bigquery_client()
        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
        client.query(sql_code, job_config=job_config).result()
    except (BadRequest, NotFound) as ex:
        err_text = getattr(ex, "message", str(ex))
        print(f"SQL Validation ERROR: {err_text}")
        return f"ERROR: {err_text}", sql_code
    print("SQL Validation SUCCESS")
    return "SUCCESS", sql_code


# --- Main Tool Function ---
async def data_engineer(request: str, tool_context: ToolContext) -> dict:
    print(f"Data Engineer received request: {request}")
    print("Step 1: Getting BigQuery schema...")
    table_metadata = get_bigquery_schema()
    dataset_id_formatted = f"`{GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET_ID}`"

    data_engineer_instruction = data_engineer_instruction_template.format(
        table_metadata=table_metadata, dataset_id=dataset_id_formatted
    )
    sql_correction_instruction = sql_correction_instruction_template.format(
        table_metadata=table_metadata, dataset_id=dataset_id_formatted
    )

    client = get_genai_client()

    generation_prompt = f"Analysis Plan Details:\n{request}\n\nPlease generate the BigQuery SQL query based on the plan and the schemas provided in the system instructions."

    print("Step 2: Generating initial SQL query...")
    sql_generation_result = client.models.generate_content(
        model=DATA_ENGINEER_MODEL_ID,
        contents=Content(role="user", parts=[Part.from_text(text=generation_prompt)]),
        config=GenerateContentConfig(
            response_schema=SQLResult,
            response_mime_type="application/json",
            system_instruction=data_engineer_instruction,
            temperature=0.0,
        ),
    )

    sql_to_validate = sql_generation_result.parsed.sql_query
    print(f"Initial generated SQL: {sql_to_validate}")

    MAX_FIX_ATTEMPTS = 5
    is_valid = False
    chat_session = None
    for attempt in range(MAX_FIX_ATTEMPTS):
        print(
            f"Step 3: Validating SQL query (attempt {attempt + 1}/{MAX_FIX_ATTEMPTS})..."
        )
        validator_result, sql_to_validate = _sql_validator(sql_to_validate)
        if validator_result == "SUCCESS":
            is_valid = True
            break
        print("SQL is invalid, attempting to correct with LLM...")
        if not chat_session:
            chat_session = client.chats.create(
                model=SQL_CORRECTOR_MODEL_ID,
                config=GenerateContentConfig(
                    response_schema=SQLResult,
                    response_mime_type="application/json",
                    system_instruction=sql_correction_instruction,
                    temperature=0.0,
                ),
            )
        correction_prompt = f"The following SQL query failed validation:\n```sql\n{sql_to_validate}```\nThe error was:\n{validator_result}\nPlease provide the corrected SQL query based on the available schemas."
        corr_result = chat_session.send_message(correction_prompt)
        sql_to_validate = corr_result.parsed.sql_query
        print(f"Corrected SQL candidate: {sql_to_validate}")

    if is_valid:
        print(f"Final validated SQL: {sql_to_validate}")
        sql_file_name = f"query_{uuid.uuid4().hex}.sql"
        await tool_context.save_artifact(
            sql_file_name,
            Part.from_bytes(
                mime_type="text/x-sql", data=sql_to_validate.encode("utf-8")
            ),
        )
        try:
            print("Step 4: Executing SQL query...")
            bigquery_client = _get_bigquery_client()
            query_job = bigquery_client.query(sql_to_validate)
            results_df = query_job.to_dataframe()
            print("Step 5: Returning results.")
            return {"query_result": results_df.to_dict(orient="records")}
        except Exception as e:
            error_message = f"Failed to execute query: {e}"
            print(error_message)
            return {"error": error_message}
    else:
        print("Could not create a valid query after multiple attempts.")
        return {
            "error": f"Could not create a valid query in {MAX_FIX_ATTEMPTS} attempts."
        }
