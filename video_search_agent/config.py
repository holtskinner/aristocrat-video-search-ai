# video_search_agent/config.py

import os
from dotenv import load_dotenv

# --- Load Environment Variables ---
# Load the .env file from the project root.
# This makes secrets and environment-specific settings available as environment variables.
load_dotenv()

# --- GCP & BigQuery Configuration ---
# Fetches the GCP Project ID from the environment.
# This is a critical variable for all Google Cloud client libraries.
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not GOOGLE_CLOUD_PROJECT:
    raise ValueError("CRITICAL: GOOGLE_CLOUD_PROJECT environment variable not set.")

# Fetches the BigQuery Dataset ID from the environment.
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID")
if not BIGQUERY_DATASET_ID:
    raise ValueError("CRITICAL: BIGQUERY_DATASET_ID environment variable not set.")

# Fetches the list of queryable table IDs from the environment.
# The string is split into a Python list, providing a typed configuration value.
BIGQUERY_TABLE_IDS = os.environ.get("BIGQUERY_TABLE_IDS", "").split(',')

# --- Model Configuration ---
ROOT_AGENT_MODEL_ID = "gemini-2.5-pro"
DATA_ENGINEER_MODEL_ID = "gemini-2.5-pro"

# --- Vertex AI Settings ---
GOOGLE_GENAI_USE_VERTEXAI = os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "False").lower() == "true"
GOOGLE_CLOUD_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "")