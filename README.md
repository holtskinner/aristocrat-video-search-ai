# Video Search AI

## Overview

This project is a sophisticated AI-powered system for indexing, searching, and analyzing video content. It leverages Google Cloud's powerful AI and data services to provide deep insights into a video library. The core of the project is an AI agent capable of understanding and processing natural language queries to search for specific moments, objects, or spoken words within a collection of videos.

## Agent Architecture

The system is built around a single, primary agent (`root_agent`) that is responsible for all tasks. This agent is designed to be a "BigQuery expert" and has a set of specialized tools to interact with the database.

### How it Works

1. **Receives a Request:** The agent receives a natural language request from the user.
2. **Discovers the Schema:** The agent's first step is always to query the database to discover the available tables and their schemas. This prevents the agent from hallucinating table names.
3. **Selects a Tool:** Based on the user's request and the database schema, the agent decides which tool to use:
    - `get_table_schema`: To get the schema of a table.
    - `execute_query`: For standard SQL queries (e.g., keyword searches).
    - `semantic_search`: For conceptual or semantic searches.
4. **Executes the Tool:** The agent executes the selected tool.
5. **Synthesizes the Answer:** The agent takes the results from the tool and synthesizes them into a human-readable answer for the user.

This single-agent architecture, combined with a directive prompt, provides a robust and reliable system for video search.

## Features

- **Automated Video Ingestion:** Scripts for batch processing and ingesting video files from Google Cloud Storage.
- **Deep Video Analysis:** Utilizes the Google Cloud Video Intelligence API for shot change detection, object tracking, speech-to-text transcription, and more.
- **Structured Indexing:** Indexes extracted video metadata into Google BigQuery for efficient, structured querying.
- **AI Agent:** A conversational agent for interacting with the video search system.

## Project Structure

```none
/
├── pyproject.toml          # Project metadata and dependencies
├── scripts/                # Data ingestion and processing scripts
│   ├── batch_ingestion.py  # Handles batch video processing
│   ├── config.py           # Configuration for scripts
│   ├── index_to_bigquery.py# Indexes video metadata into BigQuery
│   └── run_ingestion.py    # Core ingestion logic invoked by batch_ingestion
└── video_search_agent/     # The core AI agent application
    ├── agent.py            # Main agent logic
    ├── config.py           # Agent configuration
    ├── prompts/            # Agent prompts
    └── tools/              # Agent tools
```

## Ingestion and Indexing Workflow

Follow these steps to ingest your videos and index their metadata into BigQuery.

### Prerequisites

- Python 3.11+
- An active Google Cloud Platform project.
- The `gcloud` CLI installed and authenticated (`gcloud auth application-default login`).
- The following GCP APIs enabled in your project: Cloud Storage, Video Intelligence API, BigQuery API, Vertex AI API.

### Step 1: Create GCS Bucket and Upload Videos

1. **Create a Google Cloud Storage (GCS) bucket.** Choose a unique name for your bucket.

    ```bash
    gsutil mb gs://your-gcs-bucket-name
    ```

2. **Create a `raw` folder inside your bucket.** This is where you will upload the videos you want to process.

    ```bash
    gsutil mkdir gs://your-gcs-bucket-name/raw
    ```

3. **Upload your videos** to the `raw` folder.

    ```bash
    gsutil -m cp /path/to/your/local/videos/* gs://your-gcs-bucket-name/raw/
    ```

### Step 2: Run Batch Ingestion

This process will analyze the videos in the `gs://<your-bucket>/raw` folder using the Video Intelligence API and store the JSON results in `gs://<your-bucket>/processed_json/`.

Run the following command, replacing the placeholder values with your GCP project ID and bucket name:

```bash
python -m scripts.batch_ingestion \
    --bucket_name "your-gcs-bucket-name" \
    --project_id "your-gcp-project-id" \
    --skip_ocr
```

This command will scan the bucket and tell you which videos it plans to process, then ask for confirmation. It's the safest way to start.

### Step 3: Set Up BigQuery Tables

Next, create the necessary tables in BigQuery to hold the indexed data. This only needs to be done once. **The dataset will be created in the `us-central1` region.**

```bash
python scripts/index_to_bigquery.py \
  --project_id "your-gcp-project-id" \
  --setup_tables
```

### Step 4: Index Video Metadata

Finally, parse the JSON output from the ingestion step and load it into the BigQuery tables you just created.

```bash
python scripts/index_to_bigquery.py \
  --project_id "your-gcp-project-id" \
  --json_folder "gs://your-gcs-bucket-name/processed_json/"
```

After completing these steps, your video metadata will be indexed and searchable in BigQuery.

### Step 5: Generate Text Embeddings

After indexing the video metadata, you can generate text embeddings for the transcribed text. These embeddings can be used for semantic search and other NLP tasks.

**Prerequisite: BigQuery Connection and Remote Model**

Before you can generate embeddings, you must have a BigQuery Connection set up in the `us-central1` region. This connection allows BigQuery to access services like Vertex AI. You also need a remote model named `text_embedding_model` in your BigQuery dataset that points to a Vertex AI embedding model (e.g., `textembedding-gecko@003`).

You can create a connection using the `gcloud` CLI:

```bash
gcloud bigquery connections create us-central1.vertex-ai-connection --connection-type=CLOUD_RESOURCE --project_id="your-gcp-project-id"
```

After creating the connection, you will need its ID for the next step.

Then, create the remote model in BigQuery. Replace `your-gcp-project-id` and `your-bigquery-dataset` with your actual values, and `your-connection-id` with the ID from the previous step.

```sql
CREATE OR REPLACE MODEL `your-gcp-project-id.your-bigquery-dataset.text_embedding_model`
REMOTE WITH CONNECTION `your-gcp-project-id.us-central1.your-connection-id`
OPTIONS (
  endpoint = 'textembedding-gecko@003'
);
```

**Run the Embedding Creation Script**

This script will use the `text_embedding_model` to generate embeddings for the text in your source table.

```bash
python scripts/create_embeddings.py \
  --project_id "your-project-id" \
  --dataset "your-dataset" \
  --source_table "bq-source-table-name" \
  --target_table "bq-target-table-name" \
  --bq_connection_name "bq-connector-name"
```

### Alternative: Testing Video Transcription

The `scripts/test_video_intelligence_stt.py` script provides a way to test the speech-to-text transcription functionality of the Google Cloud Video Intelligence API on a single video file. This is useful for debugging or for quickly transcribing a single video without running the full batch ingestion process.

**Note:** This script is for testing purposes only. The Video Intelligence API does not produce results in the same way as the batch ingestion script. It displays the results to the Terminal and does not store them in a GCS Bucket. More work would be needed to update the script to store the results, format them, and then ingest them into BigQuery.

To run the script, you will need to provide the GCS URI of the video file you want to transcribe.

```bash
python scripts/test_video_intelligence_stt.py \
  --video_uri "gs://bucket-name/raw/video-name"
```

## Running the Video Search AI Agent

1. **Navigate to the project directory**:

    ```bash
    cd /Users/jasonpendleton/Projects/video_search_ai
    ```

2. **Create and activate a virtual environment with `uv`:**

    ```bash
    uv venv
    source .venv/bin/activate
    ```

3. **Install dependencies with `uv`:**

    ```bash
    uv pip install -r requirements.txt
    ```

4. **Create a `.env` file** in the `video_search_agent` directory with the following content, filling in your Google Cloud project details:

    ```env
    GOOGLE_GENAI_USE_VERTEXAI=True
    GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
    GOOGLE_CLOUD_LOCATION="us-central1"
    BIGQUERY_DATASET_ID="video_search"
    BIGQUERY_TABLE_IDS="videos_metadata,video_segments,video_embeddings"
    ```

5. **Run the agent:**

    ```bash
    adk web
    ```
