# scripts/create_embeddings.py
import argparse

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery


def create_embeddings(
    project_id: str,
    dataset_name: str,
    source_table_name: str,
    target_table_name: str,
    bq_connection_name: str,
    text_column: str = "combined_text",
    primary_key: str = "segment_id",
) -> None:
    """Creates a remote model in BigQuery and then uses it to generate text
    embeddings, storing them in a new table.

    Args:
        project_id (str): The Google Cloud project ID.
        dataset_name (str): The BigQuery dataset name.
        source_table_name (str): The name of the source table with text data.
        target_table_name (str): The name of the table to store embeddings.
        bq_connection_name (str): The ID of the BigQuery Connection in the 'us-central1' region.
        text_column (str): The column containing the text to embed.
        primary_key (str): The unique identifier column for each segment.
    """
    print("\n" + "=" * 60)
    print("🧠 BIGQUERY EMBEDDING GENERATION")
    print("=" * 60)
    print(f"🏢 Project: {project_id}")
    print(f"📦 Dataset: {dataset_name}")
    print(f"📖 Source Table: {source_table_name}")
    print(f"🎯 Target Table: {target_table_name}")
    print(f"🔌 BQ Connection: {bq_connection_name}")
    print("=" * 60 + "\n")

    client = bigquery.Client(project=project_id)

    # --- Step 1: Create the Remote Model in BigQuery ---
    # This model acts as a pointer to the Vertex AI foundation model.
    model_id = f"`{project_id}.{dataset_name}.text_embedding_model`"
    connection_id = f"`{project_id}.us-central1.{bq_connection_name}`"
    vertex_model_name = "text-embedding-004"

    print(f"1. Ensuring BigQuery model '{model_id}' exists...")

    create_model_query = f"""
    CREATE OR REPLACE MODEL {model_id}
    REMOTE WITH CONNECTION {connection_id}
    OPTIONS (endpoint = '{vertex_model_name}');
    """

    try:
        model_job = client.query(create_model_query)
        model_job.result()  # Wait for the model creation to complete
        print("   ✅ Model created/verified successfully.")
    except GoogleAPICallError as e:
        print(f"\n❌ Failed to create or replace the BigQuery remote model: {e}")
        print(
            "   Please ensure the BigQuery Connection is correctly set up in the 'us' region."
        )
        raise

    # --- Step 2: Generate Embeddings Using the New Model ---
    source_table_id = f"`{project_id}.{dataset_name}.{source_table_name}`"
    target_table_id = f"`{project_id}.{dataset_name}.{target_table_name}`"

    print(f"\n2. Generating embeddings using model '{model_id}'...")

    # Corrected the SELECT statement to use the correct column name 'text_embedding'
    generate_embeddings_query = f"""
    CREATE OR REPLACE TABLE {target_table_id} AS
    SELECT
        t.{primary_key},
        t.text_embedding
    FROM
        ML.GENERATE_TEXT_EMBEDDING(
            MODEL {model_id},
            (
                SELECT
                    {primary_key},
                    {text_column} AS content
                FROM
                    {source_table_id}
                WHERE
                    {text_column} IS NOT NULL AND TRIM({text_column}) != ''
            ),
            STRUCT(TRUE AS flatten_json_output)
        ) AS t;
    """

    print("   🚀 Executing BigQuery job...")
    print("      (This may take a few minutes depending on the amount of data)")

    try:
        embedding_job = client.query(generate_embeddings_query)
        embedding_job.result()

        print("\n" + "=" * 60)
        print("✅ EMBEDDING GENERATION COMPLETE!")
        print("=" * 60)

        target_table = client.get_table(target_table_id.replace("`", ""))
        print(f"   Table '{target_table_name}' created/updated successfully.")
        print(f"   Total rows (embeddings created): {target_table.num_rows}")
        print("=" * 60 + "\n")

    except GoogleAPICallError as e:
        print(f"\n❌ A BigQuery API error occurred during embedding generation: {e}")
        raise
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
        raise


def main() -> None:
    """Main function to parse arguments and run the embedding creation."""
    parser = argparse.ArgumentParser(
        description="Generate text embeddings using BigQuery ML."
    )
    parser.add_argument(
        "--project_id",
        type=str,
        required=True,
        help="The Google Cloud Project ID.",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="BigQuery dataset name.",
    )
    parser.add_argument(
        "--source_table",
        type=str,
        required=True,
        help="Source table with text data.",
    )
    parser.add_argument(
        "--target_table",
        type=str,
        required=True,
        help="Target table to store embeddings.",
    )
    parser.add_argument(
        "--bq_connection_name",
        type=str,
        required=True,
        help="The ID of the BigQuery Connection (must be in 'us-central1' region).",
    )
    args = parser.parse_args()

    create_embeddings(
        project_id=args.project_id,
        dataset_name=args.dataset,
        source_table_name=args.source_table,
        target_table_name=args.target_table,
        bq_connection_name=args.bq_connection_name,
    )


if __name__ == "__main__":
    main()
