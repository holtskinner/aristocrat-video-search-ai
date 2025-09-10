# scripts/index_to_bigquery.py
import hashlib
import json
import re
from datetime import datetime

from google.cloud import bigquery, storage


class VideoIndexerBQ:
    """Index video transcriptions directly to BigQuery for ADK access."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client()

        # BigQuery dataset and tables
        self.dataset_id = "video_search"
        self.segments_table = "video_segments"
        self.videos_table = "videos_metadata"

    def setup_bigquery_schema(self) -> None:
        """Create BigQuery dataset and tables optimized for ADK queries."""
        # Create dataset
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US-CENTRAL1"
        dataset.description = "Video search data for ADK agent queries"

        try:
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            print(f"✅ Dataset {dataset_id} ready")
        except Exception as e:
            print(f"Dataset exists or error: {e}")

        # Videos metadata table schema
        videos_schema = [
            bigquery.SchemaField(
                "video_id",
                "STRING",
                mode="REQUIRED",
                description="Unique video identifier",
            ),
            bigquery.SchemaField(
                "video_title",
                "STRING",
                mode="REQUIRED",
                description="Original video filename",
            ),
            bigquery.SchemaField(
                "video_gcs_uri",
                "STRING",
                mode="REQUIRED",
                description="GCS URI of source video",
            ),
            bigquery.SchemaField(
                "audio_gcs_uri",
                "STRING",
                mode="NULLABLE",
                description="GCS URI of extracted audio",
            ),
            bigquery.SchemaField(
                "json_gcs_uri",
                "STRING",
                mode="REQUIRED",
                description="GCS URI of processed JSON",
            ),
            bigquery.SchemaField(
                "duration_seconds",
                "FLOAT64",
                mode="NULLABLE",
                description="Total video duration",
            ),
            bigquery.SchemaField(
                "total_segments",
                "INTEGER",
                mode="NULLABLE",
                description="Number of segments",
            ),
            bigquery.SchemaField(
                "total_speakers",
                "INTEGER",
                mode="NULLABLE",
                description="Number of unique speakers",
            ),
            bigquery.SchemaField(
                "has_diarization",
                "BOOLEAN",
                mode="NULLABLE",
                description="Whether speaker diarization was used",
            ),
            bigquery.SchemaField(
                "has_ocr",
                "BOOLEAN",
                mode="NULLABLE",
                description="Whether OCR was performed",
            ),
            bigquery.SchemaField(
                "processed_date",
                "TIMESTAMP",
                mode="REQUIRED",
                description="When the video was processed",
            ),
        ]

        # Comprehensive segments table schema - using INTEGER for time clustering
        segments_schema = [
            # Core identifiers
            bigquery.SchemaField(
                "segment_id",
                "STRING",
                mode="REQUIRED",
                description="Unique segment identifier",
            ),
            bigquery.SchemaField(
                "video_id",
                "STRING",
                mode="REQUIRED",
                description="Parent video identifier",
            ),
            bigquery.SchemaField(
                "video_title",
                "STRING",
                mode="REQUIRED",
                description="Original video filename",
            ),
            # Timing information - both float and integer versions
            bigquery.SchemaField(
                "start_time_seconds",
                "FLOAT64",
                mode="REQUIRED",
                description="Segment start time in seconds (precise)",
            ),
            bigquery.SchemaField(
                "end_time_seconds",
                "FLOAT64",
                mode="REQUIRED",
                description="Segment end time in seconds (precise)",
            ),
            bigquery.SchemaField(
                "start_time_int",
                "INTEGER",
                mode="REQUIRED",
                description="Segment start time in seconds (integer for clustering)",
            ),
            bigquery.SchemaField(
                "duration_seconds",
                "FLOAT64",
                mode="NULLABLE",
                description="Segment duration",
            ),
            # Content
            bigquery.SchemaField(
                "transcript",
                "STRING",
                mode="NULLABLE",
                description="Speech transcript text",
            ),
            bigquery.SchemaField(
                "slide_text",
                "STRING",
                mode="NULLABLE",
                description="Text extracted from slides via OCR",
            ),
            bigquery.SchemaField(
                "combined_text",
                "STRING",
                mode="NULLABLE",
                description="Combined transcript and slide text for searching",
            ),
            # Search optimization
            bigquery.SchemaField(
                "keywords",
                "STRING",
                mode="REPEATED",
                description="Extracted keywords for fast filtering",
            ),
            bigquery.SchemaField(
                "topics",
                "STRING",
                mode="REPEATED",
                description="Identified topics/themes",
            ),
            # Speaker information
            bigquery.SchemaField(
                "speaker_tag",
                "INTEGER",
                mode="NULLABLE",
                description="Speaker identifier from diarization",
            ),
            # Metadata
            bigquery.SchemaField(
                "word_count",
                "INTEGER",
                mode="NULLABLE",
                description="Number of words in segment",
            ),
            bigquery.SchemaField(
                "char_count",
                "INTEGER",
                mode="NULLABLE",
                description="Number of characters in segment",
            ),
            # File references
            bigquery.SchemaField(
                "video_gcs_uri",
                "STRING",
                mode="REQUIRED",
                description="GCS URI of source video",
            ),
            bigquery.SchemaField(
                "json_gcs_uri",
                "STRING",
                mode="REQUIRED",
                description="GCS URI of processed JSON",
            ),
            # Processing metadata
            bigquery.SchemaField(
                "indexed_at",
                "TIMESTAMP",
                mode="REQUIRED",
                description="When this segment was indexed",
            ),
        ]

        # Create videos table
        videos_table_id = f"{self.project_id}.{self.dataset_id}.{self.videos_table}"
        videos_table = bigquery.Table(videos_table_id, schema=videos_schema)

        try:
            videos_table = self.bq_client.create_table(videos_table, exists_ok=True)
            print(f"✅ Table {videos_table_id} ready")
        except Exception as e:
            print(f"Videos table exists or error: {e}")

        # Create segments table with clustering on INTEGER field
        segments_table_id = f"{self.project_id}.{self.dataset_id}.{self.segments_table}"
        segments_table = bigquery.Table(segments_table_id, schema=segments_schema)

        # Add clustering on commonly queried fields (using INTEGER for time)
        segments_table.clustering_fields = ["video_id", "speaker_tag", "start_time_int"]

        try:
            segments_table = self.bq_client.create_table(segments_table, exists_ok=True)
            print(f"✅ Table {segments_table_id} ready with clustering")
        except Exception as e:
            print(f"Segments table exists or error: {e}")

        # Create search views
        self._create_search_views()

    def _create_search_views(self) -> None:
        """Create simplified views for ADK agent queries."""
        # Main search view
        search_view_id = f"{self.project_id}.{self.dataset_id}.search_view"
        search_view_query = f"""
        CREATE OR REPLACE VIEW `{search_view_id}` AS
        SELECT
            s.video_title,
            s.speaker_tag,
            s.start_time_seconds,
            s.end_time_seconds,
            ROUND(s.duration_seconds, 1) as duration_seconds,
            s.transcript,
            s.slide_text,
            s.combined_text,
            ARRAY_TO_STRING(s.keywords, ', ') as keywords_list,
            ARRAY_TO_STRING(s.topics, ', ') as topics_list,
            s.word_count,

            -- Formatted references
            CONCAT(
                'Video: ', s.video_title,
                ' | Time: ', CAST(ROUND(s.start_time_seconds) AS STRING),
                '-', CAST(ROUND(s.end_time_seconds) AS STRING), 's',
                ' | Speaker: ', CAST(s.speaker_tag AS STRING)
            ) as segment_reference,

            -- Direct link to video with timestamp
            CONCAT(
                'https://storage.cloud.google.com/',
                SUBSTR(s.video_gcs_uri, 6),
                '#t=', CAST(ROUND(s.start_time_seconds) AS STRING)
            ) as video_link,

            -- Metadata
            v.total_speakers,
            v.has_diarization,
            s.indexed_at

        FROM `{self.project_id}.{self.dataset_id}.{self.segments_table}` s
        JOIN `{self.project_id}.{self.dataset_id}.{self.videos_table}` v
        ON s.video_id = v.video_id
        ORDER BY s.video_title, s.start_time_seconds
        """

        # Speaker summary view
        speaker_view_id = f"{self.project_id}.{self.dataset_id}.speaker_summary"
        speaker_view_query = f"""
        CREATE OR REPLACE VIEW `{speaker_view_id}` AS
        SELECT
            video_title,
            speaker_tag,
            COUNT(*) as segment_count,
            SUM(duration_seconds) as total_speaking_seconds,
            ROUND(SUM(duration_seconds) / 60, 1) as total_speaking_minutes,
            MIN(start_time_seconds) as first_appearance,
            MAX(end_time_seconds) as last_appearance,
            SUM(word_count) as total_words
        FROM `{self.project_id}.{self.dataset_id}.{self.segments_table}`
        GROUP BY video_title, speaker_tag
        ORDER BY video_title, speaker_tag
        """

        # Topics overview view
        topics_view_id = f"{self.project_id}.{self.dataset_id}.topics_overview"
        topics_view_query = f"""
        CREATE OR REPLACE VIEW `{topics_view_id}` AS
        SELECT
            topic,
            COUNT(DISTINCT video_id) as video_count,
            COUNT(*) as segment_count,
            ARRAY_AGG(DISTINCT video_title LIMIT 10) as sample_videos
        FROM `{self.project_id}.{self.dataset_id}.{self.segments_table}`,
        UNNEST(topics) as topic
        GROUP BY topic
        ORDER BY segment_count DESC
        """

        try:
            self.bq_client.query(search_view_query).result()
            print(f"✅ Search view created: {search_view_id}")
        except Exception as e:
            print(f"Search view error: {e}")

        try:
            self.bq_client.query(speaker_view_query).result()
            print(f"✅ Speaker summary view created: {speaker_view_id}")
        except Exception as e:
            print(f"Speaker view error: {e}")

        try:
            self.bq_client.query(topics_view_query).result()
            print(f"✅ Topics overview view created: {topics_view_id}")
        except Exception as e:
            print(f"Topics view error: {e}")

    def index_video_json(self, json_uri: str, batch_size: int = 500):
        """Index a processed video JSON file to BigQuery."""
        print(f"\n📥 Indexing video from: {json_uri}")

        # Read JSON from GCS
        bucket_name = json_uri.split("/")[2]
        blob_path = "/".join(json_uri.split("/")[3:])
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            print(f"❌ File not found: {json_uri}")
            return None

        json_content = blob.download_as_text()
        video_data = json.loads(json_content)

        # Extract video info
        video_title = video_data.get("video_title", "Unknown")
        segments = video_data.get("segments", [])

        if not segments:
            print(f"⚠️ No segments found in {video_title}")
            return None

        video_id = hashlib.md5(video_title.encode()).hexdigest()[:12]

        # Determine file paths
        video_gcs_uri = json_uri.replace("/processed_json/", "/raw/").replace(
            ".json", ""
        )
        # Check for various video extensions
        for ext in [".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"]:
            test_uri = video_gcs_uri + ext
            test_bucket, test_blob = test_uri.replace("gs://", "").split("/", 1)
            if self.storage_client.bucket(test_bucket).blob(test_blob).exists():
                video_gcs_uri = test_uri
                break

        audio_gcs_uri = video_gcs_uri.replace("/raw/", "/audio/")
        for ext in [".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"]:
            audio_gcs_uri = audio_gcs_uri.replace(ext, ".wav")

        print(f"📹 Video: {video_title}")
        print(f"🔑 Video ID: {video_id}")
        print(f"📊 Segments to process: {len(segments)}")

        # Analyze segments for metadata
        unique_speakers = set()
        has_diarization = False
        has_ocr = False
        total_duration = 0

        for segment in segments:
            speaker = segment.get("speaker_tag", 0)
            unique_speakers.add(speaker)
            if speaker > 0:
                has_diarization = True
            if segment.get("slide_text", "").strip():
                has_ocr = True
            end_time = segment.get("end_time_seconds", 0)
            total_duration = max(total_duration, end_time)

        # Insert video metadata
        video_metadata = {
            "video_id": video_id,
            "video_title": video_title,
            "video_gcs_uri": video_gcs_uri,
            "audio_gcs_uri": audio_gcs_uri,
            "json_gcs_uri": json_uri,
            "duration_seconds": total_duration,
            "total_segments": len(segments),
            "total_speakers": len(unique_speakers),
            "has_diarization": has_diarization,
            "has_ocr": has_ocr,
            "processed_date": datetime.now().isoformat(),
        }

        videos_table_id = f"{self.project_id}.{self.dataset_id}.{self.videos_table}"
        errors = self.bq_client.insert_rows_json(videos_table_id, [video_metadata])
        if errors:
            print(f"⚠️ Error inserting video metadata: {errors}")
        else:
            print("✅ Video metadata inserted")

        # Process segments
        segments_to_insert = []

        for i, segment in enumerate(segments):
            # Generate segment ID
            segment_id = f"{video_id}_{i:04d}"

            # Get text content
            transcript = segment.get("transcript", "").strip()
            slide_text = segment.get("slide_text", "").strip()
            combined_text = f"{transcript} {slide_text}".strip()

            # Skip empty segments
            if not combined_text:
                continue

            # Extract keywords and topics
            keywords = self._extract_keywords(combined_text)
            topics = self._identify_topics(combined_text)

            # Calculate metrics
            word_count = len(combined_text.split())
            char_count = len(combined_text)

            # Get time values
            start_time = float(segment.get("start_time_seconds", 0))
            end_time = float(segment.get("end_time_seconds", 0))

            # Prepare segment for BigQuery
            segments_to_insert.append(
                {
                    "segment_id": segment_id,
                    "video_id": video_id,
                    "video_title": video_title,
                    "start_time_seconds": start_time,
                    "end_time_seconds": end_time,
                    "start_time_int": int(start_time),  # Integer version for clustering
                    "duration_seconds": end_time - start_time,
                    "transcript": transcript[:10000],  # Limit to 10K chars
                    "slide_text": slide_text[:5000],  # Limit to 5K chars
                    "combined_text": combined_text[:15000],  # Limit to 15K chars
                    "keywords": keywords,
                    "topics": topics,
                    "speaker_tag": segment.get("speaker_tag", 0),
                    "word_count": word_count,
                    "char_count": char_count,
                    "video_gcs_uri": video_gcs_uri,
                    "json_gcs_uri": json_uri,
                    "indexed_at": datetime.now().isoformat(),
                }
            )

            # Insert in batches
            if len(segments_to_insert) >= batch_size:
                self._insert_segments(segments_to_insert)
                segments_to_insert = []

        # Insert remaining segments
        if segments_to_insert:
            self._insert_segments(segments_to_insert)

        print(f"✅ Indexing complete for: {video_title}")

        # Print summary
        if has_diarization:
            print(f"   🎭 Speakers found: {unique_speakers}")
        print(f"   ⏱️ Duration: {total_duration / 60:.1f} minutes")

        # Print sample queries
        self._print_sample_queries(video_id, video_title)

        return video_id

    def _insert_segments(self, segments: list[dict]) -> None:
        """Insert segments to BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_id}.{self.segments_table}"
        errors = self.bq_client.insert_rows_json(table_id, segments)

        if errors:
            print(f"❌ Error inserting segments: {errors}")
        else:
            print(f"   ✅ Inserted {len(segments)} segments")

    def _extract_keywords(self, text: str, max_keywords: int = 20) -> list[str]:
        """Extract important keywords from text."""
        # Common stop words
        stop_words = {
            "the",
            "a",
            "an",
            "and",
            "or",
            "but",
            "in",
            "on",
            "at",
            "to",
            "for",
            "of",
            "with",
            "by",
            "from",
            "as",
            "is",
            "was",
            "are",
            "were",
            "been",
            "be",
            "have",
            "has",
            "had",
            "do",
            "does",
            "did",
            "will",
            "would",
            "could",
            "should",
            "may",
            "might",
            "must",
            "can",
            "this",
            "that",
            "these",
            "those",
            "i",
            "you",
            "he",
            "she",
            "it",
            "we",
            "they",
            "what",
            "which",
            "who",
            "when",
            "where",
            "why",
            "how",
            "all",
            "each",
            "every",
            "both",
            "few",
            "more",
            "most",
            "other",
            "some",
            "such",
            "only",
            "own",
            "same",
            "so",
            "than",
            "too",
            "very",
            "just",
            "there",
            "here",
            "then",
            "now",
            "also",
            "well",
            "even",
            "back",
            "still",
            "way",
            "our",
            "their",
            "them",
            "about",
            "out",
            "up",
            "down",
            "over",
            "under",
            "after",
            "before",
            "into",
            "through",
            "during",
            "against",
            "between",
            "above",
            "below",
            "any",
            "because",
            "being",
            "doing",
            "having",
            "get",
            "got",
            "getting",
        }

        # Extract words (alphanumeric, including technical terms)
        words = re.findall(r"\b[a-zA-Z0-9]+\b", text.lower())

        # Count word frequency
        word_count = {}
        for word in words:
            if len(word) > 2 and word not in stop_words and not word.isdigit():
                word_count[word] = word_count.get(word, 0) + 1

        # Get top keywords by frequency
        sorted_words = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
        return [word for word, count in sorted_words[:max_keywords]]

    def _identify_topics(self, text: str) -> list[str]:
        """Identify high-level topics from text."""
        topics = []
        text_lower = text.lower()

        # Define topic patterns (customize for your domain)
        topic_patterns = {
            "migration": ["migrate", "migration", "migrating", "transfer", "moving"],
            "synth": ["synth", "polysynth", "synthesis"],
            "ui_ux": ["ui", "ux", "interface", "user experience", "design"],
            "agent_development": [
                "agent",
                "adk",
                "agent development",
                "build agent",
                "create agent",
            ],
            "machine_learning": [
                "machine learning",
                "ml",
                "neural",
                "model",
                "training",
                "inference",
            ],
            "ai": [
                "artificial intelligence",
                "ai",
                "llm",
                "large language",
                "gpt",
                "gemini",
            ],
            "api": ["api", "endpoint", "rest", "graphql", "webhook", "integration"],
            "cloud": ["cloud", "gcp", "google cloud", "aws", "azure", "deployment"],
            "data": ["data", "dataset", "database", "query", "sql", "bigquery"],
            "development": ["development", "coding", "programming", "software", "code"],
            "testing": ["test", "testing", "debug", "qa", "quality"],
            "documentation": ["documentation", "docs", "readme", "guide", "tutorial"],
        }

        for topic, patterns in topic_patterns.items():
            if any(pattern in text_lower for pattern in patterns):
                topics.append(topic)

        return topics[:10]  # Limit to 10 topics per segment

    def _print_sample_queries(self, video_id: str, video_title: str) -> None:
        """Print sample BigQuery queries for ADK agent."""
        print("\n📝 Sample queries for ADK Agent:")
        print("-" * 60)

        print(f"""
-- Find mentions of a specific topic in this video
SELECT
    segment_reference,
    transcript,
    keywords_list,
    video_link
FROM `{self.project_id}.{self.dataset_id}.search_view`
WHERE video_title = '{video_title}'
  AND LOWER(combined_text) LIKE '%synth%'
ORDER BY start_time_seconds;

-- Search across all videos for a keyword
SELECT
    video_title,
    segment_reference,
    transcript,
    video_link
FROM `{self.project_id}.{self.dataset_id}.search_view`
WHERE LOWER(combined_text) LIKE '%migration%'
ORDER BY video_title, start_time_seconds
LIMIT 20;
""")


def main() -> None:
    """Main function to index videos."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Index processed videos to BigQuery for ADK"
    )
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument(
        "--json_uri", help="GCS URI of processed JSON (gs://bucket/path/file.json)"
    )
    parser.add_argument(
        "--json_folder",
        help="GCS folder with multiple JSONs (gs://bucket/processed_json/)",
    )
    parser.add_argument(
        "--setup_tables", action="store_true", help="Setup BigQuery tables and views"
    )

    args = parser.parse_args()

    indexer = VideoIndexerBQ(args.project_id)

    if args.setup_tables:
        print("🔧 Setting up BigQuery schema...")
        indexer.setup_bigquery_schema()

    if args.json_uri:
        indexer.index_video_json(args.json_uri)

    if args.json_folder:
        # Process all JSONs in folder
        storage_client = storage.Client()

        # Parse folder URI
        if args.json_folder.startswith("gs://"):
            parts = args.json_folder.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
        else:
            print("❌ Invalid GCS URI. Must start with gs://")
            return

        bucket = storage_client.bucket(bucket_name)

        json_files = []
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith(".json"):
                json_files.append(f"gs://{bucket_name}/{blob.name}")

        print(f"Found {len(json_files)} JSON files to process")

        for json_uri in json_files:
            print(f"\n{'=' * 60}")
            indexer.index_video_json(json_uri)


if __name__ == "__main__":
    main()
