# scripts/path_utils.py
"""
Utilities for generating and parsing GCS paths for the pipeline.
"""
import os
from .config import SUPPORTED_VIDEO_FORMATS

def parse_gcs_uri(uri: str) -> tuple:
    """Parses a GCS URI into bucket and blob name."""
    if not uri.startswith("gs://"):
        raise ValueError("Invalid GCS URI. Must start with 'gs://'.")
    parts = uri.replace("gs://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS URI format. Must be gs://bucket-name/path/to/blob.")
    return parts[0], parts[1]

def get_derived_paths(video_uri: str) -> dict:
    """
    From a single video URI, derive all other related paths.
    
    Returns a dictionary with:
    - video_uri
    - audio_uri
    - json_uri
    - base_filename (e.g., "my_video")
    - bucket_name
    """
    bucket_name, video_blob_name = parse_gcs_uri(video_uri)
    
    base_filename = os.path.basename(video_blob_name)
    
    # Remove any supported video extension to get the clean name
    clean_name = base_filename
    for ext in SUPPORTED_VIDEO_FORMATS:
        if clean_name.lower().endswith(ext):
            clean_name = clean_name[:-len(ext)]
            break
            
    # Create the audio blob name in the /audio/ directory
    audio_blob_name = video_blob_name.replace('raw/', 'audio/', 1)
    for ext in SUPPORTED_VIDEO_FORMATS:
        if audio_blob_name.lower().endswith(ext):
            audio_blob_name = audio_blob_name[:-len(ext)] + '.wav'
            break
            
    # Create the JSON blob name in the /processed_json/ directory
    json_blob_name = f"processed_json/{clean_name.replace(' ', '_').replace('-', '_')}.json"

    return {
        "video_uri": video_uri,
        "audio_uri": f"gs://{bucket_name}/{audio_blob_name}",
        "json_uri": f"gs://{bucket_name}/{json_blob_name}",
        "base_filename": clean_name,
        "bucket_name": bucket_name,
    }