# scripts/config.py
"""
Central configuration for the Video Search AI pipeline.
"""

# A unique ID for the custom recognizer
RECOGNIZER_ID = "video-search-ingestion-recognizer-v3"

# Supported video formats - THE SINGLE SOURCE OF TRUTH
SUPPORTED_VIDEO_FORMATS = ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.flv']