# video_search_agent/tools/utils.py

from google.genai import Client


def get_genai_client() -> Client:
    """Returns a Gen AI client."""
    return Client(vertexai=True)
