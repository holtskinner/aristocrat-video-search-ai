# video_search_agent/agent.py

from google.adk.agents import LlmAgent
from .tools.data_engineer import data_engineer
from .prompts import root_agent_prompt

# The main agent for the video search system.
root_agent = LlmAgent(
    name="VideoSearchAgent",
    model="gemini-2.5-pro",
    instruction=root_agent_prompt.ROOT_AGENT_PROMPT,
    tools=[
        data_engineer,
    ],
    description="An agent that can answer questions about a video library by querying a BigQuery database."
)