# Plan for Re-introducing a Planner

This document outlines the plan for re-introducing a planner to the video search agent.

## Planner Prompt

```python
# video_search_agent/prompts/planner_prompt.py

SYSTEM_INSTRUCTION = """
You are a master planner. Your job is to take a complex user request and break it down into a series of smaller, more manageable steps. Each step should be a clear and concise instruction that can be executed by a data engineer.

For example, if the user asks:

"Find all the videos where a specific person is talking about a specific topic, and then find the part of the video where they talk about it."

You should break this down into the following steps:

1.  **Find the videos where the person is speaking.** This will require a query to the `videos_metadata` table.
2.  **Find the video segments where the topic is discussed.** This will require a query to the `video_segments` table.
3.  **Join the results of the two queries** to find the specific video segments where the person is talking about the topic.

Your output should be a JSON object with a single key, "steps", which is a list of strings. Each string is a step in the plan.
"""
```

## Planner Agent

```python
# video_search_agent/tools/planner.py

from google.adk.agents import LlmAgent
from ..prompts import planner_prompt

planner = LlmAgent(
    name="Planner",
    model="gemini-2.5-pro",
    instruction=planner_prompt.SYSTEM_INSTRUCTION,
    response_mime_type="application/json",
    description="A planner that can break down complex user requests into a series of smaller, more manageable steps."
)
```

## Summary of Changes

In the new project, the `root_agent` will be updated to use the new planner agent. The `root_agent` will pass the user's request directly to the planner, and the planner will then orchestrate the execution of the plan, calling the `data_engineer` tool for each step. The planner will be an `AgentTool` used by the `root_agent`.

This will make the system more robust and reliable by separating the planning and execution steps. The `root_agent` will be responsible for interacting with the user and managing the overall conversation, while the planner will be responsible for breaking down complex requests and the `data_engineer` will be responsible for executing the individual steps of the plan.
