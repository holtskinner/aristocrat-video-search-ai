# Project Memory: video_search_ai (as of 2025-08-28)

## Final Architecture: Single Agent with Tools

After a series of iterations and experiments, the system has been stabilized with a **single-agent architecture**. This approach proved to be the most reliable and robust solution.

The architecture consists of:
-   A single `root_agent` defined in `video_search_agent/agent.py`.
-   A set of specialized tools in `video_search_agent/tools/tools.py` that the `root_agent` uses to interact with the BigQuery database.
-   A directive prompt in `video_search_agent/prompts/root_agent_prompt.py` that guides the `root_agent` on how to use its tools.

This architecture is a departure from the more complex, multi-agent systems that were initially attempted. The key learning is that for this specific use case, a single, well-instructed agent with a clear set of tools is more effective than a more complex, hierarchical system.

## Core Components

### 1. The Agent (`agent.py`)

-   **File:** `video_search_agent/agent.py`
-   **Agent Name:** `root_agent` (internally named `VideoSearchAgent`)
-   **Model:** `gemini-2.5-pro`
-   **Description:** The agent's primary role is to answer questions about a video library by querying a BigQuery database.

### 2. The Prompt (`root_agent_prompt.py`)

-   **File:** `video_search_agent/prompts/root_agent_prompt.py`
-   **Key Instructions:**
    -   The agent is explicitly told that it **MUST** use its tools to answer questions and should not make assumptions about the database schema.
    -   The **first step** for the agent is always to discover the table schemas by using the `get_table_schema` tool.
    -   It is instructed to default to semantic search for most queries.
    -   It provides a clear example workflow of how the agent should think and act.

### 3. The Tools (`tools.py`)

-   **File:** `video_search_agent/tools/tools.py`
-   **Available Tools:**
    -   **`get_table_schema(table_names: str) -> str`**
    -   **`execute_query(query: str) -> str`**
    -   **`semantic_search(query: str) -> str`**

## Key Learnings and Future Improvements

This project has been a valuable learning experience. Here are some of the key takeaways:

-   **Start Simple:** It's often best to start with the simplest possible architecture and only add complexity when it's absolutely necessary.
-   **Directive Prompts are Key:** The success of an agent is highly dependent on the quality of its prompt. A clear, directive prompt that tells the agent *how* to use its tools is more effective than a vague, open-ended one.
-   **Tools are More Reliable than Complex Prompts:** Encapsulating complexity in tools is a more robust approach than trying to put all the logic in the prompt.
-   **Semantic Search is Powerful:** Semantic search is a powerful tool for this use case, and it should be the default search method.

**Future Improvements:**

As you suggested, now that we have a stable baseline, we can explore re-introducing a planner to handle more complex, multi-step queries. A simple "routing" planner that can break down a complex query into a series of calls to the main agent would be a good next step.