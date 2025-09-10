# video_search_agent/tools/quiz_generator.py

import json
import uuid
from typing import List, Optional, Dict, Any

from google.adk.tools import ToolContext
from google.genai.types import Content, GenerateContentConfig, Part
from pydantic import BaseModel, Field, ValidationError

from video_search_agent.tools.utils import get_genai_client
from video_search_agent.config import DATA_ENGINEER_MODEL_ID


# ---------- Internal models (not exposed as tool params) ----------

class SegmentInput(BaseModel):
    video_title: str
    speaker_tag: Optional[int] = None
    start_time_seconds: int
    transcript: str
    video_link: Optional[str] = None

class QuizQuestion(BaseModel):
    id: str
    type: str  # "mcq" or "truefalse"
    stem: str
    options: Optional[List[str]] = None
    answer_index: Optional[int] = None
    answer_boolean: Optional[bool] = None
    rationale: Optional[str] = None
    reference: Optional[str] = None
    link: Optional[str] = None

class QuizPayload(BaseModel):
    topic: str
    difficulty: str
    questions: List[QuizQuestion]


SYSTEM_INSTRUCTION = """
You are an expert quiz author. Create clear, unambiguous questions from the provided transcript snippets.
Rules:
1) Build questions strictly from the supplied content; do not invent facts.
2) Prefer comprehension/recall; avoid vague wording and double negatives.
3) For MCQ, write 1 correct option + 3 plausible distractors.
4) Keep stems concise.
5) If a reference and link are provided, keep them as-is.
6) If rationales are requested, include one sentence explaining correctness.
Output must conform to the provided response schema.
"""


def _mm_ss(seconds: int) -> str:
    m, s = divmod(max(0, int(seconds)), 60)
    return f"{m:02d}:{s:02d}"


async def quiz_generator(
    topic: str,
    segments: List[Dict[str, Any]],
    num_questions: int,
    difficulty: str,
    style: str,
    include_rationales: bool,
    use_timestamp_links: bool,
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """
    Generate a quiz from video transcript segments.

    Use this tool when the user asks to be quizzed on a topic covered in the video library.
    Typical flow:
      1) Call `data_engineer` to fetch relevant segments.
      2) Pass those segments here with the user's preferences.

    Args:
        topic: The quiz topic or intent (e.g., "Vector indexes in BigQuery").
        segments: A list of segment dicts from `data_engineer` with keys:
                  video_title (str), start_time_seconds (int), transcript (str),
                  and optional speaker_tag (int), video_link (str).
        num_questions: Number of questions to generate (1..20).
        difficulty: "easy", "medium", "hard", or "mixed".
        style: "mcq", "truefalse", or "mixed".
        include_rationales: Whether to include one-sentence rationales.
        use_timestamp_links: Whether to include timestamped links in results.

    Returns:
        dict with:
          - status: "success" | "error"
          - quiz_json_artifact: str (on success)
          - quiz_markdown_artifact: str (on success)
          - quiz_preview: dict (lightweight preview payload) OR
          - error_message: str (on error)

    Notes:
        Do not describe ToolContext to the model; ADK injects it automatically.
    """
    # --- Validate simple constraints (ADK requires the tool to handle its own input hygiene) ---
    if not topic or not isinstance(topic, str):
        return {"status": "error", "error_message": "Missing or invalid 'topic'."}
    if not isinstance(segments, list) or not segments:
        return {"status": "error", "error_message": "Provide non-empty 'segments' from data_engineer."}
    if not (1 <= int(num_questions) <= 20):
        return {"status": "error", "error_message": "'num_questions' must be between 1 and 20."}
    difficulty = difficulty.lower()
    if difficulty not in {"easy", "medium", "hard", "mixed"}:
        return {"status": "error", "error_message": "Invalid 'difficulty'. Use easy|medium|hard|mixed."}
    style = style.lower()
    if style not in {"mcq", "truefalse", "mixed"}:
        return {"status": "error", "error_message": "Invalid 'style'. Use mcq|truefalse|mixed."}

    # Normalize and cap transcript length to keep prompts efficient
    seg_models: List[SegmentInput] = []
    try:
        for s in segments:
            seg_models.append(
                SegmentInput(
                    video_title=str(s.get("video_title", "")),
                    speaker_tag=s.get("speaker_tag"),
                    start_time_seconds=int(s.get("start_time_seconds", 0)),
                    transcript=str(s.get("transcript", ""))[:4000],
                    video_link=s.get("video_link"),
                )
            )
    except (ValidationError, Exception) as e:
        return {"status": "error", "error_message": f"Invalid 'segments' shape: {e}"}

    # Build condensed prompt context for the model
    condensed = []
    for seg in seg_models[:30]:  # cap to avoid overlong prompts
        ref = f"Video: {seg.video_title} | Time: {_mm_ss(seg.start_time_seconds)}"
        if seg.speaker_tag is not None:
            ref += f" | Speaker: {seg.speaker_tag}"
        link = None
        if use_timestamp_links and seg.video_link:
            link = f"{seg.video_link}#t={max(0, seg.start_time_seconds - 10)}"
        condensed.append(
            {"reference": ref, "link": link, "transcript": seg.transcript}
        )

    # Call model
    client = get_genai_client()
    config = GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        response_schema=QuizPayload,
        response_mime_type="application/json",
        temperature=0.2,
    )
    user_prompt = {
        "topic": topic,
        "num_questions": int(num_questions),
        "difficulty": difficulty,
        "style": style,
        "include_rationales": bool(include_rationales),
        "segments": condensed,
    }

    try:
        result = client.models.generate_content(
            model=DATA_ENGINEER_MODEL_ID,  # reuse your configured model
            contents=Content(role="user", parts=[Part.from_text(text=json.dumps(user_prompt))]),
            config=config,
        )
        quiz: QuizPayload = result.parsed
    except Exception as e:
        return {"status": "error", "error_message": f"Generation failed: {e}"}

    # Persist artifacts
    quiz_id = uuid.uuid4().hex[:8]
    json_name = f"quiz_{quiz_id}.json"
    md_name = f"quiz_{quiz_id}.md"

    md_lines = [f"# Quiz: {quiz.topic} ({difficulty})", ""]
    for i, q in enumerate(quiz.questions, start=1):
        md_lines.append(f"**Q{i}. {q.stem}**")
        if q.type == "mcq" and q.options:
            for idx, opt in enumerate(q.options):
                abc = "ABCD"[idx] if idx < 4 else f"Option {idx+1}"
                md_lines.append(f"- {abc}. {opt}")
        elif q.type == "truefalse":
            md_lines.append("- True")
            md_lines.append("- False")
        if q.reference:
            md_lines.append(f"_Reference: {q.reference}_")
        if q.link:
            md_lines.append(f"[Watch clip]({q.link})")
        md_lines.append("")

    md_lines.append("---")
    md_lines.append("## Answer Key")
    for i, q in enumerate(quiz.questions, start=1):
        if q.type == "mcq" and q.answer_index is not None and q.options:
            md_lines.append(f"- Q{i}: {chr(65 + q.answer_index)}")
        elif q.type == "truefalse" and q.answer_boolean is not None:
            md_lines.append(f"- Q{i}: {'True' if q.answer_boolean else 'False'}")
        if q.rationale:
            md_lines.append(f"  - Rationale: {q.rationale}")

    try:
        await tool_context.save_artifact(
            json_name,
            Part.from_bytes(
                mime_type="application/json",
                data=quiz.model_dump_json(indent=2).encode("utf-8"),
            ),
        )
        await tool_context.save_artifact(
            md_name,
            Part.from_bytes(
                mime_type="text/markdown",
                data="\n".join(md_lines).encode("utf-8"),
            ),
        )
    except Exception as e:
        return {"status": "error", "error_message": f"Saving artifacts failed: {e}"}

    return {
        "status": "success",
        "quiz_json_artifact": json_name,
        "quiz_markdown_artifact": md_name,
        "quiz_preview": quiz.model_dump(),
    }
