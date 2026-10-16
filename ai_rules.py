"""
AI scoring rules: gate 4 of the old LegTracker ingestion, copied verbatim.

Everything between the COPIED markers was extracted unchanged (by
`ast.get_source_segment`) from the old backend/app/services/ai_processing.py:
the system prompt, the per-bill prompt builder, the 13 allowed topics, the
input caps, the tolerant JSON parser and the output normaliser. The old
threshold (sync.AI_THRESHOLD = 30) and embedding text recipe
(semantic_scoring.bill_text_for_embedding) are reproduced below them.
"""
from __future__ import annotations

import ast
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ==================== COPIED from ai_processing.py ====================
RELEVANCE_TOPICS = [
    "Animal welfare",
    "Wildlife conservation",
    "Endangered species",
    "Livestock and agriculture",
    "Aquatic life and fisheries",
    "Veterinary and animal health",
    "Hunting and trapping",
    "Animal cruelty and crimes",
    "Service and companion animals",
    "Habitat and land use",
    "Environmental protection",
    "Food safety and labeling",
    "Trafficking and smuggling",
]


SYSTEM_PROMPT = (
    "You are an analyst for an animal-welfare and environmental legislative tracking "
    "system. You score how relevant a U.S. congressional bill is to animal protection, "
    "wildlife, and related environmental concerns, and you write a concise plain-language "
    "impact summary for advocacy staff. Be factual and grounded only in the provided text. "
    "Always respond with a single JSON object and nothing else."
)


MAX_SUMMARY_CHARS = 8000


MAX_ACTION_CHARS = 2000


def _strip_html(text: Optional[str]) -> Optional[str]:
    """Remove HTML tags and collapse whitespace from a summary blob."""
    if not text:
        return text
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", no_tags).strip()


def _build_user_prompt(document: LegislativeDocument) -> str:
    """Assemble the per-bill prompt from the fields we already have on the document."""
    summary = _strip_html(document.official_summary) or "(no official summary available)"
    if len(summary) > MAX_SUMMARY_CHARS:
        summary = summary[:MAX_SUMMARY_CHARS] + " ...[truncated]"

    last_action = (document.last_action_text or "")[:MAX_ACTION_CHARS]
    subjects = [s.name for s in document.subjects] if document.subjects else []

    # The display title is often just an acronym ("WATCH Act"); the official title
    # is what actually describes the bill, so give the model both when they differ.
    official_title = getattr(document, "official_title", None)
    title_lines = [f"Title: {document.title or '(untitled)'}"]
    if official_title and official_title != document.title:
        title_lines.append(f"Official title: {official_title}")

    parts = [
        f"Bill: {document.source_id}",
        *title_lines,
        f"Policy area: {document.policy_area or '(none)'}",
        f"Congress.gov subjects: {', '.join(subjects) if subjects else '(none)'}",
        f"Latest action: {last_action or '(none)'}",
        "",
        "Official summary:",
        summary,
        "",
        "Allowed relevance topics (choose only from this list):",
        ", ".join(RELEVANCE_TOPICS),
        "",
        "Respond with a JSON object with exactly these keys:",
        '  "relevance_score": integer 0-100 (how relevant to animal welfare / wildlife / '
        "related environmental concerns; 0 = unrelated, 100 = central focus),",
        '  "relevance_topics": array of strings drawn ONLY from the allowed topics list above,',
        '  "relevance_rationale": one or two sentences justifying the score,',
        '  "ai_summary": a 2-4 sentence plain-language impact summary for advocacy staff.',
    ]
    return "\n".join(parts)


def _parse_llm_json(content: str) -> Dict[str, Any]:
    """Parse the model's JSON reply, tolerating ```json fences or surrounding prose."""
    first_exc = None
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        first_exc = e
        logger.debug("Initial JSON parse failed, attempting sanitization: %s", e)

    def _sanitize_control_chars(s: str) -> str:
        """Replace raw control characters with safe JSON escapes ONLY inside string values."""
        def _escape_inside_quotes(match: re.Match) -> str:
            val = match.group(0)
            def _repl(m: re.Match) -> str:
                ch = m.group(0)
                if ch == "\n":
                    return "\\n"
                if ch == "\r":
                    return "\\r"
                if ch == "\t":
                    return "\\t"
                return "\\u%04x" % ord(ch)
            sanitized_val = re.sub(r"[\x00-\x1f]", _repl, val[1:-1])
            return f'"{sanitized_val}"'

        return re.sub(r'"(?:[^"\\]|\\.)*"', _escape_inside_quotes, s)

    # Try sanitizing the whole content and parse again
    try:
        sanitized = _sanitize_control_chars(content)
        return json.loads(sanitized)
    except json.JSONDecodeError:
        logger.debug("Sanitized parse failed, attempting to extract JSON object from sanitized content.")

    # As a last resort try to extract the first {...} block, sanitize it, and parse
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        # Re-raise the original parse error to preserve context
        raise first_exc

    try:
        obj_text = _sanitize_control_chars(match.group(0))
        return json.loads(obj_text)
    except json.JSONDecodeError:
        pass

    # Last resort: model may have returned a Python dict literal (single quotes).
    # ast.literal_eval is safe — it only evaluates literals, not arbitrary code.
    try:
        result = ast.literal_eval(match.group(0))
        if isinstance(result, dict):
            return result
    except Exception:
        pass

    logger.exception("Failed to parse LLM JSON after sanitization and extraction.")
    raise first_exc


def _normalize_output(raw: Dict[str, Any]) -> Tuple[int, List[str], str, str]:
    """Coerce and validate the model output into the columns we store."""
    try:
        score = int(round(float(raw.get("relevance_score", 0))))
    except (TypeError, ValueError):
        score = 0
    score = max(0, min(100, score))

    topics_raw = raw.get("relevance_topics") or []
    if isinstance(topics_raw, str):
        topics_raw = [topics_raw]
    allowed = set(RELEVANCE_TOPICS)
    topics = [t for t in topics_raw if isinstance(t, str) and t in allowed]

    rationale = str(raw.get("relevance_rationale") or "").strip()
    summary = str(raw.get("ai_summary") or "").strip()

    return score, topics, rationale, summary

# ======================= end of copied code =======================

# Old sync.AI_THRESHOLD: scores below this were dropped as low_score.
AI_THRESHOLD = 30


def embedding_text(bill) -> str:
    """Old semantic_scoring.bill_text_for_embedding(): title, policy area, the
    bill's (approved) subjects, the AI summary (else the CRS summary) and the
    official title, assembled by the same build_candidate_text()."""
    import rules
    subject_names = [s.name for s in bill.subjects] if bill.subjects else []
    summary = bill.ai_summary or bill.official_summary
    return rules.build_candidate_text(bill.title, bill.policy_area, subject_names, summary,
                                      official_title=getattr(bill, "official_title", None))