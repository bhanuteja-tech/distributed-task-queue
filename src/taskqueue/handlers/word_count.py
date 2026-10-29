"""Deterministic Word Count Aggregation Task Handler.

Processes text data deterministically, computing frequency distributions and statistics.
"""

import re
from collections import Counter
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.exceptions import PermanentTaskError
from taskqueue.handlers.base import BaseTaskHandler


class WordCountHandler(BaseTaskHandler):
    """Processes textual content and generates deterministic frequency analysis."""

    task_type = "word_count_aggregation"

    async def execute(
        self,
        task_id: str,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> dict[str, Any]:
        text = payload.get("text")
        if text is None:
            raise PermanentTaskError("Missing required parameter: 'text'")

        if not isinstance(text, str):
            raise PermanentTaskError("Parameter 'text' must be a string")

        min_word_length = payload.get("min_word_length", 3)
        top_k = payload.get("top_k", 10)

        # Tokenize words, convert to lowercase, filter punctuation
        words = re.findall(r"\b[a-zA-Z]{" + str(min_word_length) + r",}\b", text.lower())
        word_counts = Counter(words)

        return {
            "total_words": len(words),
            "unique_words": len(word_counts),
            "top_words": dict(word_counts.most_common(top_k)),
            "lexical_diversity": round(len(word_counts) / max(1, len(words)), 4),
        }
