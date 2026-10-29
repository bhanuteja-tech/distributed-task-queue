"""Task handlers registration and setup."""

from taskqueue.handlers.base import BaseTaskHandler, HandlerRegistry, registry
from taskqueue.handlers.ledger import LedgerBatchHandler
from taskqueue.handlers.report import ReportGenerationHandler
from taskqueue.handlers.word_count import WordCountHandler

# Automatically register default handlers
registry.register(ReportGenerationHandler())
registry.register(LedgerBatchHandler())
registry.register(WordCountHandler())

__all__ = [
    "BaseTaskHandler",
    "HandlerRegistry",
    "registry",
    "ReportGenerationHandler",
    "LedgerBatchHandler",
    "WordCountHandler",
]

