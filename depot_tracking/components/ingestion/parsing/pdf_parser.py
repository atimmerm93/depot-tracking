"""Backward-compatible alias for ING parser module."""

from __future__ import annotations

from depot_tracking.core.parser_errors import UnsupportedPdfDocument
from .ing_pdf_parser import INGPdfParser

__all__ = ["INGPdfParser", "UnsupportedPdfDocument"]
