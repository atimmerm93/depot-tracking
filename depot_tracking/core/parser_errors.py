from __future__ import annotations


class PdfParseError(ValueError):
    pass


class UnsupportedPdfDocument(PdfParseError):
    pass
