from __future__ import annotations


class IdentifierCanonicalizer:
    def __init__(
        self,
        *,
        wkn_alias_to_canonical: dict[str, str] | None = None,
        isin_alias_to_canonical: dict[str, str] | None = None,
        consors_legacy_wkn_alias_to_canonical: dict[str, str] | None = None,
    ) -> None:
        self._wkn_alias_to_canonical = wkn_alias_to_canonical or {
            "A3DUN5": "A2ABYA",
        }
        self._isin_alias_to_canonical = isin_alias_to_canonical or {
            "US0494681010": "GB00BZ09BD16",
        }
        self._consors_legacy_wkn_alias_to_canonical = consors_legacy_wkn_alias_to_canonical or {
            "018556": "855681",
            "018631": "863186",
            "019184": "918422",
            "191842": "918422",
            "018801": "880135",
            "188013": "880135",
            "9A1J4U": "A1J4U4",
            "109098": "909800",
        }

    def canonicalize(self, *, wkn: str, isin: str | None) -> tuple[str, str | None]:
        resolved_wkn = (wkn or "").upper()
        resolved_isin = isin.upper() if isin else None

        if resolved_wkn in self._wkn_alias_to_canonical:
            resolved_wkn = self._wkn_alias_to_canonical[resolved_wkn]

        if resolved_isin and resolved_isin in self._isin_alias_to_canonical:
            resolved_isin = self._isin_alias_to_canonical[resolved_isin]

        return resolved_wkn, resolved_isin

    def is_legacy_consors_alias_wkn(self, wkn: str) -> bool:
        return (wkn or "").upper() in self._consors_legacy_wkn_alias_to_canonical

    def iter_legacy_consors_aliases(self) -> tuple[tuple[str, str], ...]:
        return tuple(self._consors_legacy_wkn_alias_to_canonical.items())
