from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By

from depot_tracking.applications.download.download_service.selenium_shared import normalize_space

TARGET_DOCUMENT_TYPES = (
    "verkauf",
    "jahresdepotauszug",
    "jahresdepotauszug wertpapiere",
    "quartalsdepotauszug",
    "quartalsdepotauszug wertpapiere",
    "kauf",
    "dividendengutschrift",
)

UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ"
LOWERCASE = "abcdefghijklmnopqrstuvwxyzäöü"


@dataclass(frozen=True)
class RowCandidate:
    doc_type: str
    signature: str
    row_text: str
    row_element: object


class ConsorsRowLocator:
    def collect_row_candidates(self, driver, *, verbose: bool = True) -> list[RowCandidate]:
        xpath_conditions = [
            f"contains(translate(normalize-space(.), '{UPPERCASE}', '{LOWERCASE}'), '{target}')"
            for target in TARGET_DOCUMENT_TYPES
        ]
        xpath = (
            f"//td[{' or '.join(xpath_conditions)}] | "
            f"//div[{' or '.join(xpath_conditions)}] | "
            f"//span[{' or '.join(xpath_conditions)}] | "
            f"//a[{' or '.join(xpath_conditions)}] | "
            f"//p[{' or '.join(xpath_conditions)}]"
        )
        try:
            nodes = driver.find_elements(By.XPATH, xpath)
        except Exception:
            return []

        candidates: dict[str, RowCandidate] = {}
        for node in nodes:
            row = self.resolve_row_container(node)
            if row is None:
                continue

            try:
                row_text = normalize_space(row.text)
            except StaleElementReferenceException:
                continue
            if not self._is_candidate_row_text(row_text):
                continue
            doc_type = self.extract_doc_type(row_text)
            if doc_type is None:
                continue

            signature = self.build_row_signature(doc_type, row_text)
            candidates[signature] = RowCandidate(
                doc_type=doc_type,
                signature=signature,
                row_text=row_text,
                row_element=row,
            )

        if not candidates:
            for row in self._collect_fallback_rows(driver):
                try:
                    row_text = normalize_space(row.text)
                except StaleElementReferenceException:
                    continue
                if not self._is_candidate_row_text(row_text):
                    continue
                doc_type = self.extract_doc_type(row_text)
                if doc_type is None:
                    continue
                if not self.find_download_buttons(row):
                    continue
                signature = self.build_row_signature(doc_type, row_text)
                candidates[signature] = RowCandidate(
                    doc_type=doc_type,
                    signature=signature,
                    row_text=row_text,
                    row_element=row,
                )

        if verbose:
            print(f"[SELENIUM][CONSORS] Matching rows on page: {len(candidates)}", flush=True)
        return list(candidates.values())

    def find_row_by_signature(self, driver, signature: str) -> RowCandidate | None:
        for row in self.collect_row_candidates(driver, verbose=False):
            if row.signature == signature:
                return row
        return None

    def resolve_row_container(self, node):
        lookups = (
            "ancestor::tr[1]",
            "ancestor::*[@role='row'][1]",
            "ancestor::*[contains(@class,'row')][1]",
            "ancestor::*[contains(@class,'message')][1]",
            "ancestor::*[contains(@class,'item')][1]",
            "ancestor::li[1]",
            "parent::*",
        )
        for xpath in lookups:
            try:
                element = node.find_element(By.XPATH, xpath)
            except (NoSuchElementException, StaleElementReferenceException):
                continue
            try:
                text = normalize_space(element.text)
            except StaleElementReferenceException:
                continue
            if not text or len(text) > 1200:
                continue
            if not self._is_candidate_row_text(text):
                continue
            if not self.has_potential_action_controls(element):
                continue
            return element
        return None

    @staticmethod
    def has_potential_action_controls(element) -> bool:
        xpath = (
            ".//a | .//button | .//*[@role='button'] | .//*[@onclick] | "
            ".//*[contains(translate(@class, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'icon')] | "
            ".//*[contains(translate(@class, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]"
        )
        try:
            controls = element.find_elements(By.XPATH, xpath)
        except StaleElementReferenceException:
            return False

        for control in controls:
            try:
                if control.is_displayed():
                    return True
            except StaleElementReferenceException:
                continue
        return False

    @staticmethod
    def extract_doc_type(text: str) -> str | None:
        lowered = normalize_space(text).lower()
        for target in TARGET_DOCUMENT_TYPES:
            if target in lowered:
                return target
        return None

    def find_download_buttons(self, row) -> list:
        try:
            controls = row.find_elements(By.XPATH, ".//a | .//button | .//*[@role='button'] | .//*[@onclick]")
        except StaleElementReferenceException:
            return []

        ranked: list[tuple[int, float, object]] = []
        for control in controls:
            try:
                if not control.is_displayed() or not control.is_enabled():
                    continue
                score = self.score_download_control(control)
                if score <= 0:
                    continue
                x_pos = float(control.location.get("x", 0))
                ranked.append((score, -x_pos, control))
            except StaleElementReferenceException:
                continue

        if ranked:
            ranked.sort(reverse=True)
            return [item[2] for item in ranked]

        try:
            icons = row.find_elements(
                By.XPATH,
                ".//a[.//*[local-name()='svg']] | "
                ".//button[.//*[local-name()='svg']] | "
                ".//*[@role='button'][.//*[local-name()='svg']] | "
                ".//*[contains(translate(@class, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')]",
            )
        except StaleElementReferenceException:
            return []

        visible_icons: list[tuple[float, object]] = []
        for control in icons:
            try:
                if control.is_displayed() and control.is_enabled():
                    visible_icons.append((float(control.location.get("x", 0)), control))
            except StaleElementReferenceException:
                continue
        if not visible_icons:
            return []

        try:
            row_rect = row.rect
            row_x = float(row_rect.get("x", 0.0))
            row_w = float(row_rect.get("width", 0.0))
        except StaleElementReferenceException:
            row_x = 0.0
            row_w = 0.0
        right_threshold = row_x + (row_w * 0.65) if row_w > 0 else 0.0
        right_side_icons = [item for item in visible_icons if item[0] >= right_threshold]
        candidates = right_side_icons if right_side_icons else visible_icons
        candidates.sort(key=lambda item: item[0])
        return [item[1] for item in candidates]

    @staticmethod
    def score_download_control(control) -> int:
        def _safe_attr(name: str) -> str:
            try:
                return normalize_space(control.get_attribute(name) or "").lower()
            except StaleElementReferenceException:
                return ""

        try:
            text = normalize_space(control.text or "").lower()
        except StaleElementReferenceException:
            return 0

        aria = _safe_attr("aria-label")
        title = _safe_attr("title")
        href = _safe_attr("href")
        css_class = _safe_attr("class")
        testid = _safe_attr("data-testid")
        marker = " ".join([text, aria, title, href, css_class, testid])
        if not marker:
            return 0

        if any(term in marker for term in ("archiv", "archive", "details", "detail", "info", "delete", "löschen")):
            return -100

        score = 0
        if "download" in marker or "herunter" in marker:
            score += 100
        if ".pdf" in href:
            score += 90
        if "download" in href:
            score += 80
        if "download" in css_class or "download" in testid:
            score += 50
        return score

    @staticmethod
    def build_row_signature(doc_type: str, row_text: str) -> str:
        text = normalize_space(row_text).lower()
        text = text.replace("ungelesen", "")
        text = re.sub(r"\s+", " ", text).strip()
        return hashlib.sha256(f"{doc_type}|{text}".encode("utf-8")).hexdigest()

    @staticmethod
    def build_row_text_snippet(row_text: str) -> str:
        normalized = normalize_space(row_text).lower().replace("ungelesen", "")
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized[:80]

    def _collect_fallback_rows(self, driver) -> list:
        try:
            return driver.find_elements(
                By.XPATH,
                "//tr[.//a or .//button] | //*[@role='row'] | //li[.//a or .//button]",
            )
        except Exception:
            return []

    @staticmethod
    def _is_candidate_row_text(row_text: str) -> bool:
        if not row_text:
            return False
        if re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", row_text) is None:
            return False
        return len(re.findall(r"\b\d{2}\.\d{2}\.\d{4}\b", row_text)) <= 1
