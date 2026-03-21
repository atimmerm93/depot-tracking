from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass

from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By

from depot_tracking.applications.download.download_service.download_support import SeleniumDownloadSupport
from depot_tracking.applications.download.download_service.selenium_shared import normalize_space, safe_click

UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ"
LOWERCASE = "abcdefghijklmnopqrstuvwxyzäöü"
TARGET_DOCUMENT_TYPES = ("ertragsabrechnung", "abrechnung verkauf", "abrechnung kauf", "depotauszug")


@dataclass(frozen=True)
class RowCandidate:
    doc_type: str
    signature: str
    row_text: str
    row_element: object


class IngRowLocator:
    def collect_row_candidates(self, driver, *, verbose: bool) -> list[RowCandidate]:
        candidates: dict[str, RowCandidate] = {}
        nodes = self._find_type_nodes(driver)
        if verbose:
            print(f"[SELENIUM] Candidate doc-type nodes found: {len(nodes)}", flush=True)

        for idx, node in enumerate(nodes, start=1):
            if verbose and (idx == 1 or idx % 25 == 0):
                print(f"[SELENIUM] Processing candidate node {idx}/{len(nodes)}...", flush=True)
            try:
                row = self.resolve_row_container(node)
                if row is None:
                    continue
                row_text = normalize_space(row.text)
                doc_type = self.extract_target_doc_type(row_text)
                if not doc_type or not row_text or not self.is_message_row(row_text):
                    continue
                signature = hashlib.sha256(f"{doc_type}|{row_text}".encode("utf-8")).hexdigest()
                if signature not in candidates:
                    candidates[signature] = RowCandidate(
                        doc_type=doc_type,
                        signature=signature,
                        row_text=row_text,
                        row_element=row,
                    )
            except StaleElementReferenceException:
                continue

        return list(candidates.values())

    def find_row_by_signature(self, driver, signature: str) -> RowCandidate | None:
        for row in self.collect_row_candidates(driver, verbose=False):
            if row.signature == signature:
                return row
        return None

    def resolve_row_container(self, node) -> object | None:
        lookups = [
            "ancestor::tr[1]",
            "ancestor::*[@role='row'][1]",
            "ancestor::*[contains(@class,'row')][1]",
            "ancestor::*[contains(@class,'message')][1]",
            "ancestor::*[contains(@class,'item')][1]",
            "ancestor::div[1]",
            "parent::*",
        ]

        for xpath in lookups:
            try:
                element = node.find_element(By.XPATH, xpath)
                text = normalize_space(element.text)
                if not text:
                    continue
                if len(text) > 500:
                    continue
                if self.is_message_row(text):
                    return element
            except NoSuchElementException:
                continue
            except StaleElementReferenceException:
                return None
        return None

    def expand_row(self, driver, row_element) -> bool:
        toggle_xpath = (
            ".//button[@aria-expanded='false'] | .//*[@role='button'][@aria-expanded='false'] | "
            ".//button[(normalize-space(.)='' or string-length(normalize-space(.)) <= 2) "
            "and (.//*[name()='svg'] or .//*[local-name()='svg'])] | "
            ".//button[contains(@aria-label,'öff') or contains(@aria-label,'expand') "
            "or contains(@class,'expand') or contains(@class,'toggle') or contains(@class,'chevron')] | "
            ".//*[@role='button'][contains(@class,'chevron') or contains(@class,'toggle')]"
        )

        try:
            toggles = row_element.find_elements(By.XPATH, toggle_xpath)
        except StaleElementReferenceException:
            return False

        for toggle in toggles:
            if safe_click(driver, toggle):
                time.sleep(0.5)
                return True

        if safe_click(driver, row_element):
            time.sleep(0.5)
            return True

        return False

    def find_download_control(self, row_element):
        exact_xpath = (
            ".//button[translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='download']"
            " | .//a[translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='download']"
        )
        try:
            exact = row_element.find_elements(By.XPATH, exact_xpath)
        except StaleElementReferenceException:
            exact = []
        for control in exact:
            try:
                if control.is_displayed() and control.is_enabled():
                    return control
            except StaleElementReferenceException:
                continue

        action_xpath = (
            ".//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download') "
            "or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunter') "
            "or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'pdf') "
            "or contains(translate(normalize-space(@aria-label), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download') "
            "or contains(translate(normalize-space(@aria-label), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunter') "
            "or contains(@href, '.pdf')]"
            " | "
            ".//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download') "
            "or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunter') "
            "or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'pdf') "
            "or contains(translate(normalize-space(@aria-label), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download') "
            "or contains(translate(normalize-space(@aria-label), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunter')]"
        )

        contexts = [row_element]
        for lookup in ("following-sibling::*[1]", "following-sibling::*[2]"):
            try:
                contexts.append(row_element.find_element(By.XPATH, lookup))
            except NoSuchElementException:
                continue
            except StaleElementReferenceException:
                continue

        for context in contexts:
            try:
                controls = context.find_elements(By.XPATH, action_xpath)
            except StaleElementReferenceException:
                continue
            for control in controls:
                try:
                    if control.is_displayed() and control.is_enabled():
                        return control
                except StaleElementReferenceException:
                    continue
        return None

    @staticmethod
    def is_download_like_url(url: str | None) -> bool:
        return SeleniumDownloadSupport.is_download_like_url(url)

    @staticmethod
    def extract_target_doc_type(value: str) -> str | None:
        normalized = normalize_space(value.lower())
        for target in TARGET_DOCUMENT_TYPES:
            if target in normalized:
                return target
        return None

    def is_message_row(self, row_text: str) -> bool:
        if self.extract_target_doc_type(row_text) is None:
            return False
        return re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", row_text) is not None

    def _find_type_nodes(self, driver) -> list:
        conditions = [
            (
                f"contains(translate(normalize-space(text()), '{UPPERCASE}', '{LOWERCASE}'), '{target}') "
                "and string-length(normalize-space(text())) <= 80"
            )
            for target in TARGET_DOCUMENT_TYPES
        ]
        xpath = f"//div[{' or '.join(conditions)}] | //span[{' or '.join(conditions)}] | //p[{' or '.join(conditions)}]"
        try:
            nodes = driver.find_elements(By.XPATH, xpath)
            return nodes[:400]
        except Exception:
            return []
