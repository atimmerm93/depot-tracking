from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By

from depot_tracking.applications.download.download_service.consors.consors_row_locator import ConsorsRowLocator, \
    RowCandidate
from depot_tracking.applications.download.download_service.download_support import SeleniumDownloadSupport
from depot_tracking.applications.download.download_service.selenium_shared import (
    SeleniumDownloadConfig,
    attach_to_current_tab,
    configure_download_directory,
    load_download_state,
    normalize_space,
    safe_click,
    save_download_state,
)


@dataclass(frozen=True)
class RowProcessingResult:
    found_increment: int = 0
    downloaded_increment: int = 0
    skipped_increment: int = 0
    errors_increment: int = 0

    def apply_to(self, stats: dict[str, int]) -> None:
        stats["found"] += self.found_increment
        stats["downloaded"] += self.downloaded_increment
        stats["skipped"] += self.skipped_increment
        stats["errors"] += self.errors_increment


class ConsorsSeleniumInboxDownloader:
    def __init__(self, config: SeleniumDownloadConfig, support: SeleniumDownloadSupport) -> None:
        self.config = config
        self._support = support
        self.download_dir = Path(config.download_dir)
        self.state_file = config.state_file or self.download_dir / ".selenium_downloaded_rows_consors.json"
        self.archive_handle: str | None = None
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.watch_download_dirs = self._support.build_watch_download_dirs(self.download_dir,
                                                                           config.fallback_download_dirs)
        self._row_locator = ConsorsRowLocator()

    def run(self) -> dict[str, int]:
        stats = {"found": 0, "downloaded": 0, "skipped": 0, "errors": 0}
        state = set() if self.config.reset_state else load_download_state(self.state_file)

        print(f"[SELENIUM][CONSORS] Attaching to Chrome at {self.config.debugger_address}...", flush=True)
        driver = attach_to_current_tab(self.config.debugger_address)
        try:
            self.archive_handle = self._select_archive_handle(driver)
            self._switch_to_archive_tab(driver)
            configure_download_directory(driver, self.download_dir)
            self._scan_archive_pages(driver, state=state, stats=stats)

            save_download_state(self.state_file, state)
            return stats
        finally:
            try:
                driver.service.stop()  # type: ignore[attr-defined]
            except Exception:
                pass

    def _scan_archive_pages(self, driver, *, state: set[str], stats: dict[str, int]) -> None:
        found_signatures: set[str] = set()
        page_index = 1
        max_pages = 200
        while page_index <= max_pages:
            rows = self._load_page_rows(driver, page_index=page_index)
            if not rows:
                if page_index == 1:
                    self._save_first_page_debug_snapshot(driver)
                if page_index > 1:
                    break
            if self._process_page_rows(
                    driver,
                    rows=rows,
                    page_index=page_index,
                    state=state,
                    stats=stats,
                    found_signatures=found_signatures,
            ):
                return
            if not rows or not self._go_to_next_page(driver):
                break
            page_index += 1

    def _load_page_rows(self, driver, *, page_index: int) -> list[RowCandidate]:
        self._switch_to_archive_tab(driver)
        print(f"[SELENIUM][CONSORS] Scanning page {page_index}...", flush=True)
        rows = self._row_locator.collect_row_candidates(driver)
        if rows:
            return rows
        for retry in range(2):
            time.sleep(1.0)
            rows = self._row_locator.collect_row_candidates(driver)
            if rows:
                print(
                    f"[SELENIUM][CONSORS] Page {page_index} populated after retry {retry + 1}.",
                    flush=True,
                )
                return rows
        return rows

    def _process_page_rows(
            self,
            driver,
            *,
            rows: list[RowCandidate],
            page_index: int,
            state: set[str],
            stats: dict[str, int],
            found_signatures: set[str],
    ) -> bool:
        for row_index, row in enumerate(rows, start=1):
            if self.config.max_documents is not None and stats["downloaded"] >= self.config.max_documents:
                return True
            result = self._process_row(
                driver,
                row=row,
                row_index=row_index,
                row_count=len(rows),
                page_index=page_index,
                state=state,
                found_signatures=found_signatures,
            )
            result.apply_to(stats)
        return False

    def _process_row(
            self,
            driver,
            *,
            row: RowCandidate,
            row_index: int,
            row_count: int,
            page_index: int,
            state: set[str],
            found_signatures: set[str],
    ) -> RowProcessingResult:
        self._switch_to_archive_tab(driver)
        print(
            f"[SELENIUM][CONSORS] Processing page {page_index} row {row_index}/{row_count}: "
            f"{row.doc_type} | {row.row_text[:100]}",
            flush=True,
        )
        row = self._refresh_row_if_possible(driver, row)
        found_increment = 0 if row.signature in found_signatures else 1
        found_signatures.add(row.signature)

        if row.signature in state:
            return RowProcessingResult(found_increment=found_increment, skipped_increment=1)

        downloaded = self._download_row_document(driver, row)
        if downloaded is None:
            print(f"[SELENIUM][CONSORS][ERROR] Download timeout for row: {row.row_text[:120]}", flush=True)
            return RowProcessingResult(found_increment=found_increment, errors_increment=1)

        state.add(row.signature)
        print(f"[SELENIUM][CONSORS][OK] Downloaded {downloaded.name} ({row.doc_type})", flush=True)
        return RowProcessingResult(found_increment=found_increment, downloaded_increment=1)

    def _download_row_document(self, driver, row: RowCandidate) -> Path | None:
        existing = self._snapshot_pdf_states()
        direct_url = self._extract_row_document_url(driver, row)
        if direct_url:
            direct_download = self._download_via_session_url(driver, direct_url, row.doc_type)
            if direct_download is not None:
                return direct_download

        buttons = self._find_download_buttons(row.row_element)
        if not buttons:
            refreshed = self._refresh_row_if_possible(driver, row)
            if refreshed is not row:
                buttons = self._find_download_buttons(refreshed.row_element)
                row = refreshed

        downloaded = self._download_via_buttons_or_js(driver, row=row, buttons=buttons, existing=existing)
        if downloaded is not None:
            return downloaded

        if buttons:
            refreshed = self._refresh_row_if_possible(driver, row)
            if refreshed is not row:
                retry_buttons = self._find_download_buttons(refreshed.row_element)
                if retry_buttons:
                    downloaded = self._attempt_row_download(driver, retry_buttons, existing)
                    row = refreshed
                    if downloaded is not None:
                        return downloaded

        if direct_url:
            downloaded = self._download_via_session_url(driver, direct_url, row.doc_type)
            if downloaded is not None:
                return downloaded

        if self._click_download_via_js(driver, row):
            return self._wait_for_download_change(
                existing,
                wait_seconds=min(self.config.download_wait_seconds, 10.0),
            )
        return None

    def _download_via_buttons_or_js(self, driver, *, row: RowCandidate, buttons: list,
                                    existing: dict[str, tuple[int, int]]) -> Path | None:
        if buttons:
            return self._attempt_row_download(driver, buttons, existing)
        if self._click_download_via_js(driver, row):
            return self._wait_for_download_change(
                existing,
                wait_seconds=min(self.config.download_wait_seconds, 10.0),
            )
        print(f"[SELENIUM][CONSORS][ERROR] No download button for row: {row.row_text[:120]}", flush=True)
        return None

    def _refresh_row_if_possible(self, driver, row: RowCandidate) -> RowCandidate:
        refreshed = self._find_row_by_signature(driver, row.signature)
        return refreshed if refreshed is not None else row

    def _save_first_page_debug_snapshot(self, driver) -> None:
        debug_html = self.download_dir / "_consors_debug_page1.html"
        debug_png = self.download_dir / "_consors_debug_page1.png"
        try:
            debug_html.write_text(driver.page_source, encoding="utf-8")
            driver.save_screenshot(str(debug_png))
            print(
                f"[SELENIUM][CONSORS][DEBUG] Saved page snapshot: {debug_html.name}, {debug_png.name}",
                flush=True,
            )
        except Exception:
            pass

    def _collect_row_candidates(self, driver, *, verbose: bool = True) -> list[RowCandidate]:
        return self._row_locator.collect_row_candidates(driver, verbose=verbose)

    def _find_row_by_signature(self, driver, signature: str) -> RowCandidate | None:
        return self._row_locator.find_row_by_signature(driver, signature)

    @staticmethod
    def _build_row_signature(doc_type: str, row_text: str) -> str:
        return ConsorsRowLocator.build_row_signature(doc_type, row_text)

    @staticmethod
    def _extract_doc_type(text: str) -> str | None:
        return ConsorsRowLocator.extract_doc_type(text)

    def _find_download_buttons(self, row) -> list:
        return self._row_locator.find_download_buttons(row)

    @staticmethod
    def _score_download_control(control) -> int:
        return ConsorsRowLocator.score_download_control(control)

    def _attempt_row_download(self, driver, buttons: list, existing: dict[str, tuple[int, int]]) -> Path | None:
        # Fast failure keeps long runs responsive; stale rows are retried once by caller after re-find.
        quick_wait = min(5.0, self.config.click_download_wait_seconds, self.config.download_wait_seconds)
        settle_wait = min(8.0, max(self.config.download_wait_seconds - quick_wait, 0.0))

        if not buttons:
            return None
        self._switch_to_archive_tab(driver)
        if not safe_click(driver, buttons[0]):
            return None

        downloaded = self._wait_for_download_change(existing, wait_seconds=quick_wait)
        if downloaded is not None:
            return downloaded

        if settle_wait > 0:
            return self._wait_for_download_change(existing, wait_seconds=settle_wait)
        return None

    def _click_download_via_js(self, driver, row: RowCandidate) -> bool:
        snippet = self._build_row_text_snippet(row.row_text)
        script = r"""
const rowSnippet = (arguments[0] || '').toLowerCase();
const docType = (arguments[1] || '').toLowerCase();

const normalize = (value) => (value || '')
  .toLowerCase()
  .replace(/ungelesen/g, '')
  .replace(/\s+/g, ' ')
  .trim();

const dateRegex = /\b\d{2}\.\d{2}\.\d{4}\b/;

const walk = (root, out) => {
  if (!root) return;
  const kids = root.children || [];
  for (const child of kids) {
    out.push(child);
    if (child.shadowRoot) walk(child.shadowRoot, out);
    walk(child, out);
  }
};

const all = [];
walk(document, all);

const rows = [];
for (const el of all) {
  if (!(el instanceof HTMLElement)) continue;
  const txt = normalize(el.innerText || '');
  if (!txt || txt.length > 420) continue;
  if (!dateRegex.test(txt)) continue;
  if (docType && !txt.includes(docType)) continue;
  if (rowSnippet && !txt.includes(rowSnippet)) continue;
  rows.push(el);
}

const scoreControl = (el) => {
  const text = normalize(el.innerText || '');
  const aria = normalize(el.getAttribute('aria-label') || '');
  const title = normalize(el.getAttribute('title') || '');
  const href = normalize(el.getAttribute('href') || '');
  const cls = normalize(el.getAttribute('class') || '');
  const marker = `${text} ${aria} ${title} ${href} ${cls}`;
  if (!marker) return -1;
  if (marker.includes('archiv') || marker.includes('detail') || marker.includes('info')) return -100;
  let score = 0;
  if (marker.includes('download') || marker.includes('herunter')) score += 100;
  if (href.includes('.pdf')) score += 90;
  if (href.includes('download')) score += 80;
  if (cls.includes('download')) score += 50;
  return score;
};

for (const row of rows) {
  const controls = [];
  const descendants = [];
  walk(row, descendants);
  for (const el of descendants) {
    if (!(el instanceof HTMLElement)) continue;
    const tag = (el.tagName || '').toLowerCase();
    const role = normalize(el.getAttribute('role') || '');
    if (!(tag === 'a' || tag === 'button' || role === 'button' || el.getAttribute('onclick'))) continue;
    const rect = el.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) continue;
    if (el.hasAttribute('disabled')) continue;
    controls.push(el);
  }
  if (!controls.length) continue;

  controls.sort((a, b) => {
    const sa = scoreControl(a);
    const sb = scoreControl(b);
    if (sa !== sb) return sb - sa;
    return a.getBoundingClientRect().left - b.getBoundingClientRect().left;
  });
  const chosen = controls[0];
  if (!chosen) continue;
  try {
    chosen.click();
    return true;
  } catch (e) {
    try {
      chosen.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
      return true;
    } catch (_e) {}
  }
}

return false;
"""
        try:
            clicked = bool(driver.execute_script(script, snippet, row.doc_type))
        except Exception:
            clicked = False
        if clicked:
            print(f"[SELENIUM][CONSORS] JS fallback click used for: {row.row_text[:100]}", flush=True)
        return clicked

    @staticmethod
    def _build_row_text_snippet(row_text: str) -> str:
        return ConsorsRowLocator.build_row_text_snippet(row_text)

    def _select_archive_handle(self, driver) -> str:
        handles = list(driver.window_handles)
        best_handle = driver.current_window_handle
        best_score = -10 ** 9

        for handle in handles:
            try:
                driver.switch_to.window(handle)
                url = (driver.current_url or "").lower()
                title = (driver.title or "").lower()
                body_text = normalize_space(driver.find_element(By.TAG_NAME, "body").text or "").lower()
            except Exception:
                continue

            score = 0
            if "consorsbank.de/web/mein-konto-und-depot/online-archiv" in url:
                score += 200
            if "online-archiv" in title:
                score += 120
            if "consorsbank" in title:
                score += 80
            if "zeilenumbruch <!doctype html>" in body_text:
                score -= 300
            if "devtools" in title or url.startswith("devtools://"):
                score -= 500
            if url.startswith("chrome://"):
                score -= 400

            if score > best_score:
                best_score = score
                best_handle = handle

        driver.switch_to.window(best_handle)
        print(f"[SELENIUM][CONSORS] Using browser tab: {driver.current_url}", flush=True)
        return best_handle

    def _switch_to_archive_tab(self, driver) -> None:
        if self.archive_handle is None:
            self.archive_handle = self._select_archive_handle(driver)
            return
        try:
            if driver.current_window_handle != self.archive_handle:
                driver.switch_to.window(self.archive_handle)
        except Exception:
            self.archive_handle = self._select_archive_handle(driver)

    def _extract_row_document_url(self, driver, row: RowCandidate) -> str | None:
        candidates: list[tuple[str, str]] = []
        try:
            anchors = row.row_element.find_elements(By.XPATH, ".//a[@href]")
        except StaleElementReferenceException:
            anchors = []

        for anchor in anchors:
            try:
                href = (anchor.get_attribute("href") or "").strip()
                text = normalize_space(anchor.text or "")
            except StaleElementReferenceException:
                continue
            if href:
                candidates.append((href, text))

        if not candidates:
            try:
                anchors = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href,'/web-document-service/api/') or contains(@href,'/documents/content/')]",
                )
            except Exception:
                anchors = []
            for anchor in anchors:
                try:
                    href = (anchor.get_attribute("href") or "").strip()
                    text = normalize_space(anchor.text or "")
                except StaleElementReferenceException:
                    continue
                if href:
                    candidates.append((href, text))

        if not candidates:
            return None

        row_norm = normalize_space(row.row_text).lower()
        row_tokens = set(re.findall(r"[a-z0-9]{4,}", row_norm))
        date_match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", row.row_text)
        row_date = date_match.group(0) if date_match else ""

        scored: list[tuple[int, str]] = []
        for href, text in candidates:
            href_lower = href.lower()
            text_norm = normalize_space(text).lower()
            score = 0
            if "/web-document-service/api/" in href_lower:
                score += 90
            if "/documents/content/" in href_lower:
                score += 70
            if text_norm:
                score += 10
            if text_norm and text_norm in row_norm:
                score += 80
            if row_date and row_date in text_norm:
                score += 35
            if text_norm and row_tokens:
                text_tokens = set(re.findall(r"[a-z0-9]{4,}", text_norm))
                if text_tokens:
                    overlap = len(row_tokens & text_tokens)
                    score += min(40, overlap * 5)
            scored.append((score, href))

        scored.sort(reverse=True)
        best_score, best_href = scored[0]
        if best_score < 70:
            return None
        return urljoin(driver.current_url, best_href)

    def _snapshot_pdf_states(self) -> dict[str, tuple[int, int]]:
        return self._support.snapshot_pdf_states(self.watch_download_dirs)

    def _download_via_session_url(self, driver, url: str, doc_type: str) -> Path | None:
        return self._support.download_via_session_url(driver, url, download_dir=self.download_dir, doc_type=doc_type)

    def _wait_for_download_change(self, previous: dict[str, tuple[int, int]], *, wait_seconds: float) -> Path | None:
        return self._support.wait_for_download_change(self.watch_download_dirs, previous, wait_seconds=wait_seconds)

    def _find_changed_pdf(self, previous: dict[str, tuple[int, int]]) -> Path | None:
        return self._support.find_changed_pdf(self.watch_download_dirs, previous)

    def _go_to_next_page(self, driver) -> bool:
        xpath = (
            "//a[contains(normalize-space(.), 'Nächste Seite') and not(contains(@class,'disabled'))] | "
            "//button[contains(normalize-space(.), 'Nächste Seite') and not(@disabled)]"
        )
        try:
            buttons = driver.find_elements(By.XPATH, xpath)
        except Exception:
            return False
        if not buttons:
            return False
        next_button = buttons[0]
        if not safe_click(driver, next_button):
            return False
        time.sleep(1.2)
        return True


def download_consors_documents_from_current_tab(config: SeleniumDownloadConfig) -> dict[str, int]:
    return ConsorsSeleniumInboxDownloader(config, support=SeleniumDownloadSupport()).run()
