from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import urljoin

from selenium import webdriver

from depot_tracking.applications.download.download_service.download_support import SeleniumDownloadSupport
from depot_tracking.applications.download.download_service.ing.ing_row_locator import IngRowLocator, RowCandidate
from depot_tracking.applications.download.download_service.selenium_shared import (
    SeleniumDownloadConfig,
    attach_to_current_tab,
    configure_download_directory,
    load_download_state,
    safe_click,
    save_download_state,
    snapshot_existing_pdfs,
    wait_for_download,
)


class IngSeleniumInboxDownloader:
    def __init__(self, config: SeleniumDownloadConfig, support: SeleniumDownloadSupport) -> None:
        self.config = config
        self._support = support
        self.download_dir = Path(config.download_dir)
        self.state_file = config.state_file or self.download_dir / ".selenium_downloaded_rows.json"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.watch_download_dirs = self._support.build_watch_download_dirs(self.download_dir,
                                                                           config.fallback_download_dirs)
        self._row_locator = IngRowLocator()

    def run(self) -> dict[str, int]:
        stats = {"found": 0, "downloaded": 0, "skipped": 0, "errors": 0}
        state = set() if self.config.reset_state else load_download_state(self.state_file)

        print(f"[SELENIUM] Attaching to Chrome at {self.config.debugger_address}...", flush=True)
        driver = attach_to_current_tab(self.config.debugger_address)
        try:
            print("[SELENIUM] Attached. Configuring download behavior...", flush=True)
            configure_download_directory(driver, self.download_dir)
            print(
                f"[SELENIUM] Watching download directories: "
                f"{', '.join(str(item) for item in self.watch_download_dirs)}",
                flush=True,
            )
            print("[SELENIUM] Scrolling inbox list to load rows...", flush=True)
            self._scroll_until_loaded(driver)
            print("[SELENIUM] Collecting matching rows...", flush=True)
            rows = self._row_locator.collect_row_candidates(driver, verbose=True)
            stats["found"] = len(rows)
            print(f"[SELENIUM] Found {len(rows)} matching rows in current tab.")

            for row in rows:
                if self.config.max_documents is not None and stats["downloaded"] >= self.config.max_documents:
                    break

                if row.signature in state:
                    stats["skipped"] += 1
                    continue

                print(f"[SELENIUM] Processing: {row.doc_type} | {row.row_text[:120]}")

                downloaded = self._download_from_row(driver, row)
                if not downloaded:
                    refreshed = self._row_locator.find_row_by_signature(driver, row.signature)
                    if refreshed is not None:
                        downloaded = self._download_from_row(driver, refreshed)

                if downloaded:
                    state.add(row.signature)
                    stats["downloaded"] += 1
                else:
                    stats["errors"] += 1

            save_download_state(self.state_file, state)
            return stats
        finally:
            # Keep the user browser session open; only stop the chromedriver service.
            try:
                driver.service.stop()  # type: ignore[attr-defined]
            except Exception:
                pass

    def _scroll_until_loaded(self, driver: webdriver.Chrome) -> None:
        last_height = -1
        stable_rounds = 0

        for round_index in range(self.config.scroll_max_rounds):
            current_height = int(driver.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"))
            if current_height == last_height:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_height = current_height

            if round_index == 0 or (round_index + 1) % 5 == 0:
                print(
                    f"[SELENIUM] Scroll round {round_index + 1}/{self.config.scroll_max_rounds}, "
                    f"height={current_height}, stable={stable_rounds}/{self.config.scroll_stable_rounds}",
                    flush=True,
                )

            if stable_rounds >= self.config.scroll_stable_rounds:
                break

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.config.round_wait_seconds)

        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.2)

    def _download_from_row(self, driver: webdriver.Chrome, row: RowCandidate) -> bool:
        existing_pdfs = snapshot_existing_pdfs(self.watch_download_dirs)
        row_element = row.row_element
        self._prepare_network_capture(driver)
        handles_before = set(driver.window_handles)
        current_url_before = driver.current_url

        # Activate/expand the target row first so row-scoped actions point to this specific document.
        self._row_locator.expand_row(driver, row_element)
        download_control = self._row_locator.find_download_control(row_element)
        if download_control is None:
            if not self._row_locator.expand_row(driver, row_element):
                print(f"[SELENIUM][ERROR] Could not open row: {row.row_text[:140]}")
                return False
            download_control = self._row_locator.find_download_control(row_element)

        if download_control is None:
            print(f"[SELENIUM][ERROR] No download button found in row: {row.row_text[:140]}")
            return False

        direct_url = self._extract_download_url(driver, download_control)
        if direct_url:
            print(f"[SELENIUM] Trying direct URL download: {direct_url[:160]}", flush=True)
            downloaded_file = self._support.download_via_session_url(
                driver,
                direct_url,
                download_dir=self.download_dir,
                doc_type=row.doc_type,
            )
            if downloaded_file:
                print(f"[SELENIUM][OK] Downloaded {downloaded_file.name} ({row.doc_type})")
                return True

        if not safe_click(driver, download_control):
            print(f"[SELENIUM][ERROR] Could not click download button in row: {row.row_text[:140]}")
            return False

        new_file = wait_for_download(
            self.watch_download_dirs,
            existing_pdfs,
            wait_seconds=self.config.click_download_wait_seconds,
        )
        if new_file is not None:
            print(f"[SELENIUM][OK] Downloaded {new_file.name} ({row.doc_type})")
            return True

        candidate_urls = []
        if driver.current_url != current_url_before:
            candidate_urls.append(driver.current_url)
        tab_url = self._extract_new_tab_url(driver, handles_before)
        if tab_url:
            candidate_urls.append(tab_url)
        candidate_urls.extend(
            urljoin(driver.current_url, item)
            for item in self._consume_captured_urls(driver)
            if self._row_locator.is_download_like_url(item)
        )

        seen = set()
        for url in candidate_urls:
            if url in seen:
                continue
            seen.add(url)
            print(f"[SELENIUM] Trying captured URL download: {url[:160]}", flush=True)
            downloaded_file = self._support.download_via_session_url(
                driver,
                url,
                download_dir=self.download_dir,
                doc_type=row.doc_type,
            )
            if downloaded_file:
                print(f"[SELENIUM][OK] Downloaded {downloaded_file.name} ({row.doc_type})")
                return True

        print(f"[SELENIUM][ERROR] Download did not complete in time: {row.row_text[:140]}")
        return False

    def _prepare_network_capture(self, driver: webdriver.Chrome) -> None:
        script = """
            if (!window.__seleniumCapturedUrls) {
                window.__seleniumCapturedUrls = [];
            }
            if (!window.__seleniumCaptureInstalled) {
                window.__seleniumCaptureInstalled = true;
                const pushUrl = (url) => {
                    try {
                        if (!url) return;
                        window.__seleniumCapturedUrls.push(String(url));
                        if (window.__seleniumCapturedUrls.length > 300) {
                            window.__seleniumCapturedUrls = window.__seleniumCapturedUrls.slice(-300);
                        }
                    } catch (e) {}
                };
                const originalFetch = window.fetch;
                window.fetch = function(...args) {
                    try {
                        const input = args[0];
                        if (typeof input === "string") pushUrl(input);
                        else if (input && input.url) pushUrl(input.url);
                    } catch (e) {}
                    return originalFetch.apply(this, args);
                };
                const originalOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function(method, url, ...rest) {
                    pushUrl(url);
                    return originalOpen.call(this, method, url, ...rest);
                };
            }
        """
        try:
            driver.execute_script(script)
        except Exception:
            pass

    def _consume_captured_urls(self, driver: webdriver.Chrome) -> list[str]:
        try:
            urls = driver.execute_script(
                "const v = window.__seleniumCapturedUrls || []; "
                "window.__seleniumCapturedUrls = []; return v;"
            )
        except Exception:
            return []

        if not isinstance(urls, list):
            return []
        return [str(item) for item in urls if isinstance(item, str)]

    def _extract_download_url(self, driver: webdriver.Chrome, control) -> str | None:
        try:
            href = control.get_attribute("href")
        except Exception:
            return None

        if not href:
            return None
        href = href.strip()
        if not href or href.startswith("javascript:") or href.startswith("#"):
            return None
        return urljoin(driver.current_url, href)

    def _extract_new_tab_url(self, driver: webdriver.Chrome, handles_before: set[str]) -> str | None:
        try:
            handles_after = set(driver.window_handles)
            original_handle = driver.current_window_handle
        except Exception:
            return None

        new_handles = [item for item in handles_after if item not in handles_before]
        if not new_handles:
            return None

        found_url = None
        for handle in new_handles:
            try:
                driver.switch_to.window(handle)
                current = driver.current_url
            except Exception:
                continue
            if self._row_locator.is_download_like_url(current):
                found_url = current
                break

        try:
            driver.switch_to.window(original_handle)
        except Exception:
            pass
        return found_url


def download_ing_documents_from_current_tab(config: SeleniumDownloadConfig) -> dict[str, int]:
    return IngSeleniumInboxDownloader(config, support=SeleniumDownloadSupport()).run()
