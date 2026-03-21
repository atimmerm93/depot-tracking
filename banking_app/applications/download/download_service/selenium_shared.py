from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException, WebDriverException


@dataclass
class SeleniumDownloadConfig:
    debugger_address: str = "127.0.0.1:9222"
    download_dir: Path = Path("incoming_pdfs")
    state_file: Path | None = None
    reset_state: bool = False
    fallback_download_dirs: list[Path] = field(default_factory=lambda: [Path.home() / "Downloads"])
    max_documents: int | None = None
    scroll_stable_rounds: int = 3
    scroll_max_rounds: int = 35
    round_wait_seconds: float = 1.0
    download_wait_seconds: float = 35.0
    click_download_wait_seconds: float = 8.0


def attach_to_current_tab(debugger_address: str) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.debugger_address = debugger_address
    try:
        return webdriver.Chrome(options=options)
    except WebDriverException as exc:
        raise RuntimeError(
            "Could not attach to current browser tab. Start Chrome with remote debugging first, "
            "for example: 'Google Chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug'."
        ) from exc


def configure_download_directory(driver: webdriver.Chrome, download_dir: Path) -> None:
    params = {"behavior": "allow", "downloadPath": str(download_dir.resolve()), "eventsEnabled": False}
    for command in ("Browser.setDownloadBehavior", "Page.setDownloadBehavior"):
        try:
            driver.execute_cdp_cmd(command, params)
            return
        except Exception:
            continue


def safe_click(driver: webdriver.Chrome, element) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    except Exception:
        pass

    try:
        element.click()
        return True
    except (ElementClickInterceptedException, WebDriverException, StaleElementReferenceException):
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def load_download_state(state_file: Path) -> set[str]:
    if not state_file.exists():
        return set()
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return set()
    signatures = payload.get("signatures")
    if not isinstance(signatures, list):
        return set()
    return {str(item) for item in signatures}


def save_download_state(state_file: Path, signatures: set[str]) -> None:
    state_file.write_text(json.dumps({"signatures": sorted(signatures)}, indent=2), encoding="utf-8")


def list_pdf_files(watch_dirs: list[Path]) -> list[Path]:
    result: list[Path] = []
    for directory in watch_dirs:
        result.extend(directory.glob("*.pdf"))
    return result


def snapshot_existing_pdfs(watch_dirs: list[Path]) -> set[str]:
    return {str(item.resolve()) for item in list_pdf_files(watch_dirs)}


def wait_for_download(
    watch_dirs: list[Path],
    existing_pdfs: set[str],
    *,
    wait_seconds: float,
) -> Path | None:
    end_time = time.time() + wait_seconds
    while time.time() < end_time:
        current_files = sorted(list_pdf_files(watch_dirs), key=lambda item: item.stat().st_mtime, reverse=True)
        for file_path in current_files:
            file_key = str(file_path.resolve())
            if file_key in existing_pdfs:
                continue
            temp_file = file_path.with_suffix(file_path.suffix + ".crdownload")
            if temp_file.exists():
                continue
            if file_path.stat().st_size == 0:
                continue
            return file_path
        time.sleep(0.5)
    return None
