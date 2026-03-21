from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests


class SeleniumDownloadSupport:
    def build_watch_download_dirs(self, download_dir: Path, fallback_download_dirs: list[Path]) -> list[Path]:
        watch_dirs = [download_dir]
        for item in fallback_download_dirs:
            path = Path(item)
            if path not in watch_dirs and path.exists():
                watch_dirs.append(path)
        return watch_dirs

    def snapshot_pdf_states(self, watch_dirs: list[Path]) -> dict[str, tuple[int, int]]:
        states: dict[str, tuple[int, int]] = {}
        for directory in watch_dirs:
            for file_path in directory.glob("*.pdf"):
                try:
                    stat = file_path.stat()
                except OSError:
                    continue
                states[str(file_path.resolve())] = (int(stat.st_mtime_ns), int(stat.st_size))
        return states

    def find_changed_pdf(self, watch_dirs: list[Path], previous: dict[str, tuple[int, int]]) -> Path | None:
        newest: list[tuple[int, Path]] = []
        for directory in watch_dirs:
            for file_path in directory.glob("*.pdf"):
                try:
                    stat = file_path.stat()
                except OSError:
                    continue
                if stat.st_size <= 0:
                    continue
                temp_file = file_path.with_suffix(file_path.suffix + ".crdownload")
                if temp_file.exists():
                    continue
                newest.append((int(stat.st_mtime_ns), file_path))

        newest.sort(key=lambda item: item[0], reverse=True)
        for mtime_ns, file_path in newest:
            key = str(file_path.resolve())
            prev = previous.get(key)
            try:
                size = int(file_path.stat().st_size)
            except OSError:
                continue
            if prev is None:
                return file_path
            if mtime_ns > prev[0] or size != prev[1]:
                return file_path
        return None

    def wait_for_download_change(
        self,
        watch_dirs: list[Path],
        previous: dict[str, tuple[int, int]],
        *,
        wait_seconds: float,
    ) -> Path | None:
        end_time = time.time() + wait_seconds
        while time.time() < end_time:
            candidate = self.find_changed_pdf(watch_dirs, previous)
            if candidate is not None:
                return candidate
            time.sleep(0.25)
        return None

    def download_via_session_url(self, driver, url: str, *, download_dir: Path, doc_type: str) -> Path | None:
        if not url:
            return None
        lower = url.lower()
        if lower.startswith("blob:") or lower.startswith("data:"):
            return None

        session = requests.Session()
        try:
            user_agent = driver.execute_script("return navigator.userAgent")
        except Exception:
            user_agent = "Mozilla/5.0"
        session.headers.update({"User-Agent": str(user_agent), "Referer": driver.current_url})

        try:
            for cookie in driver.get_cookies():
                if "name" in cookie and "value" in cookie:
                    session.cookies.set(
                        cookie["name"],
                        cookie["value"],
                        domain=cookie.get("domain"),
                        path=cookie.get("path", "/"),
                    )
            response = session.get(url, timeout=45, allow_redirects=True)
        except Exception:
            return None

        if response.status_code >= 400:
            return None

        content_type = (response.headers.get("content-type") or "").lower()
        payload = response.content
        if "pdf" not in content_type and b"%PDF" not in payload[:1024]:
            return None

        filename = self._filename_from_response(response, doc_type=doc_type)
        target = self._unique_download_path(download_dir=download_dir, filename=filename)
        target.write_bytes(payload)
        return target

    @staticmethod
    def is_download_like_url(url: str | None) -> bool:
        if not url:
            return False
        lower = url.lower()
        if lower.startswith("blob:") or lower.startswith("data:"):
            return False
        return (".pdf" in lower) or ("download" in lower) or ("dokument" in lower)

    @staticmethod
    def _filename_from_response(response: requests.Response, *, doc_type: str) -> str:
        content_disposition = response.headers.get("content-disposition") or ""
        match = re.search(r"filename\\*=UTF-8''([^;]+)", content_disposition, flags=re.IGNORECASE)
        if match:
            name = match.group(1)
        else:
            match = re.search(r'filename=\"?([^\";]+)\"?', content_disposition, flags=re.IGNORECASE)
            if match:
                name = match.group(1)
            else:
                url_name = Path(urlparse(response.url).path).name
                name = url_name or f"{doc_type}_{int(time.time())}.pdf"

        name = name.strip().replace("/", "_").replace("\\", "_")
        if not name.lower().endswith(".pdf"):
            name = f"{name}.pdf"
        return name

    @staticmethod
    def _unique_download_path(*, download_dir: Path, filename: str) -> Path:
        target = download_dir / filename
        if not target.exists():
            return target
        base = target.stem
        suffix = target.suffix
        for idx in range(1, 1000):
            candidate = download_dir / f"{base}_{idx}{suffix}"
            if not candidate.exists():
                return candidate
        return download_dir / f"{base}_{int(time.time())}{suffix}"

