import argparse
import hashlib
import logging
import re
import sys
from collections import deque
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

DEFAULT_TIMEOUT = 20
DEFAULT_USER_AGENT = "phpbb_downloader/2.0 (+offline-archiver)"


def setup_logger(log_file: Path | None, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("phpbb_downloader")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.addHandler(console)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    return logger


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w.\-]+", "_", name, flags=re.ASCII)
    return name.strip("._") or "item"


def read_lines(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(f"{line}\n")


class PhpbbDownloader:
    def __init__(
        self,
        start_url: str,
        output_dir: Path,
        db_file: Path,
        broken_file: Path,
        logger: logging.Logger,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.start_url = start_url.rstrip("/")
        self.output_dir = output_dir
        self.db_file = db_file
        self.broken_file = broken_file
        self.logger = logger
        self.timeout = timeout

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

        parsed = urlparse(self.start_url)
        if not parsed.scheme:
            raise ValueError(
                "start_url must include scheme, e.g. https://example.com/forum"
            )
        self.base_netloc = parsed.netloc
        self.base_scheme = parsed.scheme

        self.visited_pages = read_lines(self.db_file)
        self.downloaded_assets = set(self.visited_pages)
        self.broken_links = read_lines(self.broken_file)

    def run(self) -> None:
        queue: deque[str] = deque([self.start_url])
        if self.start_url in self.visited_pages:
            self.logger.info(
                "Start URL is already present in database; skipping page fetch: %s",
                self.start_url,
            )
            return

        while queue:
            page_url = queue.popleft()
            if page_url in self.visited_pages:
                continue

            html = self.fetch_text(page_url)
            if html is None:
                self.mark_broken(page_url)
                continue

            new_links = self.process_page(page_url, html)
            self.visited_pages.add(page_url)
            append_line(self.db_file, page_url)

            for link in new_links:
                if link not in self.visited_pages:
                    queue.append(link)

        self.logger.info(
            "Done. Downloaded pages: %d, assets: %d",
            len(self.visited_pages),
            len(self.downloaded_assets),
        )

    def fetch_text(self, url: str) -> str | None:
        self.logger.info("Downloading page: %s", url)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            self.logger.error("Cannot open page %s: %s", url, exc)
            return None

    def fetch_binary(self, url: str) -> bytes | None:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            self.logger.error("Cannot download asset %s: %s", url, exc)
            self.mark_broken(url)
            return None

    def process_page(self, page_url: str, html: str) -> set[str]:
        soup = BeautifulSoup(html, "html.parser")
        next_links: set[str] = set()

        self.rewrite_stylesheets(soup, page_url)
        self.rewrite_scripts(soup, page_url)
        self.rewrite_images(soup, page_url)
        self.rewrite_anchors(soup, page_url, next_links)

        page_path = self.local_page_path(page_url)
        page_path.parent.mkdir(parents=True, exist_ok=True)
        page_path.write_text(str(soup), encoding="utf-8")
        return next_links

    def rewrite_stylesheets(self, soup: BeautifulSoup, page_url: str) -> None:
        for tag in soup.find_all("link", href=True):
            abs_url = self.normalize_url(page_url, tag["href"])
            parsed = urlparse(abs_url)

            if parsed.netloc != self.base_netloc:
                tag["href"] = "#"
                continue

            if parsed.path.endswith(".css") or parsed.path.endswith("style.php"):
                local = self.download_asset(abs_url, kind="css")
                tag["href"] = self.rel_from_root(local)
            else:
                tag["href"] = "#"

    def rewrite_scripts(self, soup: BeautifulSoup, page_url: str) -> None:
        for tag in soup.find_all("script", src=True):
            abs_url = self.normalize_url(page_url, tag["src"])
            parsed = urlparse(abs_url)
            if parsed.netloc == self.base_netloc and parsed.path.endswith(".js"):
                local = self.download_asset(abs_url, kind="js")
                tag["src"] = self.rel_from_root(local)
            else:
                tag["src"] = "#"

    def rewrite_images(self, soup: BeautifulSoup, page_url: str) -> None:
        for tag in soup.find_all("img", src=True):
            abs_url = self.normalize_url(page_url, tag["src"])
            parsed = urlparse(abs_url)
            if parsed.netloc != self.base_netloc:
                continue
            local = self.download_asset(abs_url, kind="img")
            tag["src"] = self.rel_from_root(local)

    def rewrite_anchors(
        self, soup: BeautifulSoup, page_url: str, next_links: set[str]
    ) -> None:
        for tag in soup.find_all("a", href=True):
            abs_url = self.normalize_url(page_url, tag["href"])
            parsed = urlparse(abs_url)

            if parsed.netloc and parsed.netloc != self.base_netloc:
                continue

            if self.is_page_link(parsed):
                normalized = self.normalize_page_link(abs_url)
                local = self.local_page_path(normalized)
                tag["href"] = self.rel_from_root(local)
                next_links.add(normalized)
                continue

            if self.is_attachment_link(parsed):
                local = self.download_asset(abs_url, kind="files")
                tag["href"] = self.rel_from_root(local)
                continue

            if parsed.path.endswith("index.php"):
                root_url = f"{self.base_scheme}://{self.base_netloc}/index.php"
                tag["href"] = self.rel_from_root(self.local_page_path(root_url))
                continue

            if parsed.netloc == self.base_netloc:
                tag["href"] = "#"

    def normalize_url(self, source_url: str, raw_link: str) -> str:
        raw_link = raw_link.strip()
        absolute = urljoin(source_url, raw_link)
        parsed = urlparse(absolute)
        clean = parsed._replace(fragment="")
        if not clean.scheme:
            clean = clean._replace(scheme=self.base_scheme)
        return urlunparse(clean)

    def normalize_page_link(self, url: str) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query, keep_blank_values=True)

        allowed: dict[str, list[str]] = {}
        for key in ("f", "t", "start"):
            if key in query:
                allowed[key] = query[key]

        path = parsed.path or "/index.php"
        normalized_query = urlencode(allowed, doseq=True)
        return urlunparse(
            (parsed.scheme, parsed.netloc, path, "", normalized_query, "")
        )

    def local_page_path(self, page_url: str) -> Path:
        parsed = urlparse(page_url)
        path = parsed.path.strip("/")
        if not path:
            base = "index"
        else:
            base = sanitize_filename(path.replace("/", "_"))
        if not base.endswith(".php") and not base.endswith(".html"):
            base += ".html"

        if parsed.query:
            query_hash = hashlib.md5(parsed.query.encode("utf-8")).hexdigest()[:8]
            base = f"{base}_{query_hash}.html"
        elif not base.endswith(".html"):
            base += ".html"
        return self.output_dir / "pages" / base

    def asset_filename(self, url: str, kind: str) -> str:
        parsed = urlparse(url)
        name = Path(parsed.path).name
        if not name:
            name = kind

        name = sanitize_filename(name)
        if parsed.query:
            query_hash = hashlib.md5(parsed.query.encode("utf-8")).hexdigest()[:8]
            stem = Path(name).stem
            suffix = Path(name).suffix
            name = f"{stem}_{query_hash}{suffix}"
        return name

    def download_asset(self, url: str, kind: str) -> Path:
        subdir = self.output_dir / kind
        subdir.mkdir(parents=True, exist_ok=True)
        target = subdir / self.asset_filename(url, kind)

        if url in self.downloaded_assets and target.exists():
            return target

        payload = self.fetch_binary(url)
        if payload is None:
            return target

        target.write_bytes(payload)
        self.downloaded_assets.add(url)
        append_line(self.db_file, url)
        return target

    def rel_from_root(self, path: Path) -> str:
        return f"./{path.relative_to(self.output_dir).as_posix()}"

    def is_page_link(self, parsed) -> bool:
        return parsed.path.endswith("viewforum.php") or parsed.path.endswith(
            "viewtopic.php"
        )

    def is_attachment_link(self, parsed) -> bool:
        return parsed.path.endswith("file.php") or parsed.path.endswith("dl_file.php")

    def mark_broken(self, url: str) -> None:
        if url in self.broken_links:
            return
        self.broken_links.add(url)
        append_line(self.broken_file, url)


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download phpBB forum pages and assets for offline browsing."
    )
    parser.add_argument("url", help="Start URL, e.g. https://forum.example.com")
    parser.add_argument(
        "--db-file", default="database.txt", help="File for downloaded/visited URLs."
    )
    parser.add_argument("--log-file", default="log.txt", help="Log file path.")
    parser.add_argument(
        "--broken-file", default="broken_links.txt", help="File for failed URLs."
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for downloaded forum mirror (default: forum netloc).",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds."
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    parsed = urlparse(args.url)
    if not parsed.netloc:
        print(
            "Error: URL must include scheme and netloc, e.g. https://forum.example.com",
            file=sys.stderr,
        )
        return 2

    output_dir = Path(args.output_dir) if args.output_dir else Path(parsed.netloc)
    log_file = Path(args.log_file)
    db_file = Path(args.db_file)
    broken_file = Path(args.broken_file)

    logger = setup_logger(log_file=log_file, verbose=args.verbose)

    downloader = PhpbbDownloader(
        start_url=args.url,
        output_dir=output_dir,
        db_file=db_file,
        broken_file=broken_file,
        logger=logger,
        timeout=args.timeout,
    )

    downloader.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
