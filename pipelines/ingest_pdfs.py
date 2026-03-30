"""Scrape and download budget PDF documents from Uruguay's OPP website."""

import os
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from google.cloud import storage

BASE_URL = "https://www.opp.gub.uy/es/presupuesto-nacional"
BUCKET_NAME = os.environ.get("GCS_BUCKET", "opp-data-lake")
GCS_PREFIX = "raw/pdfs"
LOCAL_DIR = Path("data/raw/pdfs")
DELAY_SECONDS = 2.5  # Respectful scraping delay


def fetch_pdf_links(client: httpx.Client, page_url: str) -> list[str]:
    """Parse the OPP budget page and extract PDF download links."""
    resp = client.get(page_url, follow_redirects=True, timeout=30.0)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    pdf_links: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if href.endswith(".pdf") and "/sites/default/files/" in href:
            full_url = urljoin(page_url, href)
            if full_url not in pdf_links:
                pdf_links.append(full_url)

    return pdf_links


def download_pdf(client: httpx.Client, url: str, local_dir: Path) -> Path | None:
    """Download a single PDF file to the local directory."""
    filename = url.split("/")[-1]
    local_path = local_dir / filename

    if local_path.exists():
        print(f"  CACHED {filename}")
        return local_path

    try:
        resp = client.get(url, follow_redirects=True, timeout=120.0)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"  SKIP {url}: {exc}")
        return None

    local_path.write_bytes(resp.content)
    size_mb = len(resp.content) / (1024 * 1024)
    print(f"  OK {filename} ({size_mb:.1f} MB)")
    return local_path


def upload_to_gcs(local_path: Path, bucket_name: str, prefix: str) -> None:
    """Upload a local file to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{prefix}/{local_path.name}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    print(f"  GCS uploaded: gs://{bucket_name}/{blob_name}")


def main() -> None:
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(
        headers={"User-Agent": "OPP-Budget-Research/1.0 (academic)"}
    ) as client:
        print(f"Fetching PDF links from {BASE_URL}...")
        pdf_links = fetch_pdf_links(client, BASE_URL)
        print(f"Found {len(pdf_links)} PDF links")

        downloaded: list[Path] = []
        for i, url in enumerate(pdf_links):
            print(f"[{i + 1}/{len(pdf_links)}] Downloading: {url.split('/')[-1]}")
            path = download_pdf(client, url, LOCAL_DIR)
            if path:
                downloaded.append(path)

            if i < len(pdf_links) - 1:
                time.sleep(DELAY_SECONDS)

    print(f"\nDownloaded {len(downloaded)} PDFs")

    if os.environ.get("UPLOAD_GCS", "false").lower() == "true":
        print("Uploading to GCS...")
        for path in downloaded:
            upload_to_gcs(path, BUCKET_NAME, GCS_PREFIX)
        print("GCS upload complete")


if __name__ == "__main__":
    main()
