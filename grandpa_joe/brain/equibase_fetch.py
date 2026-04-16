"""
Equibase data fetcher for GRANDPA_JOE.
Downloads chart data from Equibase using their API or direct download URLs.

Supports:
- Free research dataset download
- Chart downloads (CSV/XML) via Equibase account
- Result chart lookups by track/date

IMPORTANT: Equibase ToS prohibits scraping. This module uses only their
official download products and API endpoints. Requires EQUIBASE_API_KEY
for paid products.
"""

import logging
import os
import shutil
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

from grandpa_joe.path_manager import PathManager

logger = logging.getLogger(__name__)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


# Equibase track codes for US thoroughbred tracks
US_TRACKS = {
    "AQU": "Aqueduct",
    "BEL": "Belmont Park",
    "CD": "Churchill Downs",
    "DMR": "Del Mar",
    "GP": "Gulfstream Park",
    "KEE": "Keeneland",
    "LRL": "Laurel Park",
    "MTH": "Monmouth Park",
    "OP": "Oaklawn Park",
    "PIM": "Pimlico",
    "SA": "Santa Anita",
    "SAR": "Saratoga",
    "TAM": "Tampa Bay Downs",
    "WO": "Woodbine",
    "PRM": "Prairie Meadows",
    "FG": "Fair Grounds",
    "CT": "Charles Town",
    "PRX": "Parx Racing",
    "FL": "Finger Lakes",
    "TDN": "Thistledown",
    "EVD": "Evangeline Downs",
    "IND": "Indiana Grand",
    "LS": "Lone Star Park",
    "PEN": "Penn National",
    "RP": "Remington Park",
    "RET": "Retama Park",
    "SUF": "Suffolk Downs",
    "TUP": "Turf Paradise",
    "GG": "Golden Gate Fields",
    "EMD": "Emerald Downs",
    "AP": "Arlington Park",
    "CBY": "Canterbury Park",
    "ELP": "Ellis Park",
    "FP": "Fairmount Park",
    "HAW": "Hawthorne",
    "HOU": "Sam Houston",
    "LAD": "Louisiana Downs",
    "MNR": "Mountaineer",
    "SUN": "Sunland Park",
    "ZIA": "Zia Park",
}


class EquibaseFetcher:
    """
    Downloads and manages Equibase data files.

    Usage:
        fetcher = EquibaseFetcher(api_key="your-key")

        # Download free research dataset
        path = fetcher.download_research_dataset()

        # Download chart for a specific track/date
        path = fetcher.download_chart("SAR", "2024-08-01")

        # List available local files
        files = fetcher.list_local_files()
    """

    RESEARCH_DATASET_URL = "https://www.equibase.com/handicappersdata.cfm"

    def __init__(self, api_key: str = "", download_dir: Optional[str] = None):
        self.api_key = api_key or os.getenv("EQUIBASE_API_KEY", "")
        self.download_dir = Path(download_dir) if download_dir else PathManager.INGESTION_DIR
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def download_research_dataset(self, email: str = "") -> Optional[str]:
        """
        Download the free Equibase 2023 research dataset.

        The research dataset requires registration at equibase.com.
        This method provides instructions since the download requires
        browser-based form submission.

        Returns:
            Path where the dataset should be placed, or None
        """
        dest_dir = self.download_dir / "equibase_research"
        dest_dir.mkdir(parents=True, exist_ok=True)

        instructions = f"""
=== EQUIBASE FREE RESEARCH DATASET ===

Equibase offers a FREE dataset with a full year of 2023 past performance
data and corresponding results charts.

To download:
1. Go to https://www.equibase.com/handicappersdata.cfm
2. Look for "Free Dataset" or "Research Dataset" link
3. Register with your email address
4. Download the dataset files
5. Place them in: {dest_dir}

Supported formats:
  - CSV files -> place in {dest_dir}/csv/
  - XML files -> place in {dest_dir}/xml/

Then run:
  python -m grandpa_joe ingest-dir {dest_dir}

This will process all CSV and XML files into Grandpa Joe's brain.
================================================================
"""
        logger.info(instructions)
        print(instructions)

        # Create a README in the directory
        readme_path = dest_dir / "README.txt"
        readme_path.write_text(instructions)

        return str(dest_dir)

    def download_chart(self, track_code: str, race_date: str,
                       fmt: str = "csv") -> Optional[str]:
        """
        Download a chart file from Equibase.

        Requires EQUIBASE_API_KEY.

        Args:
            track_code: Track abbreviation (e.g., "SAR")
            race_date: Date string (e.g., "2024-08-01")
            fmt: Format - "csv" or "xml"

        Returns:
            Path to downloaded file, or None if failed
        """
        if not REQUESTS_AVAILABLE:
            logger.error("requests library not installed")
            return None

        if not self.api_key:
            logger.error(
                "EQUIBASE_API_KEY required for chart downloads. "
                "Set it in .env or environment."
            )
            return None

        track_code = track_code.upper()
        filename = f"{track_code}_{race_date}.{fmt}"
        dest_path = self.download_dir / filename

        if dest_path.exists():
            logger.info(f"Chart already downloaded: {dest_path}")
            return str(dest_path)

        # Equibase chart download endpoint
        # Note: The exact URL format depends on your Equibase subscription
        url = (
            f"https://www.equibase.com/premium/chartdownload.cfm"
            f"?track={track_code}&date={race_date}&format={fmt}"
        )

        try:
            resp = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": "GrandpaJoe/0.1.0",
                },
                timeout=30,
            )

            if resp.status_code == 200:
                dest_path.write_bytes(resp.content)
                logger.info(f"Downloaded chart: {dest_path}")
                return str(dest_path)
            elif resp.status_code == 401:
                logger.error("Invalid EQUIBASE_API_KEY — check your key")
            elif resp.status_code == 402:
                logger.error(
                    f"Chart requires purchase: {track_code} {race_date} "
                    f"($1.50/chart at equibase.com)"
                )
            elif resp.status_code == 404:
                logger.warning(f"Chart not found: {track_code} {race_date}")
            else:
                logger.warning(f"Download failed: HTTP {resp.status_code}")
            return None

        except requests.RequestException as e:
            logger.error(f"Download error: {e}")
            return None

    def download_charts_batch(self, track_code: str, dates: List[str],
                              fmt: str = "csv") -> Dict:
        """
        Download multiple charts for a track.

        Args:
            track_code: Track abbreviation
            dates: List of date strings
            fmt: "csv" or "xml"

        Returns:
            Dict with download results
        """
        results = {"downloaded": [], "failed": [], "skipped": []}

        for date in dates:
            path = self.download_chart(track_code, date, fmt)
            if path:
                results["downloaded"].append(path)
            else:
                results["failed"].append(f"{track_code}_{date}")

        return results

    def list_local_files(self, pattern: str = "*") -> List[Dict]:
        """
        List downloaded data files.

        Returns:
            List of dicts with file info
        """
        files = []
        for ext in ["*.csv", "*.xml", "*.zip"]:
            for f in sorted(self.download_dir.rglob(ext)):
                files.append({
                    "path": str(f),
                    "name": f.name,
                    "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                    "format": f.suffix[1:],
                })
        return files

    def extract_zip(self, zip_path: str) -> List[str]:
        """
        Extract a downloaded ZIP file (common for bulk downloads).

        Returns:
            List of extracted file paths
        """
        zip_path = Path(zip_path)
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP not found: {zip_path}")

        extract_dir = zip_path.parent / zip_path.stem
        extract_dir.mkdir(exist_ok=True)

        extracted = []
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                # Only extract CSV/XML files
                if info.filename.lower().endswith((".csv", ".xml")):
                    zf.extract(info, str(extract_dir))
                    extracted.append(str(extract_dir / info.filename))
                    logger.info(f"Extracted: {info.filename}")

        return extracted

    def get_status(self) -> Dict:
        """Get fetcher status."""
        local_files = self.list_local_files()
        return {
            "api_key_set": bool(self.api_key),
            "download_dir": str(self.download_dir),
            "local_files": len(local_files),
            "total_size_mb": sum(f["size_mb"] for f in local_files),
            "csv_files": sum(1 for f in local_files if f["format"] == "csv"),
            "xml_files": sum(1 for f in local_files if f["format"] == "xml"),
        }


def _route_xml(brain, filepath: str) -> Dict:
    """Route an XML file to the correct parser (SIMD vs chart)."""
    from grandpa_joe.brain.equibase_simd import is_simd_file, ingest_simd
    from grandpa_joe.brain.equibase_xml import ingest_xml

    if is_simd_file(filepath):
        return ingest_simd(brain, filepath)
    return ingest_xml(brain, filepath)


def ingest_directory(brain, directory: str) -> Dict:
    """
    Ingest all CSV and XML files from a directory into the racing brain.

    Automatically routes SIMD XML files to the SIMD parser and other XML
    files to the chart parser.

    Args:
        brain: RacingBrain instance
        directory: Path to directory with data files

    Returns:
        Aggregate counts
    """
    from grandpa_joe.brain.ingestion import ingest_csv

    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    totals = {"files_processed": 0, "files_failed": 0}

    def _merge_counts(counts):
        for k, v in counts.items():
            if k == "files_processed":
                continue
            totals[k] = totals.get(k, 0) + v

    # Process CSVs
    for csv_file in sorted(dir_path.rglob("*.csv")):
        logger.info(f"Ingesting CSV: {csv_file.name}")
        try:
            _merge_counts(ingest_csv(brain, str(csv_file)))
            totals["files_processed"] += 1
        except Exception as e:
            logger.error(f"Failed: {csv_file.name}: {e}")
            totals["files_failed"] += 1

    # Process XMLs (auto-routes SIMD vs chart)
    for xml_file in sorted(dir_path.rglob("*.xml")):
        logger.info(f"Ingesting XML: {xml_file.name}")
        try:
            _merge_counts(_route_xml(brain, str(xml_file)))
            totals["files_processed"] += 1
        except Exception as e:
            logger.error(f"Failed: {xml_file.name}: {e}")
            totals["files_failed"] += 1

    # Process ZIPs (extract then ingest)
    for zip_file in sorted(dir_path.rglob("*.zip")):
        logger.info(f"Extracting ZIP: {zip_file.name}")
        try:
            fetcher = EquibaseFetcher()
            extracted = fetcher.extract_zip(str(zip_file))
            for f in extracted:
                if f.endswith(".csv"):
                    _merge_counts(ingest_csv(brain, f))
                elif f.endswith(".xml"):
                    _merge_counts(_route_xml(brain, f))
                else:
                    continue
                totals["files_processed"] += 1
        except Exception as e:
            logger.error(f"Failed: {zip_file.name}: {e}")
            totals["files_failed"] += 1

    return totals


def compute_days_since_previous(brain) -> int:
    """
    Backfill days_since_prev_race for all past performances that are missing it.

    Computes the gap between consecutive race dates per horse.

    Returns:
        Number of records updated
    """
    conn = brain._connect()
    try:
        # Get all PPs missing days_since_prev_race, grouped by horse
        rows = conn.execute(
            "SELECT id, horse_id, race_date "
            "FROM past_performances "
            "ORDER BY horse_id, race_date"
        ).fetchall()

        updates = 0
        prev_horse_id = None
        prev_date = None

        for row in rows:
            horse_id = row["horse_id"]
            race_date = row["race_date"]

            if horse_id == prev_horse_id and prev_date and race_date:
                try:
                    from datetime import datetime
                    d1 = datetime.strptime(prev_date[:10], "%Y-%m-%d")
                    d2 = datetime.strptime(race_date[:10], "%Y-%m-%d")
                    days = (d2 - d1).days
                    if days >= 0:
                        conn.execute(
                            "UPDATE past_performances SET days_since_prev_race = ? "
                            "WHERE id = ?",
                            (days, row["id"])
                        )
                        updates += 1
                except (ValueError, TypeError):
                    pass

            prev_horse_id = horse_id
            prev_date = race_date

        conn.commit()
        logger.info(f"Backfilled days_since_prev_race for {updates} records")
        return updates
    finally:
        conn.close()
