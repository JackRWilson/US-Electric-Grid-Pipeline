import shutil
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

DATA_PATH = Path("/data")


def download(url, path, retries=5):
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                with path.open("wb") as output:
                    shutil.copyfileobj(response, output)
            if path.stat().st_size == 0:
                raise ValueError(f"Downloaded an empty file from {url}")
            return
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            path.unlink(missing_ok=True)
            print(f"  attempt {attempt}/{retries} failed: {error}", flush=True)
            if attempt == retries:
                raise
            time.sleep(3)


def fetch(url, relative_path):
    out_path = DATA_PATH / relative_path
    if out_path.is_file() and out_path.stat().st_size > 0:
        print(f"skip   {relative_path}", flush=True)
        return

    print(f"fetch  {relative_path}", flush=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = out_path.with_name(out_path.name + ".part")
    try:
        download(url, part_path)
        part_path.replace(out_path)
    finally:
        part_path.unlink(missing_ok=True)


def fetch_zip_member(url, relative_path):
    out_path = DATA_PATH / relative_path
    if out_path.is_file() and out_path.stat().st_size > 0:
        print(f"skip   {relative_path}", flush=True)
        return

    print(f"fetch  {relative_path} (zip member)", flush=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path = out_path.with_name(out_path.name + ".zip.part")
    part_path = out_path.with_name(out_path.name + ".part")
    try:
        download(url, archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            matches = [
                member
                for member in archive.infolist()
                if Path(member.filename).name == out_path.name and not member.is_dir()
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"Expected one {out_path.name} member in {url}; found {len(matches)}"
                )
            member = matches[0]
            if member.file_size == 0:
                raise ValueError(f"Archive member {out_path.name} is empty")
            with archive.open(member) as source, part_path.open("wb") as output:
                shutil.copyfileobj(source, output)
        part_path.replace(out_path)
    finally:
        archive_path.unlink(missing_ok=True)
        part_path.unlink(missing_ok=True)


def main():
    fetch_zip_member(
        "https://www.eia.gov/electricity/data/eia860/xls/eia8602025.zip",
        "2___Plant_Y2025.xlsx",
    )
    fetch_zip_member(
        "https://apps.bea.gov/regional/zip/CAINC1.zip",
        "CAINC1__ALL_AREAS_1969_2024.csv",
    )
    fetch(
        "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv",
        "sub-est2025.csv",
    )
    fetch_zip_member(
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip",
        "2020_Gaz_counties_national.txt",
    )
    fetch_zip_member(
        "https://www.eia.gov/electricity/data/eia861/zip/f8612025.zip",
        "Balancing_Authority_2025.xlsx",
    )
    fetch(
        "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/out_eia861__yearly_balancing_authority_service_territory.parquet",
        "out_eia861__yearly_balancing_authority_service_territory.parquet",
    )
    print("static data ready", flush=True)


if __name__ == "__main__":
    main()
