import logging
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _build_session():
    retry = Retry(
        total=4,
        connect=3,
        read=1,
        status=3,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


SESSION = _build_session()


class ExtractAPI:
    def _request(self, url, params, timeout=(5, 30)):
        response = SESSION.get(url, params=params, timeout=timeout)
        if not response.ok:
            raise RuntimeError(
                f"API request failed with status code {response.status_code} for endpoint {url}"
            )
        return response

    def get_eia_data(self, url, params, page_size=5000, max_seconds=1800):
        params = {**params, "length": page_size, "offset": 0}
        all_rows = []
        cutoff = time.time() + max_seconds

        while True:
            if time.time() > cutoff:
                raise TimeoutError(
                    f"EIA extract exceeded {max_seconds}s at offset {params["offset"]} - aborting"
                )
            data = self._request(url, params, timeout=(5, 300)).json()["response"]

            total = int(data["total"])
            rows = data["data"]
            if not rows:
                break

            all_rows.extend(rows)

            if len(all_rows) >= total:
                break
            params["offset"] += len(rows)

        return pd.DataFrame(all_rows)

    def get_weather_data(self, url, params):
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                response = self._request(url, params, timeout=(5, 120))
                return response.json()
            except (
                requests.exceptions.RequestException,
                requests.exceptions.JSONDecodeError,
            ) as exc:
                if attempt == attempts:
                    if isinstance(exc, requests.exceptions.JSONDecodeError):
                        raise RuntimeError(
                            f"Weather API returned invalid JSON after {attempts} attempts"
                        ) from exc
                    raise
                logger.warning(
                    "Weather API request failed (attempt %d/%d): %s",
                    attempt,
                    attempts,
                    exc,
                )
                time.sleep(2 ** (attempt - 1))
