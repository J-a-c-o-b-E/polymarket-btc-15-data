# main.py
import csv
import json
import os
import time
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, List

import httpx
import websockets
import asyncio

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"


# defaults
DEFAULT_SLUG_URL = "https://polymarket.com/event/btc-updown-15m-1766449800"
DEFAULT_QUOTE_NOTIONALS_USDC = [1, 10, 100, 1000, 10000]
DEFAULT_POLL_SECONDS = 1.0
SESSION_SECONDS = 15
DEFAULT_REFRESH_SESSION_EVERY_SECONDS = 2.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 2.5

DEFAULT_LOCAL_OUT_DIR = "session_csv"
DEFAULT_DRIVE_FOLDER_PATH = ["Polymarket Data", "btc"]


@dataclass
class SessionState:
    slug: str
    up_token_id: str
    down_token_id: str


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_slug(s: str) -> str:
    s = (s or "").strip()
    if "/event/" in s:
        return s.split("/event/", 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")
    if "/market/" in s:
        return s.split("/market/", 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")
    return s


def slug_prefix_from_slug(slug: str) -> str:
    parts = slug.rsplit("-", 1)
    return (parts[0] + "-") if len(parts) == 2 else (slug + "-")


def current_bucket_ts(bucket_seconds: int) -> int:
    now = int(time.time())
    return (now // bucket_seconds) * bucket_seconds


def maybe_json(v):
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        t = v.strip()
        if not t:
            return v
        if (t.startswith("[") and t.endswith("]")) or (t.startswith("{") and t.endswith("}")):
            try:
                return json.loads(t)
            except Exception:
                return v
    return v


class ChainlinkPriceFeed:
    def __init__(self):
        self._lock = threading.Lock()
        self.latest_price: Optional[float] = None
        self.latest_ts_ms: Optional[int] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def get_latest(self) -> Tuple[Optional[float], Optional[int]]:
        with self._lock:
            return self.latest_price, self.latest_ts_ms

    def _run_forever(self):
        asyncio.run(self._ws_loop())

    async def _ws_loop(self):
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}
            ],
        }
        backoff = 0.5
        while not self._stop.is_set():
            try:
                async with websockets.connect(RTDS_WS_URL, ping_interval=None) as ws:
                    await ws.send(json.dumps(sub_msg))
                    last_ping = time.time()
                    while not self._stop.is_set():
                        if time.time() - last_ping >= 5.0:
                            try:
                                await ws.ping()
                            except Exception:
                                break
                            last_ping = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue

                        payload = msg.get("payload") or {}
                        if (payload.get("symbol") or "").lower() != "btc/usd":
                            continue

                        value = payload.get("value")
                        ts_ms = payload.get("timestamp") or msg.get("timestamp")

                        if isinstance(value, (int, float)) and isinstance(ts_ms, int):
                            with self._lock:
                                self.latest_price = float(value)
                                self.latest_ts_ms = int(ts_ms)

                backoff = 0.5
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.6, 10.0)


def make_http_client(http_timeout_seconds: float) -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": "pm-collector/1.0"},
        timeout=httpx.Timeout(http_timeout_seconds),
        limits=httpx.Limits(max_connections=5, max_keepalive_connections=0),
        http2=False,
    )


def fetch_gamma_market_by_slug(http: httpx.Client, slug: str) -> Optional[dict]:
    try:
        r = http.get(
            f"{GAMMA_API}/markets",
            params={"slug": slug, "closed": False, "limit": 1, "offset": 0},
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        return None
    return None


def resolve_up_down_tokens(market: dict) -> Tuple[str, str]:
    outcomes = maybe_json(market.get("outcomes"))
    token_ids = maybe_json(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) < 2 or len(token_ids) < 2:
        raise RuntimeError("gamma market missing outcomes or clobTokenIds")

    outcomes_norm = [str(x).strip().lower() for x in outcomes]
    token_norm = [str(x).strip() for x in token_ids]

    if "up" in outcomes_norm and "down" in outcomes_norm:
        up_id = token_norm[outcomes_norm.index("up")]
        down_id = token_norm[outcomes_norm.index("down")]
        return up_id, down_id

    return token_norm[0], token_norm[1]


def fetch_order_book_asks_safe(
    http: httpx.Client,
    token_id: str,
    prev: List[Tuple[float, float]],
    retries: int = 3,
) -> List[Tuple[float, float]]:
    for attempt in range(retries):
        try:
            r = http.get(f"{CLOB_API}/book", params={"token_id": token_id})
            r.raise_for_status()
            data = r.json()
            asks = data.get("asks") or []
            out: List[Tuple[float, float]] = []
            for a in asks:
                try:
                    p = float(a["price"])
                    s = float(a["size"])
                    if p > 0 and s > 0:
                        out.append((p, s))
                except Exception:
                    pass
            if out:
                out.sort(key=lambda x: x[0])
                return out
            return prev
        except Exception:
            time.sleep(0.05 * (2 ** attempt))
    return prev


def avg_fill_prices_for_notionals(asks: List[Tuple[float, float]], notionals: List[float]) -> List[Optional[float]]:
    results: List[Optional[float]] = [None] * len(notionals)

    remaining = [float(n) for n in notionals]
    spent = [0.0 for _ in notionals]
    shares = [0.0 for _ in notionals]
    done = [False for _ in notionals]
    unfinished = len(notionals)

    for price, size in asks:
        if unfinished == 0:
            break

        level_cash = price * size

        for i in range(len(notionals)):
            if done[i]:
                continue

            if remaining[i] <= 1e-9:
                done[i] = True
                unfinished -= 1
                continue

            if level_cash <= remaining[i] + 1e-12:
                spent[i] += level_cash
                shares[i] += size
                remaining[i] -= level_cash
                if remaining[i] <= 1e-9:
                    done[i] = True
                    unfinished -= 1
            else:
                take_shares = remaining[i] / price
                spent[i] += remaining[i]
                shares[i] += take_shares
                remaining[i] = 0.0
                done[i] = True
                unfinished -= 1

    for i in range(len(notionals)):
        results[i] = (spent[i] / shares[i]) if shares[i] > 0 else None

    return results


def cents(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x * 100.0, 6)


class DriveUploader:
    def __init__(self, service_account_json_path: str, shared_drive_id: Optional[str] = None):
        scopes = ["https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_file(service_account_json_path, scopes=scopes)
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)
        self.shared_drive_id = shared_drive_id

    def _list(self, q: str) -> List[dict]:
        params = {
            "q": q,
            "fields": "files(id,name,mimeType,parents)",
            "pageSize": 50,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if self.shared_drive_id:
            params["corpora"] = "drive"
            params["driveId"] = self.shared_drive_id
        else:
            params["corpora"] = "user"
        res = self.service.files().list(**params).execute()
        return res.get("files", [])

    def _find_folder(self, name: str, parent_id: Optional[str]) -> Optional[str]:
        mime = "application/vnd.google-apps.folder"
        if parent_id:
            q = f"name='{name}' and mimeType='{mime}' and '{parent_id}' in parents and trashed=false"
        else:
            q = f"name='{name}' and mimeType='{mime}' and trashed=false"
        files = self._list(q)
        return files[0]["id"] if files else None

    def _create_folder(self, name: str, parent_id: Optional[str]) -> str:
        metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            metadata["parents"] = [parent_id]
        f = self.service.files().create(body=metadata, fields="id", supportsAllDrives=True).execute()
        return f["id"]

    def ensure_folder_path(self, path_parts: List[str]) -> str:
        parent = None
        for part in path_parts:
            found = self._find_folder(part, parent)
            if not found:
                found = self._create_folder(part, parent)
            parent = found
        return parent

    def upload_or_update(self, local_path: str, drive_folder_id: str, drive_filename: str):
        q = f"name='{drive_filename}' and '{drive_folder_id}' in parents and trashed=false"
        existing = self._list(q)
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)

        if existing:
            file_id = existing[0]["id"]
            self.service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
        else:
            metadata = {"name": drive_filename, "parents": [drive_folder_id]}
            self.service.files().create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()


def session_filename(prefix: str, bucket_ts: int) -> str:
    return f"{prefix}{bucket_ts}.csv"


def write_header(writer: csv.writer, quote_notionals_usdc: List[int]):
    headers = ["timestamp_utc"]
    for n in quote_notionals_usdc:
        headers.append(f"up_buy_{n}_avg_cents")
        headers.append(f"down_buy_{n}_avg_cents")
    headers.append("btc_chainlink_usd")
    writer.writerow(headers)


def load_quote_notionals() -> List[int]:
    raw = os.getenv("QUOTE_NOTIONALS_USDC", "")
    if not raw.strip():
        return DEFAULT_QUOTE_NOTIONALS_USDC
    try:
        vals = [int(x.strip()) for x in raw.split(",") if x.strip()]
        return vals if vals else DEFAULT_QUOTE_NOTIONALS_USDC
    except Exception:
        return DEFAULT_QUOTE_NOTIONALS_USDC


def load_drive_folder_path() -> List[str]:
    raw = os.getenv("DRIVE_FOLDER_PATH", "")
    if raw.strip():
        parts = [p.strip() for p in raw.split("/") if p.strip()]
        return parts if parts else DEFAULT_DRIVE_FOLDER_PATH
    root = os.getenv("DRIVE_ROOT_FOLDER", "Polymarket Data").strip() or "Polymarket Data"
    sub = os.getenv("DRIVE_SUBFOLDER", "btc").strip() or "btc"
    return [root, sub]


def ensure_service_account_file() -> str:
    env_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if env_json:
        path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", "/tmp/service_account.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(env_json)
        return path
    return os.getenv("SERVICE_ACCOUNT_JSON", "service_account.json")


def main():
    slug_url = os.getenv("SLUG_URL", DEFAULT_SLUG_URL)
    poll_seconds = float(os.getenv("POLL_SECONDS", str(DEFAULT_POLL_SECONDS)))
    refresh_session_every = float(os.getenv("REFRESH_SESSION_EVERY_SECONDS", str(DEFAULT_REFRESH_SESSION_EVERY_SECONDS)))
    http_timeout_seconds = float(os.getenv("HTTP_TIMEOUT_SECONDS", str(DEFAULT_HTTP_TIMEOUT_SECONDS)))
    local_out_dir = os.getenv("LOCAL_OUT_DIR", DEFAULT_LOCAL_OUT_DIR)

    quote_notionals_usdc = load_quote_notionals()
    notionals = [float(x) for x in quote_notionals_usdc]

    drive_folder_path = load_drive_folder_path()
    shared_drive_id = os.getenv("SHARED_DRIVE_ID", "").strip() or None
    sa_path = ensure_service_account_file()

    os.makedirs(local_out_dir, exist_ok=True)

    initial_slug = parse_slug(slug_url)
    slug_prefix = slug_prefix_from_slug(initial_slug)

    drive = DriveUploader(sa_path, shared_drive_id=shared_drive_id)
    drive_folder_id = drive.ensure_folder_path(drive_folder_path)

    feed = ChainlinkPriceFeed()
    feed.start()

    http = make_http_client(http_timeout_seconds)

    state: Optional[SessionState] = None
    last_refresh = 0.0

    last_up_asks: List[Tuple[float, float]] = []
    last_down_asks: List[Tuple[float, float]] = []

    current_bucket = current_bucket_ts(SESSION_SECONDS)
    local_file = os.path.join(local_out_dir, session_filename(slug_prefix, current_bucket))
    f = open(local_file, "w", newline="", encoding="utf-8")
    w = csv.writer(f)
    write_header(w, quote_notionals_usdc)
    f.flush()

    print("drive_folder_path", "/".join(drive_folder_path))
    print("poll_seconds", poll_seconds)
    print("session_seconds", SESSION_SECONDS)
    print("local_out_dir", os.path.abspath(local_out_dir))
    print("current_file", os.path.abspath(local_file))

    def close_and_upload(path: str, filename: str) -> bool:
        for attempt in range(5):
            try:
                drive.upload_or_update(path, drive_folder_id, filename)
                return True
            except Exception:
                time.sleep(0.5 * (2 ** attempt))
        return False

    try:
        while True:
            loop_start = time.perf_counter()
            now = time.time()
            bucket = current_bucket_ts(SESSION_SECONDS)

            if bucket != current_bucket:
                try:
                    f.flush()
                    f.close()
                except Exception:
                    pass

                fname = os.path.basename(local_file)
                ok = close_and_upload(local_file, fname)
                print("uploaded" if ok else "upload_failed", fname)

                current_bucket = bucket
                local_file = os.path.join(local_out_dir, session_filename(slug_prefix, current_bucket))
                f = open(local_file, "w", newline="", encoding="utf-8")
                w = csv.writer(f)
                write_header(w, quote_notionals_usdc)
                f.flush()

                last_up_asks = []
                last_down_asks = []
                state = None
                last_refresh = 0.0

            if state is None or (now - last_refresh) >= refresh_session_every:
                last_refresh = now
                target_slug = f"{slug_prefix}{current_bucket}"
                try:
                    m = fetch_gamma_market_by_slug(http, target_slug)
                    if m:
                        up_id, down_id = resolve_up_down_tokens(m)
                        if state is None or state.slug != target_slug:
                            state = SessionState(slug=target_slug, up_token_id=up_id, down_token_id=down_id)
                            print("switched_session", state.slug)
                except Exception:
                    pass

            if state is None:
                time.sleep(0.05)
                continue

            try:
                up_asks = fetch_order_book_asks_safe(http, state.up_token_id, last_up_asks)
                if up_asks:
                    last_up_asks = up_asks

                down_asks = fetch_order_book_asks_safe(http, state.down_token_id, last_down_asks)
                if down_asks:
                    last_down_asks = down_asks

            except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError, httpx.PoolTimeout):
                try:
                    http.close()
                except Exception:
                    pass
                http = make_http_client(http_timeout_seconds)
                time.sleep(0.1)
                continue

            if not last_up_asks or not last_down_asks:
                time.sleep(0.05)
                continue

            up_avg = avg_fill_prices_for_notionals(last_up_asks, notionals)
            down_avg = avg_fill_prices_for_notionals(last_down_asks, notionals)

            btc_price, _ = feed.get_latest()
            ts = utc_iso_now()

            row = [ts]
            for i in range(len(notionals)):
                row.extend([cents(up_avg[i]), cents(down_avg[i])])
            row.append(btc_price)

            w.writerow(row)
            f.flush()

            elapsed = time.perf_counter() - loop_start
            sleep_for = poll_seconds - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            f.flush()
            f.close()
        except Exception:
            pass

        try:
            http.close()
        except Exception:
            pass

        feed.stop()

        try:
            fname = os.path.basename(local_file)
            ok = close_and_upload(local_file, fname)
            print("uploaded" if ok else "upload_failed", fname)
        except Exception:
            pass


if __name__ == "__main__":
    main()
