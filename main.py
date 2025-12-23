# main.py
import asyncio
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

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError


# -----------------------------
# endpoints
# -----------------------------
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

SCOPES = ["https://www.googleapis.com/auth/drive"]


# -----------------------------
# defaults (override via env)
# -----------------------------
DEFAULT_SLUG_URL = "https://polymarket.com/event/btc-updown-15m-1766449800"
DEFAULT_QUOTE_NOTIONALS_USDC = [1, 10, 100, 1000, 10000]

DEFAULT_POLL_SECONDS = 1.0
DEFAULT_MARKET_BUCKET_SECONDS = 15 * 60  # 900 seconds
DEFAULT_REFRESH_SESSION_EVERY_SECONDS = 2.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 2.5
DEFAULT_LOCAL_OUT_DIR = "session_csv"


# -----------------------------
# helpers
# -----------------------------
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


def load_int_list_env(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        vals = [int(x.strip()) for x in raw.split(",") if x.strip()]
        return vals if vals else default
    except Exception:
        return default


def cents(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x * 100.0, 6)


def fsync_flush(f):
    try:
        f.flush()
    except Exception:
        return
    try:
        os.fsync(f.fileno())
    except Exception:
        pass


# -----------------------------
# chainlink BTC feed via polymarket websocket
# -----------------------------
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


# -----------------------------
# polymarket fetch
# -----------------------------
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
                out.sort(key=lambda x: x[0])  # cheapest first
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


# -----------------------------
# google drive (oauth user)
# -----------------------------
class DriveUploader:
    def __init__(self):
        token_json = os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "").strip()
        if not token_json:
            raise RuntimeError("missing GOOGLE_OAUTH_TOKEN_JSON")

        token_info = json.loads(token_json)
        creds = Credentials.from_authorized_user_info(token_info, scopes=SCOPES)

        if not creds.valid and creds.refresh_token:
            creds.refresh(Request())

        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def _list(self, q: str) -> List[dict]:
        res = self.service.files().list(
            q=q,
            fields="files(id,name,mimeType,parents)",
            pageSize=50,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        return res.get("files", [])

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


# -----------------------------
# csv
# -----------------------------
def session_filename(prefix: str, bucket_ts: int) -> str:
    return f"{prefix}{bucket_ts}.csv"


def write_header(writer: csv.writer, quote_notionals_usdc: List[int]):
    headers = ["timestamp_utc"]
    for n in quote_notionals_usdc:
        headers.append(f"up_buy_{n}_avg_cents")
        headers.append(f"down_buy_{n}_avg_cents")
    headers.append("btc_chainlink_usd")
    writer.writerow(headers)


def ensure_file_with_header(path: str, quote_notionals_usdc: List[int]) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        write_header(w, quote_notionals_usdc)
        fsync_flush(f)


# -----------------------------
# main loop
# -----------------------------
def main():
    slug_url = os.getenv("SLUG_URL", DEFAULT_SLUG_URL)
    poll_seconds = float(os.getenv("POLL_SECONDS", str(DEFAULT_POLL_SECONDS)))
    market_bucket_seconds = int(os.getenv("MARKET_BUCKET_SECONDS", str(DEFAULT_MARKET_BUCKET_SECONDS)))
    refresh_session_every = float(os.getenv("REFRESH_SESSION_EVERY_SECONDS", str(DEFAULT_REFRESH_SESSION_EVERY_SECONDS)))
    http_timeout_seconds = float(os.getenv("HTTP_TIMEOUT_SECONDS", str(DEFAULT_HTTP_TIMEOUT_SECONDS)))
    local_out_dir = os.getenv("LOCAL_OUT_DIR", DEFAULT_LOCAL_OUT_DIR)

    quote_notionals_usdc = load_int_list_env("QUOTE_NOTIONALS_USDC", DEFAULT_QUOTE_NOTIONALS_USDC)
    notionals = [float(x) for x in quote_notionals_usdc]

    drive_folder_id = os.getenv("DRIVE_FOLDER_ID", "").strip()
    if not drive_folder_id:
        raise RuntimeError("missing DRIVE_FOLDER_ID env var")

    os.makedirs(local_out_dir, exist_ok=True)

    initial_slug = parse_slug(slug_url)
    slug_prefix = slug_prefix_from_slug(initial_slug)

    drive = DriveUploader()

    # one-time startup upload test
    test_path = os.path.join(local_out_dir, "drive_test.csv")
    with open(test_path, "w", encoding="utf-8") as tf:
        tf.write("ok,utc\n")
        tf.write(f"1,{utc_iso_now()}\n")
    try:
        drive.upload_or_update(test_path, drive_folder_id, "drive_test.csv")
        print("drive_test_upload_ok")
    except Exception as e:
        print("drive_test_upload_failed", type(e).__name__, repr(e))

    feed = ChainlinkPriceFeed()
    feed.start()

    http = make_http_client(http_timeout_seconds)

    state: Optional[SessionState] = None
    last_refresh = 0.0

    last_up_asks: List[Tuple[float, float]] = []
    last_down_asks: List[Tuple[float, float]] = []

    current_bucket = current_bucket_ts(market_bucket_seconds)
    current_slug = f"{slug_prefix}{current_bucket}"

    local_file = os.path.join(local_out_dir, session_filename(slug_prefix, current_bucket))
    ensure_file_with_header(local_file, quote_notionals_usdc)

    f = open(local_file, "a", newline="", encoding="utf-8")
    w = csv.writer(f)

    print("drive_folder_id", drive_folder_id)
    print("poll_seconds", poll_seconds)
    print("market_bucket_seconds", market_bucket_seconds)
    print("local_out_dir", os.path.abspath(local_out_dir))
    print("current_file", os.path.abspath(local_file))

    def upload_with_logs(path: str, filename: str) -> bool:
        for attempt in range(5):
            try:
                drive.upload_or_update(path, drive_folder_id, filename)
                return True
            except HttpError as e:
                print(
                    "upload_error_http",
                    "attempt", attempt + 1,
                    "status", getattr(e, "status_code", None),
                    "error", str(e),
                )
            except Exception as e:
                print(
                    "upload_error",
                    "attempt", attempt + 1,
                    "type", type(e).__name__,
                    "error", repr(e),
                )
            time.sleep(0.5 * (2 ** attempt))
        return False

    try:
        while True:
            loop_start = time.perf_counter()
            now = time.time()

            # rotate at bucket boundary, upload previous file once (end of session)
            bucket = current_bucket_ts(market_bucket_seconds)
            if bucket != current_bucket:
                try:
                    fsync_flush(f)
                    f.close()
                except Exception:
                    pass

                prev_name = os.path.basename(local_file)
                ok = upload_with_logs(local_file, prev_name)
                print("uploaded" if ok else "upload_failed", prev_name)

                current_bucket = bucket
                current_slug = f"{slug_prefix}{current_bucket}"
                local_file = os.path.join(local_out_dir, session_filename(slug_prefix, current_bucket))
                ensure_file_with_header(local_file, quote_notionals_usdc)

                f = open(local_file, "a", newline="", encoding="utf-8")
                w = csv.writer(f)

                # reset per-session caches
                last_up_asks = []
                last_down_asks = []
                state = None
                last_refresh = 0.0

            # refresh tokens for current session slug
            if state is None or (now - last_refresh) >= refresh_session_every:
                last_refresh = now
                try:
                    m = fetch_gamma_market_by_slug(http, current_slug)
                    if m:
                        up_id, down_id = resolve_up_down_tokens(m)
                        if state is None or state.slug != current_slug:
                            state = SessionState(slug=current_slug, up_token_id=up_id, down_token_id=down_id)
                            print("switched_session", state.slug)
                except Exception:
                    pass

            if state is None:
                time.sleep(0.05)
                continue

            # books
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
                time.sleep(0.15)
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
            fsync_flush(f)

            elapsed = time.perf_counter() - loop_start
            sleep_for = poll_seconds - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            fsync_flush(f)
            f.close()
        except Exception:
            pass

        try:
            http.close()
        except Exception:
            pass

        feed.stop()

        # final upload for the currently-open session file
        try:
            name = os.path.basename(local_file)
            ok = upload_with_logs(local_file, name)
            print("uploaded" if ok else "upload_failed", name)
        except Exception:
            pass


if __name__ == "__main__":
    main()
