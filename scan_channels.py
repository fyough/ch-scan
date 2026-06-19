#!/usr/bin/env python3
"""
Channel range scanner for telecloud.tv-style CDN endpoints.

Sweeps CH{NNNN}/index.m3u8 across a numeric range, doing:
  Stage 1: fast status + content check (concurrent) — confirms it's real HLS
  Stage 2: for valid hits, try to extract the real channel name from:
             a) text hints inside the m3u8 (#EXT-X-SESSION-DATA, comments, etc.)
             b) ID3 tags (TIT2/TXXX) embedded in the first media segment

Results MERGE with any existing playlist.m3u / scan_log.csv in the repo:
  - Existing channels not in this run's range are kept as-is.
  - Channels in this run's range are replaced with the newest result
    (whether that's now valid, now invalid, or a new/updated name).

Outputs:
  - playlist.m3u   : all known valid channels (merged across runs)
  - scan_log.csv   : full history-less log of latest known state per channel

Usage:
  python scan_channels.py --start 3000 --end 4000
  python scan_channels.py --start 0 --end 9999 --workers 30
  python scan_channels.py --ids 3225,3223,3239        # test specific list instead of a range
  python scan_channels.py --start 3000 --end 3100 --no-name-lookup   # skip ID3 digging, faster
"""

import argparse
import asyncio
import csv
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urljoin

import aiohttp

try:
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3NoHeaderError
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False

BASE_HOST = "cdn.telecloud.tv"
BASE_PORT = 80
URL_TEMPLATE = "http://{host}:{port}/CH{chan}/index.m3u8"

HEADERS = {
    # Some CDNs gate on UA / referer; adjust here if you find this one does too.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
SEGMENT_TIMEOUT = aiohttp.ClientTimeout(total=8)
SEGMENT_BYTE_CAP = 2_000_000  # don't pull more than ~2MB of a segment just to read tags

CSV_FIELDS = ["channel", "url", "name", "status", "valid", "reason", "last_checked"]


def pad_channel(n: int, width: int = 4) -> str:
    return str(n).zfill(width)


def extract_name_from_text(m3u8_text: str) -> str:
    """Look for name hints inside the m3u8 playlist text itself."""
    # #EXT-X-SESSION-DATA:DATA-ID="...",VALUE="Channel Name"
    m = re.search(r'#EXT-X-SESSION-DATA:.*VALUE="([^"]+)"', m3u8_text)
    if m:
        return m.group(1).strip()

    # Some providers stick a name in a comment line like: # Channel: TV LATVIA
    m = re.search(r'#\s*(?:Channel|Name|Title)\s*[:=]\s*(.+)', m3u8_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # #EXTINF line with a name (rare for live index, but check anyway)
    m = re.search(r'#EXTINF:[-\d.]*\s*,\s*(.+)', m3u8_text)
    if m and m.group(1).strip() and m.group(1).strip() != "-1":
        return m.group(1).strip()

    return ""


def first_segment_url(m3u8_text: str, base_url: str) -> str:
    """Find the first .ts/.aac/.m4s segment (or nested variant playlist) referenced."""
    for line in m3u8_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        return urljoin(base_url, line)
    return ""


async def extract_name_from_segment(session: aiohttp.ClientSession, segment_url: str) -> str:
    """Download the start of a media segment and look for ID3 TIT2/TXXX tags."""
    if not segment_url or not MUTAGEN_AVAILABLE:
        return ""

    try:
        async with session.get(segment_url, headers=HEADERS, timeout=SEGMENT_TIMEOUT) as resp:
            if resp.status != 200:
                return ""
            chunk = await resp.content.read(SEGMENT_BYTE_CAP)
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return ""

    if not chunk:
        return ""

    # Write to a temp buffer mutagen can read (it needs a file-like/seekable object)
    import io
    buf = io.BytesIO(chunk)
    try:
        audio = MP3(buf)
    except Exception:
        return ""

    if audio.tags is None:
        return ""

    for key in ("TIT2", "TIT1"):
        if key in audio.tags:
            val = str(audio.tags[key])
            if val.strip():
                return val.strip()

    for key in audio.tags.keys():
        if key.startswith("TXXX"):
            val = str(audio.tags[key])
            if val.strip():
                return val.strip()

    return ""


async def check_channel(session: aiohttp.ClientSession, chan_num: int, sem: asyncio.Semaphore,
                         width: int, lookup_name: bool):
    chan_id = pad_channel(chan_num, width)
    url = URL_TEMPLATE.format(host=BASE_HOST, port=BASE_PORT, chan=chan_id)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    result = {
        "channel": f"CH{chan_id}",
        "url": url,
        "name": "",
        "status": None,
        "valid": False,
        "reason": "",
        "last_checked": now,
    }

    async with sem:
        try:
            async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
                result["status"] = resp.status
                if resp.status != 200:
                    result["reason"] = f"HTTP {resp.status}"
                    return result
                body = await resp.text(errors="ignore")
        except asyncio.TimeoutError:
            result["reason"] = "timeout"
            return result
        except aiohttp.ClientError as e:
            result["reason"] = f"client_error: {e}"
            return result

        stripped = body.strip()
        if not stripped.startswith("#EXTM3U"):
            result["reason"] = "not_m3u8 (no #EXTM3U header)"
            return result
        if "#EXT-X-" not in stripped:
            result["reason"] = "no HLS tags found"
            return result
        if "<html" in stripped.lower():
            result["reason"] = "html_error_page"
            return result

        result["valid"] = True
        result["reason"] = "ok"

        # Try text-based name hint first (cheap)
        name = extract_name_from_text(stripped)

        # If no luck and lookup is enabled, dig into the first segment's ID3 tags
        if not name and lookup_name:
            seg_url = first_segment_url(stripped, url)
            name = await extract_name_from_segment(session, seg_url)

        result["name"] = name or result["channel"]
        return result


async def run_scan(channel_nums, workers: int, width: int, lookup_name: bool):
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, ssl=False)
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_channel(session, n, sem, width, lookup_name) for n in channel_nums]
        total = len(tasks)
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            res = await coro
            results.append(res)
            tag = "VALID" if res["valid"] else "-"
            label = res["name"] if res["valid"] else res["reason"]
            print(f"[{i}/{total}] {res['channel']}: {res['status']} {tag} {label}", flush=True)

    return results


def load_existing_csv(csv_path: str) -> dict:
    """Load prior results keyed by channel id. Returns {} if file doesn't exist."""
    existing = {}
    if not os.path.exists(csv_path):
        return existing
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["valid"] = row.get("valid", "False") == "True"
            existing[row["channel"]] = row
    return existing


def merge_results(existing: dict, new_results: list) -> dict:
    """New results always win for the channels they cover; everything else is kept."""
    merged = dict(existing)
    for r in new_results:
        merged[r["channel"]] = r
    return merged


def write_outputs(merged: dict, m3u_path: str, csv_path: str):
    def chan_key(item):
        try:
            return int(item[0].replace("CH", ""))
        except ValueError:
            return 0

    rows_sorted = [r for _, r in sorted(merged.items(), key=chan_key)]
    valid = [r for r in rows_sorted if r["valid"]]

    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for r in valid:
            display_name = r.get("name") or r["channel"]
            f.write(f'#EXTINF:-1 tvg-id="{r["channel"]}",{display_name}\n')
            f.write(f'{r["url"]}\n')

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows_sorted:
            row = {k: r.get(k, "") for k in CSV_FIELDS}
            writer.writerow(row)

    return valid, rows_sorted


def main():
    parser = argparse.ArgumentParser(description="Scan a CDN channel ID range for live HLS streams.")
    parser.add_argument("--start", type=int, default=0, help="Start of channel number range (inclusive)")
    parser.add_argument("--end", type=int, default=9999, help="End of channel number range (inclusive)")
    parser.add_argument("--ids", type=str, default=None,
                         help="Comma-separated explicit list of channel numbers, overrides --start/--end")
    parser.add_argument("--workers", type=int, default=25, help="Concurrent requests")
    parser.add_argument("--width", type=int, default=4, help="Zero-pad width for channel number (e.g. 4 -> CH0007)")
    parser.add_argument("--no-name-lookup", action="store_true",
                         help="Skip ID3 segment lookup, only use text hints (faster)")
    parser.add_argument("--out-m3u", type=str, default="playlist.m3u")
    parser.add_argument("--out-csv", type=str, default="scan_log.csv")
    args = parser.parse_args()

    if args.ids:
        channel_nums = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    else:
        channel_nums = list(range(args.start, args.end + 1))

    lookup_name = not args.no_name_lookup
    if lookup_name and not MUTAGEN_AVAILABLE:
        print("WARNING: mutagen not installed, falling back to text-only name detection. "
              "Run: pip install mutagen", file=sys.stderr)

    print(f"Scanning {len(channel_nums)} channel IDs with {args.workers} workers "
          f"(name lookup: {'on' if lookup_name else 'off'})...")
    start_time = datetime.now(timezone.utc)

    new_results = asyncio.run(run_scan(channel_nums, args.workers, args.width, lookup_name))

    existing = load_existing_csv(args.out_csv)
    merged = merge_results(existing, new_results)
    valid, all_rows = write_outputs(merged, args.out_m3u, args.out_csv)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    new_valid_count = sum(1 for r in new_results if r["valid"])
    print(f"\nDone in {elapsed:.1f}s. This run: {new_valid_count}/{len(channel_nums)} valid.")
    print(f"Total known valid channels across all runs: {len(valid)}/{len(all_rows)}")
    print(f"Wrote {args.out_m3u} and {args.out_csv}")

    if not valid:
        print("No valid channels found.", file=sys.stderr)


if __name__ == "__main__":
    main()
