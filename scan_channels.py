#!/usr/bin/env python3
"""
Channel range scanner for telecloud.tv-style CDN endpoints.

Sweeps CH{NNNN}/index.m3u8 across a numeric range, doing:
  Stage 1: status + content check (concurrent), with one automatic retry
           on failure (timeout/error/non-200) before giving up on a channel
  Stage 2: for any 2xx hit, verify the body looks like real HLS
           (starts with #EXTM3U, has #EXT-X-* tags, not an HTML error page)

Results MERGE with any existing playlist.m3u / scan_log.csv in the repo:
  - Existing channels not in this run's range are kept as-is.
  - Channels in this run's range are replaced with the newest result —
    including if a channel that was invalid before is now valid (e.g. it
    was offline last scan), or vice versa.

Outputs:
  - playlist.m3u   : all known valid channels (merged across runs)
  - scan_log.csv   : latest known state per channel (merged across runs)

Usage:
  python scan_channels.py --start 3000 --end 4000
  python scan_channels.py --start 0 --end 9999 --workers 30
  python scan_channels.py --ids 3225,3223,3239        # test specific list instead of a range
"""

import argparse
import asyncio
import csv
import os
import sys
from datetime import datetime, timezone

import aiohttp

BASE_HOST = "cdn.telecloud.tv"
BASE_PORT = 80
URL_TEMPLATE = "http://{host}:{port}/CH{chan}/index.m3u8"

HEADERS = {
    # Some CDNs gate on UA / referer; adjust here if you find this one does too.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=18)
RETRY_DELAY_SECONDS = 2
CSV_FIELDS = ["channel", "url", "status", "valid", "reason", "last_checked"]


def pad_channel(n: int, width: int = 4) -> str:
    """CH3225 style padding. Adjust width if your IDs aren't always 4 digits."""
    return str(n).zfill(width)


async def _attempt_check(session: aiohttp.ClientSession, url: str) -> dict:
    """Single attempt: status check + content verification. No retry logic here."""
    result = {"status": None, "valid": False, "reason": ""}

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
    return result


async def check_channel(session: aiohttp.ClientSession, chan_num: int, sem: asyncio.Semaphore):
    chan_id = pad_channel(chan_num)
    url = URL_TEMPLATE.format(host=BASE_HOST, port=BASE_PORT, chan=chan_id)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    async with sem:
        attempt = await _attempt_check(session, url)

        # One automatic retry if the first attempt failed, in case it was a
        # transient timeout/slow-start rather than a genuinely dead channel.
        if not attempt["valid"]:
            await asyncio.sleep(RETRY_DELAY_SECONDS)
            retry_attempt = await _attempt_check(session, url)
            if retry_attempt["valid"]:
                attempt = retry_attempt
            else:
                # Keep the more informative reason if attempts differ
                attempt["reason"] = f"{attempt['reason']} (retry: {retry_attempt['reason']})"

    return {
        "channel": f"CH{chan_id}",
        "url": url,
        "status": attempt["status"],
        "valid": attempt["valid"],
        "reason": attempt["reason"],
        "last_checked": now,
    }


async def run_scan(channel_nums, workers: int):
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, ssl=False)
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_channel(session, n, sem) for n in channel_nums]
        total = len(tasks)
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            res = await coro
            results.append(res)
            tag = "VALID" if res["valid"] else "-"
            print(f"[{i}/{total}] {res['channel']}: {res['status']} {tag} {res['reason']}", flush=True)

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
    """New results always win for the channels they cover; everything else is kept as-is.
    This means a channel found offline in a prior run will flip to valid here if it's
    back up now, and vice versa."""
    merged = dict(existing)
    for r in new_results:
        merged[r["channel"]] = r
    return merged


def write_outputs(merged: dict, m3u_path: str, csv_path: str):
    # Sort by channel number for nice ordering
    def chan_key(item):
        try:
            return int(item[0].replace("CH", ""))
        except ValueError:
            return 0

    rows_sorted = [r for _, r in sorted(merged.items(), key=chan_key)]
    valid = [r for r in rows_sorted if r["valid"]]

    # M3U output
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for r in valid:
            f.write(f'#EXTINF:-1 tvg-id="{r["channel"]}",{r["channel"]}\n')
            f.write(f'{r["url"]}\n')

    # CSV log (everything, valid or not)
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
    parser.add_argument("--out-m3u", type=str, default="playlist.m3u")
    parser.add_argument("--out-csv", type=str, default="scan_log.csv")
    args = parser.parse_args()

    global pad_channel
    width = args.width

    def pad_channel(n, w=width):
        return str(n).zfill(w)

    if args.ids:
        channel_nums = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    else:
        channel_nums = list(range(args.start, args.end + 1))

    print(f"Scanning {len(channel_nums)} channel IDs with {args.workers} workers...")
    start_time = datetime.now(timezone.utc)

    new_results = asyncio.run(run_scan(channel_nums, args.workers))

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
