#!/usr/bin/env python3
"""
Channel range scanner for peaky.techcoder40.workers.dev.

Sweeps {N}.m3u8 across a numeric range, doing:
  Stage 1: fast HEAD/GET status check (concurrent)
  Stage 2: for any 2xx hit, fetch body and verify it looks like real HLS
           (starts with #EXTM3U, has #EXT-X-* tags, not an HTML error page)

Outputs:
  - playlist_peaky.m3u   : valid channels as a playable M3U
  - scan_log_peaky.csv   : every channel tried + result (status, valid, reason)

Usage:
  python scan_channels_peaky.py --start 1 --end 200
  python scan_channels_peaky.py --start 0 --end 999 --workers 30
  python scan_channels_peaky.py --ids 79,80,81        # test specific list instead of a range
"""

import argparse
import asyncio
import csv
import sys
from datetime import datetime, timezone

import aiohttp

BASE_URL = "https://peaky.techcoder40.workers.dev"
URL_TEMPLATE = "{base}/{chan}.m3u8"

HEADERS = {
    # Some CDNs gate on UA / referer; adjust here if you find this one does too.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)


async def check_channel(session: aiohttp.ClientSession, chan_num: int, sem: asyncio.Semaphore):
    chan_id = str(chan_num)
    url = URL_TEMPLATE.format(base=BASE_URL, chan=chan_id)

    result = {
        "channel": chan_id,
        "url": url,
        "status": None,
        "valid": False,
        "reason": "",
    }

    async with sem:
        # Stage 1: status check
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

        # Stage 2: content verification
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


def write_outputs(results, m3u_path: str, csv_path: str):
    def chan_key(r):
        try:
            return int(r["channel"])
        except ValueError:
            return 0

    results_sorted = sorted(results, key=chan_key)
    valid = [r for r in results_sorted if r["valid"]]

    # M3U output
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for r in valid:
            f.write(f'#EXTINF:-1 tvg-id="{r["channel"]}",{r["channel"]}\n')
            f.write(f'{r["url"]}\n')

    # CSV log (everything, valid or not)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["channel", "url", "status", "valid", "reason"])
        writer.writeheader()
        writer.writerows(results_sorted)

    return valid


def main():
    parser = argparse.ArgumentParser(description="Scan peaky.techcoder40.workers.dev for live HLS streams.")
    parser.add_argument("--start", type=int, default=0, help="Start of channel number range (inclusive)")
    parser.add_argument("--end", type=int, default=999, help="End of channel number range (inclusive)")
    parser.add_argument("--ids", type=str, default=None,
                         help="Comma-separated explicit list of channel numbers, overrides --start/--end")
    parser.add_argument("--workers", type=int, default=25, help="Concurrent requests")
    parser.add_argument("--out-m3u", type=str, default="playlist_peaky.m3u")
    parser.add_argument("--out-csv", type=str, default="scan_log_peaky.csv")
    args = parser.parse_args()

    if args.ids:
        channel_nums = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
    else:
        channel_nums = list(range(args.start, args.end + 1))

    print(f"Scanning {len(channel_nums)} channel IDs with {args.workers} workers...")
    start_time = datetime.now(timezone.utc)

    results = asyncio.run(run_scan(channel_nums, args.workers))
    valid = write_outputs(results, args.out_m3u, args.out_csv)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\nDone in {elapsed:.1f}s. {len(valid)}/{len(channel_nums)} channels valid.")
    print(f"Wrote {args.out_m3u} and {args.out_csv}")

    if not valid:
        print("No valid channels found.", file=sys.stderr)


if __name__ == "__main__":
    main()
