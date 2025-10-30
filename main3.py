#!/usr/bin/env python3
# batch_session_to_tdata.py

import argparse
import asyncio
import csv
import logging
import time
from pathlib import Path
from telethon import TelegramClient
from opentele.td import TDesktop
from opentele.api import UseCurrentSession

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger("batch_convert")


async def convert_session(session_path: Path, api_id: int, api_hash: str, out_dir: Path, timeout: int):
    result = {
        "session_file": str(session_path),
        "status": "failed",
        "error": "",
        "duration_s": 0.0
    }
    start = time.time()
    try:
        client = TelegramClient(str(session_path), api_id, api_hash)
        await client.connect()

        me = await client.get_me()
        if not me or getattr(me, 'id', None) is None:
            result["error"] = "Session not authorized / no user id"
            await client.disconnect()
            return result

        # convert to tdata
        tdesk = await client.ToTDesktop(flag=UseCurrentSession)
        session_out = out_dir / session_path.stem
        session_out.mkdir(parents=True, exist_ok=True)
        tdesk.SaveTData(str(session_out))
        await client.disconnect()
        result["status"] = "ok"

    except Exception as e:
        result["error"] = str(e)
    finally:
        result["duration_s"] = round(time.time() - start, 2)
    return result


async def main(args):
    sessions_dir = Path(args.sessions)
    out_dir = Path(args.out)
    out_dir.mkdir(exist_ok=True, parents=True)
    csv_path = out_dir / "report.csv"

    # collect all .session files
    session_files = list(sessions_dir.glob("*.session"))
    if not session_files:
        log.error("No .session files found in %s", sessions_dir)
        return

    results = []
    sem = asyncio.Semaphore(args.threads)

    async def sem_task(s):
        async with sem:
            return await convert_session(s, args.api_id, args.api_hash, out_dir, args.timeout)

    tasks = [sem_task(s) for s in session_files]
    for coro in asyncio.as_completed(tasks):
        res = await coro
        results.append(res)
        log.info("Processed %s -> %s (%s s)", res["session_file"], res["status"], res["duration_s"])

    # write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["session_file", "status", "error", "duration_s"])
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    log.info("Batch conversion finished. Report saved to %s", csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch convert Telethon sessions to tdata (skipping invalid)")
    parser.add_argument("--sessions", required=True, help="Path to directory containing .session files")
    parser.add_argument("--out", default="tdata_out", help="Output folder for tdata")
    parser.add_argument("--api-id", required=True, type=int, help="API ID")
    parser.add_argument("--api-hash", required=True, help="API Hash")
    parser.add_argument("--threads", type=int, default=10, help="Concurrent workers")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout per session (s)")
    args = parser.parse_args()

    asyncio.run(main(args))
