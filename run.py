#!/usr/bin/env python3
"""
Local launcher for pdi2airflow.

Starts the FastAPI app and (on most systems) opens the browser to it.
Run from the project root:

    python run.py             # default port 8765
    python run.py --port 9000

Equivalent to: uvicorn backend.app:app --port 8765
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import webbrowser


def _open_browser_when_ready(url: str, delay: float = 1.5) -> None:
    time.sleep(delay)
    try:
        webbrowser.open(url)
    except Exception:
        pass


def main() -> int:
    p = argparse.ArgumentParser(description="Run pdi2airflow locally.")
    p.add_argument("--port", type=int, default=8765, help="port to bind (default 8765)")
    p.add_argument("--host", default="127.0.0.1", help="host to bind (default 127.0.0.1)")
    p.add_argument("--no-browser", action="store_true", help="do not auto-open the browser")
    p.add_argument("--reload", action="store_true", help="reload on code changes (dev)")
    args = p.parse_args()

    try:
        import uvicorn  # type: ignore[import-not-found]
    except ImportError:
        sys.stderr.write(
            "uvicorn is not installed. Install it with:\n"
            "    pip install -r requirements.txt\n"
        )
        return 1

    url = f"http://{args.host}:{args.port}/"
    if not args.no_browser:
        threading.Thread(
            target=_open_browser_when_ready, args=(url,), daemon=True
        ).start()

    print(f"\npdi2airflow → {url}\n")
    uvicorn.run(
        "backend.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
