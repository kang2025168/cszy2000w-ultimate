#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ultimate_v1.strategy_c_watchlist import sync_strategy_c_watchlist


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync the canonical 25-stock Strategy C watchlist.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing the database.")
    parser.add_argument(
        "--keep-legacy",
        action="store_true",
        help="Keep non-canonical C records instead of pruning them.",
    )
    args = parser.parse_args()
    result = sync_strategy_c_watchlist(dry_run=args.dry_run, prune_legacy=not args.keep_legacy)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
