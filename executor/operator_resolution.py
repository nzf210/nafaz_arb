"""Apply an HMAC-signed operator resolution to one unresolved submit.

The signed envelope must be produced by an authorized operator workflow. The
shared HMAC secret is supplied out-of-band through a secret manager; never put
it in the envelope or commit it to the repository.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from executor.global_stop import GlobalStopService


async def _apply(envelope: dict) -> dict:
    dsn = os.getenv("POSTGRES_DSN", "").strip()
    secret = os.getenv("OPERATOR_RESOLUTION_HMAC_SECRET", "")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    if not secret:
        raise RuntimeError("OPERATOR_RESOLUTION_HMAC_SECRET is required")
    service = GlobalStopService(dsn, min_size=1, max_size=1)
    await service.start()
    try:
        return await service.resolve_submit_attempt_signed(envelope, secret=secret)
    finally:
        await service.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve one ambiguous submit with signed operator evidence")
    parser.add_argument("--envelope", required=True, help="path to the signed JSON envelope")
    args = parser.parse_args()
    envelope = json.loads(Path(args.envelope).read_text(encoding="utf-8"))
    result = asyncio.run(_apply(envelope))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
