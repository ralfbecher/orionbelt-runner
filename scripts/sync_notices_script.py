#!/usr/bin/env python
"""Push this repo's third_party_notices.py into the sibling repos that vendor it.

The generator is vendored rather than packaged, so that the notice and the code
that produced it are reviewable in one diff. The cost of that choice is drift, and
this is what pays it: the file is split on its two `PER-REPO CONFIGURATION`
banners, each sibling keeps its own middle section, and the shared half is
replaced wholesale from here.

    uv run --no-sync python scripts/sync_notices_script.py --check   # CI / pre-flight
    uv run --no-sync python scripts/sync_notices_script.py           # write

`--check` reports which siblings have drifted without touching them, and is the
useful mode: it answers "is a fix I made here still only here?".

Siblings are located relative to this checkout, so this works on a machine where
they are cloned side by side and reports the rest as absent rather than failing.
Repos that deliberately keep their own generator are not listed and must not be
added: orionbelt-analytics and orionbelt-ontology-builder answer different
questions with their notices (a version-free register whose texts ship in the
image, and an inventory of bundled ontology data), and overwriting either with
this one would lose the reasoning they contain.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parent.parent
SIBLINGS = ("orionbelt-chat", "orionbelt-semantic-layer-mcp")

OPEN_BANNER = "# PER-REPO CONFIGURATION\n"
CLOSE_BANNER = "# END PER-REPO CONFIGURATION"
RULE = "# " + "═" * 79 + "\n"


def split(text: str, where: str) -> tuple[str, str, str]:
    """(shared preamble, per-repo config, shared logic).

    Raises rather than guessing: a copy whose banners have been edited away cannot
    be synced safely, because the boundary between "yours" and "ours" is the only
    thing protecting that repo's own configuration from being overwritten.
    """
    try:
        open_at = text.index(OPEN_BANNER)
        close_at = text.index(CLOSE_BANNER)
        head_end = text.rindex(RULE, 0, open_at)
    except ValueError as exc:
        raise SystemExit(
            f"error: {where} has no intact PER-REPO CONFIGURATION banners, so the "
            f"shared half cannot be told from the local one. Restore them by hand "
            f"before syncing."
        ) from exc
    tail_start = text.index("\n", text.index("\n", close_at) + 1) + 1
    return text[:head_end], text[head_end:tail_start], text[tail_start:]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="report drift without writing anything"
    )
    args = parser.parse_args()

    source = (ROOT / "scripts/third_party_notices.py").read_text(encoding="utf-8")
    canonical_head, _, canonical_tail = split(source, "this repo's copy")

    drifted, missing, synced = [], [], []
    for name in SIBLINGS:
        target = ROOT.parent / name / "scripts/third_party_notices.py"
        if not target.exists():
            missing.append(name)
            continue
        current = target.read_text(encoding="utf-8")
        head, config, tail = split(current, str(target))
        if head == canonical_head and tail == canonical_tail:
            synced.append(name)
            continue
        drifted.append(name)
        if not args.check:
            target.write_text(canonical_head + config + canonical_tail, encoding="utf-8")

    for name in synced:
        print(f"  up to date  {name}")
    for name in missing:
        print(f"  not cloned  {name}")
    for name in drifted:
        print(f"  {'DRIFTED' if args.check else 'updated'}     {name}")

    if drifted and args.check:
        print(
            f"\nerror: {len(drifted)} sibling(s) carry an out-of-date shared half. "
            f"Run this without --check to update them, and commit each repo "
            f"separately.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
