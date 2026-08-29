"""Render a PDF inside the built Docker image, through the runner's own code path.

Run by the CI `image` job against the image it just built, not by pytest — hence
the name, which keeps it out of collection.

It earns a script of its own rather than another unit test because what it checks
is a property of the Dockerfile and its base image, not of the code: that
`render_pdf` works with the system libraries and fonts *this image* has. The unit
tests in `test_report_pdf.py` skip themselves when WeasyPrint cannot render, which
is right for a developer machine and useless as a guarantee about a published
artifact.

The two font failures look nothing alike, so it checks for both. With no font
installed at all, Pango fails its own assertions and the process dies — observed
as `pango_font_describe_with_absolute_size: assertion 'font != NULL' failed`
followed by a segfault, which any non-zero exit catches. Subtler is a font present
but not usable for the requested family: WeasyPrint then renders happily and emits
a valid PDF full of empty boxes. Nothing about the bytes says so except the
absence of an embedded font program, which is what `font_evidence` looks for.
"""

from __future__ import annotations

import re
import sys
import zlib

from orionbelt_runner.client import ExecuteResult
from orionbelt_runner.report import render_pdf
from orionbelt_runner.spec import ReportSection, ReportSpec


def font_evidence(pdf: bytes) -> tuple[set[str], int]:
    """Return the font names a PDF references, and how many font programs it embeds.

    Both live inside Flate-compressed object streams in WeasyPrint's output, so a
    search over the raw bytes finds neither. Streams that do not decompress are
    skipped rather than treated as an error: images and other binary payloads are
    expected to be there.
    """
    searchable = [pdf]
    for chunk in re.findall(rb"stream\r?\n(.*?)\r?\nendstream", pdf, re.S):
        try:
            searchable.append(zlib.decompress(chunk))
        except zlib.error:
            continue
    blob = b"".join(searchable)
    names = {name.decode() for name in re.findall(rb"/BaseFont\s*/([\w+-]+)", blob)}
    return names, len(re.findall(rb"/FontFile", blob))


def main() -> int:
    pdf = render_pdf(
        ReportSpec(
            output="smoke.pdf",
            title="Smoke",
            format="pdf",
            sections=[ReportSection(heading="Revenue by country", query="q", render="table")],
        ),
        {
            "q": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Revenue"],
                rows=[["DE", 5000], ["US", 7345]],
                row_count=2,
            )
        },
        context={"date": "2026-05-04"},
    )

    if pdf[:5] != b"%PDF-":
        print(f"FAIL: render_pdf did not return a PDF: {pdf[:20]!r}", file=sys.stderr)
        return 1

    names, embedded = font_evidence(pdf)
    if not embedded:
        print(
            f"FAIL: the PDF embeds no font program (referenced: {sorted(names) or 'none'}). "
            f"WeasyPrint rendered, but had nothing to draw glyphs with — check that a "
            f"font package is installed in the image.",
            file=sys.stderr,
        )
        return 1

    print(f"ok: {len(pdf)} bytes, {embedded} font programs embedded: {', '.join(sorted(names))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
