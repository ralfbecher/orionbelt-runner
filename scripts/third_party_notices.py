#!/usr/bin/env python
"""Generate (or verify) THIRD-PARTY-NOTICES.md for the redistributed dependencies.

The runner's wheel ships only ``src/orionbelt_runner`` — installers fetch the
dependencies themselves, so a wheel carries no third-party attribution duty. The
**Docker image does**: it copies a fully populated ``.venv``, which redistributes
every runtime package as a binary, and MIT / BSD / Apache-2.0 / MPL all require
their notice to travel with that binary.

Rather than trust that, this script derives the notice file from ``uv.lock`` (the
same resolution the image is built from) and the license texts each wheel
installs into ``*.dist-info/licenses/``.

    uv sync --locked --extra dev                                  # `dev` pulls in
                                                                  # pyarrow + weasyprint,
                                                                  # so the whole closure
                                                                  # is importable
    uv run --no-sync python scripts/third_party_notices.py         # write the file
    uv run --no-sync python scripts/third_party_notices.py --check # CI: policy + drift

``--check`` fails when the file is stale *and* when a dependency introduces a
license that is not permissive and not explicitly acknowledged below — the point
being that a new copyleft dependency has to be a decision someone made, not one
that arrives with a lockfile bump.
"""

from __future__ import annotations

import argparse
import importlib.metadata as importlib_metadata
import re
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import Any, Literal

from packaging.markers import Marker

ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = ROOT / "uv.lock"
NOTICES_PATH = ROOT / "THIRD-PARTY-NOTICES.md"
DOCKERFILE_PATH = ROOT / "Dockerfile"
OVERRIDE_DIR = Path(__file__).resolve().parent / "license-overrides"

ROOT_PACKAGE = "orionbelt-runner"

# Extras this file accounts for. Not the same question as what gets redistributed:
# the image installs only what the Dockerfile asks for (see dockerfile_extras), and
# packages reachable only through an extra it skips are listed as informational —
# a user can install them, but pip fetches them from PyPI, not from us. `dev` is in
# neither set: pytest, ruff and mypy are tools the project runs, not works it ships.
NOTICED_EXTRAS = ("arrow", "pdf")

# The published image is linux/CPython; the wheel supports every minor from
# requires-python upward. Union the marker evaluation across those so a
# `python_full_version < '3.13'` dependency (typing-extensions) is still
# credited, and a win32-only or PyPy-only one is not.
_BASE_ENV = {
    "os_name": "posix",
    "sys_platform": "linux",
    "platform_system": "Linux",
    "platform_machine": "x86_64",
    "platform_python_implementation": "CPython",
    "implementation_name": "cpython",
}
PUBLISH_ENVIRONMENTS = [
    {**_BASE_ENV, "python_version": v, "python_full_version": f"{v}.0"}
    for v in ("3.12", "3.13", "3.14")
]

Verdict = Literal["permissive", "weak-copyleft", "strong-copyleft", "unknown"]

# Normalised license tokens that need no further thought. Keyed on the SPDX id
# where a package declares one, plus the classifier spellings of the same
# licenses for packages still using the old `Classifier: License ::` form.
PERMISSIVE = {
    "0BSD",
    "APACHE-2.0",
    "BSD",
    "BSD-2-CLAUSE",
    "BSD-3-CLAUSE",
    "HPND",
    "ISC",
    "MIT",
    "MIT-0",
    "MIT-CMU",
    "PSF-2.0",
    "PYTHON-2.0",
    "UNLICENSE",
    "ZLIB",
}

# Spellings seen in the wild -> the token above.
LICENSE_ALIASES = {
    "APACHE SOFTWARE LICENSE": "APACHE-2.0",
    "APACHE LICENSE 2.0": "APACHE-2.0",
    "BSD LICENSE": "BSD",
    "BSD-2-CLAUSE LICENSE": "BSD-2-CLAUSE",
    "BSD-3-CLAUSE LICENSE": "BSD-3-CLAUSE",
    "ISC LICENSE (ISCL)": "ISC",
    "ISC LICENSE": "ISC",
    "MIT LICENSE": "MIT",
    "MIT NO ATTRIBUTION": "MIT-0",
    "PYTHON SOFTWARE FOUNDATION LICENSE": "PSF-2.0",
    "MOZILLA PUBLIC LICENSE 1.1 (MPL 1.1)": "MPL-1.1",
    "MOZILLA PUBLIC LICENSE 2.0 (MPL 2.0)": "MPL-2.0",
    "GNU GENERAL PUBLIC LICENSE V2 OR LATER (GPLV2+)": "GPL-2.0-OR-LATER",
    "GNU LESSER GENERAL PUBLIC LICENSE V2 OR LATER (LGPLV2+)": "LGPL-2.1-OR-LATER",
}

# pyphen offers a choice of three licenses (see _pyphen_note), and the image ships
# it, so one of them has to be chosen. RALFORION elects the LGPL: pyphen is used
# unmodified as an ordinary installed package that the runner imports, which is
# the case the LGPL is written for, and it is the election a licence audit expects
# to see. The GPL option is never available to us — it would reach the runner's
# own code. Set back to None only if the image stops installing the `pdf` extra.
PYPHEN_ELECTION: str | None = "LGPL-2.1-or-later"


def _pyphen_note() -> str:
    """pyphen's entry, which depends on whether we actually redistribute it."""
    shape = (
        "Tri-licensed GPL-2.0+/LGPL-2.1+/MPL-1.1, reached only through the `pdf` extra "
        "(WeasyPrint's hyphenation). "
    )
    if PYPHEN_ELECTION is None:
        return shape + (
            "**No election has been made, because RALFORION does not redistribute "
            "pyphen.** The published Docker image does not install the `pdf` extra, so it "
            "contains no copy of it; someone running `pip install orionbelt-runner[pdf]` "
            "receives pyphen from PyPI directly rather than from us, which makes the "
            "choice among its three licenses theirs to make, not ours. It is listed here "
            "because it is part of the closure the runner can use, not because it is "
            "shipped. Should a future artifact bundle it — a PDF-capable image, a "
            "vendored deployment — an election becomes necessary at that point, and "
            "`scripts/third_party_notices.py` fails the build if the Dockerfile starts "
            "installing the `pdf` extra while no election is recorded. Its bundled "
            "LibreOffice hyphenation dictionaries carry their own GPL/LGPL/MPL terms."
        )
    return shape + (
        f"The Docker image installs that extra, so it hands over a copy of pyphen and "
        f"the choice among its three licenses becomes ours to make: **RALFORION elects "
        f"{PYPHEN_ELECTION}** and does not accept the GPL option. Pyphen is imported "
        f"unmodified as an ordinary installed package — replaceable in place under "
        f"`site-packages`, never vendored into `orionbelt_runner` or statically linked "
        f"— which is the arrangement the LGPL permits, so the runner remains under its "
        f"own license. Do not patch pyphen in the image: modifications to it would be "
        f"LGPL and would have to be published. Its source for the version shipped is "
        f"the sdist on PyPI. The LibreOffice hyphenation dictionaries bundled inside it "
        f"travel with it and carry their own GPL/LGPL/MPL terms."
    )


@dataclass(frozen=True)
class Acknowledgement:
    """A non-permissive license we looked at and accepted, and why.

    `license_expression` is what was reviewed, verbatim. The exemption is bound to
    it rather than to the package name: a package that relicenses — certifi moving
    off MPL-2.0, say — must come back through the gate instead of inheriting an
    approval granted to different terms.
    """

    license_expression: str
    note: str


ACKNOWLEDGED: dict[str, Acknowledgement] = {
    "certifi": Acknowledgement(
        "Mozilla Public License 2.0 (MPL 2.0)",
        "MPL-2.0 is file-level copyleft: it reaches the files themselves, not the "
        "program that imports them. We ship certifi unmodified as a separate "
        "installed package, so the obligation is satisfied by shipping this notice "
        "and its license text. Do not patch certifi in place — patch it and the "
        "modified files must be published under MPL-2.0.",
    ),
    "pyphen": Acknowledgement(
        "GNU General Public License v2 or later (GPLv2+) OR GNU Lesser General Public "
        "License v2 or later (LGPLv2+) OR Mozilla Public License 1.1 (MPL 1.1)",
        _pyphen_note(),
    ),
}


# Attribution for what the image carries below the Python layer. uv.lock cannot
# see any of it, so this part is prose — kept here rather than in the Markdown so
# the whole notice file stays generated, and parameterised on the Dockerfile so a
# base-image bump cannot leave a stale interpreter version behind.
PLATFORM_SECTION = r"""## Platform layer (Docker image)

`uv.lock` knows only PyPI packages, so the list above stops at the Python layer.
The published image is built `FROM {{IMAGE}}` and therefore also
redistributes a CPython interpreter and a Debian userland. Neither is vendored, patched, or
rebuilt — the base image is used exactly as published.

**CPython** is under the Python Software Foundation License Agreement, Version 2:
permissive, no copyleft, nothing to elect. Its full text — including the
historical BeOpen, CNRI and CWI terms that Python inherits — ships inside the
image at `/usr/local/lib/python{{PYVER}}/LICENSE.txt`. PSF-2.0 §3 would require a
summary of changes made to Python; the runner makes none.

**The Debian userland** is a mix of permissive, LGPL and GPL packages. Debian
ships every package's terms at `/usr/share/doc/<package>/copyright`, so that
attribution travels inside the image alongside the binaries it covers. Two points
on the copyleft there:

- Nothing GPL is *linked*. The libraries bound at runtime — glibc, libffi,
  libssl, libsqlite3, liblzma, readline, and Pango, which WeasyPrint draws
  through — are LGPL or permissive, and all are
  linked dynamically, which is the case the LGPL permits. (A Debian `copyright`
  file frequently mentions the GPL because a build script or a sibling binary in
  the same source package is GPL'd; glibc is the standard example, an LGPL
  library shipped next to GPL'd tools like `ldd`.)
- The genuinely GPL'd components are the OS utilities — `bash`, `coreutils`,
  `dpkg`, `apt`, `tar`, `sed`, `util-linux` and friends. They are *programs*
  sitting beside the runner in the same filesystem, not code linked into or
  imported by it: mere aggregation under GPL-2 §2. They do not reach
  `orionbelt_runner`, which stays under its own license.

**Corresponding source.** Debian keeps a permanent, timestamped archive of every
package version it has ever published at <https://snapshot.debian.org>. The base
image is pinned when the image is built, so the exact source that produced every
OS binary in a given image stays retrievable there. To enumerate what an image
contains and pull the matching sources:

```bash
docker run --rm <image> dpkg-query -W -f='${source:Package} ${source:Version}\n'
# then `apt-get source <package>=<version>` against the snapshot.debian.org
# suite for that image's build date
```
"""


@dataclass(frozen=True)
class Package:
    name: str
    version: str
    license_expression: str
    verdict: Verdict
    texts: tuple[tuple[str, str], ...]  # (filename, contents)
    in_image: bool  # redistributed by the Docker image, vs reachable via an extra


def runtime_base_image() -> tuple[str, str]:
    """The Dockerfile's runtime base image, as (image, python minor).

    Read rather than hardcoded: the platform notice names the interpreter version
    and the path its license sits at, and a base-image bump must not be able to
    leave either of those saying something untrue.
    """
    match = re.search(
        r"^FROM\s+(python:(\d+\.\d+)[^\s]*)\s+AS\s+runtime\s*$",
        DOCKERFILE_PATH.read_text(encoding="utf-8"),
        re.IGNORECASE | re.MULTILINE,
    )
    if match is None:
        raise SystemExit(
            "could not find a `FROM python:<version> AS runtime` line in the Dockerfile — "
            "the platform-layer notice is derived from it"
        )
    return match.group(1), match.group(2)


def normalize(name: str) -> str:
    """PEP 503 name normalisation — `ruamel.yaml` and `ruamel-yaml` are one package."""
    return re.sub(r"[-_.]+", "-", name).lower()


def marker_applies(marker: str | None) -> bool:
    if marker is None:
        return True
    parsed = Marker(marker)
    return any(parsed.evaluate(environment=env) for env in PUBLISH_ENVIRONMENTS)


def distributed_closure(lock: dict[str, Any], extras: Sequence[str]) -> dict[str, str]:
    """Walk uv.lock from the root package to every dependency we redistribute.

    Returns {normalised name: version}. Extras are followed as their own edges so
    `fonttools[woff]` pulls brotli while a plain `fonttools` would not.
    """
    packages = {normalize(p["name"]): p for p in lock["package"]}
    found: dict[str, str] = {}
    seen: set[tuple[str, tuple[str, ...]]] = set()

    def walk(name: str, extras: tuple[str, ...]) -> None:
        key = (normalize(name), tuple(sorted(extras)))
        if key in seen:
            return
        seen.add(key)
        package = packages.get(normalize(name))
        if package is None:
            raise SystemExit(f"{name} is required but missing from uv.lock")
        found[normalize(name)] = package["version"]
        edges = list(package.get("dependencies", []))
        for extra in extras:
            edges += package.get("optional-dependencies", {}).get(extra, [])
        for edge in edges:
            if marker_applies(edge.get("marker")):
                walk(edge["name"], tuple(edge.get("extra", ())))

    walk(ROOT_PACKAGE, tuple(extras))
    del found[normalize(ROOT_PACKAGE)]  # our own license is LICENSE, not a notice
    return found


# GPL but not LGPL: the lookbehind is what keeps "LGPL-2.1" out of the strong
# bucket, and it has to survive both "GPL-3.0-only" and a classifier's "(GPLv2+)".
_STRONG_COPYLEFT = re.compile(r"(?<![A-Z])A?GPL")
_WEAK_COPYLEFT = re.compile(r"(?<![A-Z])(LGPL|MPL|EPL|CDDL|CPL|OSL|EUPL)")


def image_closure_extras(lock: dict[str, Any]) -> list[str]:
    """Extras the root package actually defines, minus the ones we never ship."""
    for package in lock["package"]:
        if normalize(package["name"]) == normalize(ROOT_PACKAGE):
            return sorted(package.get("optional-dependencies", {}))
    raise SystemExit(f"{ROOT_PACKAGE} is missing from uv.lock")


def classify(expression: str) -> Verdict:
    """Classify a declared license expression conservatively.

    Anything not recognisably permissive is escalated rather than guessed at — an
    expression this function cannot parse should stop a release, not pass one.
    Note the escalation order: a tri-licensed package offering GPL *or* something
    weaker reads as strong here, because the weaker option only applies once
    somebody has elected it in writing (see ACKNOWLEDGED).
    """
    text = expression.upper()
    # Longest phrase first, so "GNU LESSER GENERAL PUBLIC LICENSE V2 OR LATER"
    # is consumed before any shorter alias can bite into it.
    for phrase, token in sorted(LICENSE_ALIASES.items(), key=lambda kv: -len(kv[0])):
        text = text.replace(phrase, token)
    # Split on the SPDX operators. This also splits an un-aliased classifier's
    # "v2 or later" into fragments, which is harmless: the regexes below match on
    # the fragment, and anything they miss lands in "unknown" and fails the gate.
    tokens = [t.strip() for t in re.split(r"\bOR\b|\bAND\b|[,;/]", text) if t.strip()]
    if not tokens:
        return "unknown"
    if any(_STRONG_COPYLEFT.search(t) for t in tokens):
        return "strong-copyleft"
    if any(_WEAK_COPYLEFT.search(t) for t in tokens):
        return "weak-copyleft"
    if all(t in PERMISSIVE for t in tokens):
        return "permissive"
    return "unknown"


def read_override(name: str) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    """Load a hand-vendored license for a wheel that ships none.

    Format: a first line `SPDX: <expression>`, then the license text. Used where
    upstream simply forgot to include LICENSE in the wheel (webencodings), so the
    text has to come from that project's own repository instead.
    """
    path = OVERRIDE_DIR / f"{name}.txt"
    if not path.exists():
        return None
    head, _, body = path.read_text(encoding="utf-8").partition("\n")
    if not head.startswith("SPDX:"):
        raise SystemExit(f"{path} must start with a 'SPDX: <expression>' line")
    return head.removeprefix("SPDX:").strip(), ((f"{path.name} (vendored)", body.strip()),)


def declared_license(metadata: importlib_metadata.PackageMetadata) -> str:
    """The package's own license claim, newest metadata form first."""
    expression = metadata.get("License-Expression")
    if expression:
        return str(expression)
    classifiers = [
        c.split("::")[-1].strip()
        for c in metadata.get_all("Classifier", [])
        if str(c).startswith("License ::")
    ]
    if classifiers:
        return " OR ".join(classifiers)
    declared = metadata.get("License")
    # Some projects paste the whole license text into `License:`; keep the first
    # line, which is invariably the name.
    return str(declared).strip().splitlines()[0] if declared else ""


def license_texts(dist: importlib_metadata.Distribution) -> tuple[tuple[str, str], ...]:
    """Every LICENSE / NOTICE / COPYING file the wheel installed, in path order.

    NOTICE is deliberately included: Apache-2.0 section 4(d) makes propagating it
    mandatory, and pyarrow ships one.
    """
    wanted = re.compile(r"(LICEN[CS]E|COPYING|NOTICE|AUTHORS)", re.IGNORECASE)
    out = []
    for entry in sorted(dist.files or [], key=str):
        text = str(entry)
        if ".dist-info" not in text or not wanted.search(Path(text).name):
            continue
        # Read through locate_file: Distribution.read_text() resolves relative to
        # the .dist-info directory, while entries in RECORD are relative to
        # site-packages, so it would silently miss every one of these.
        try:
            body = Path(str(dist.locate_file(entry))).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        out.append((Path(text).name, body))
    return tuple((name, body.strip()) for name, body in out if body.strip())


def collect(lock: dict[str, Any], image_extras: frozenset[str]) -> list[Package]:
    packages = []
    problems = []
    defined = set(image_closure_extras(lock))
    if not image_extras <= defined:
        raise SystemExit(
            f"error: the Dockerfile installs undefined extra(s) "
            f"{sorted(image_extras - defined)} — pyproject defines {sorted(defined)}. "
            f"A typo here would silently shrink what this file claims the image ships."
        )
    if not image_extras <= set(NOTICED_EXTRAS):
        raise SystemExit(
            f"error: the Dockerfile installs {sorted(image_extras - set(NOTICED_EXTRAS))}, "
            f"which this notice does not account for. Add it to NOTICED_EXTRAS so the "
            f"packages it pulls in are credited — they are being redistributed."
        )
    in_image = distributed_closure(lock, sorted(image_extras))
    for name, version in sorted(distributed_closure(lock, NOTICED_EXTRAS).items()):
        override = read_override(name)
        if override is not None:
            expression, texts = override
        else:
            try:
                dist = importlib_metadata.distribution(name)
            except importlib_metadata.PackageNotFoundError:
                problems.append(
                    f"{name} is in the distributed closure but not installed — "
                    f"run `uv sync --locked --extra dev` first"
                )
                continue
            if dist.version != version:
                problems.append(f"{name}: uv.lock pins {version} but {dist.version} is installed")
            expression = declared_license(dist.metadata)
            texts = license_texts(dist)
            if not texts:
                problems.append(
                    f"{name} {version} ships no license file — add "
                    f"scripts/license-overrides/{name}.txt with its text"
                )
                continue
        packages.append(
            Package(name, version, expression, classify(expression), texts, name in in_image)
        )
    if problems:
        raise SystemExit("\n".join(f"error: {p}" for p in problems))
    return packages


def render(packages: list[Package], image_extras: frozenset[str]) -> str:
    # Packages whose text had to be vendored ship none of their own, so inside the
    # image this file is the only place their license exists. Saying so keeps the
    # sentence above honest without hardcoding which package it is.
    vendored = sorted(
        package.name for package in packages if (OVERRIDE_DIR / f"{package.name}.txt").exists()
    )
    vendored_note = (
        [
            f"One exception: {', '.join(f'`{name}`' for name in vendored)} ships no",
            "license file of its own — upstream omits it from the wheel — so inside the",
            "image this file is the only copy of its terms. Its text is reproduced below",
            "from the project's own repository.",
            "",
        ]
        if vendored
        else []
    )

    quoted = [f"`--extra {extra}`" for extra in sorted(image_extras)]
    # "a and b", "a, b and c" — a bare comma join reads as a truncated list.
    extras_phrase = (
        " and ".join(filter(None, [", ".join(quoted[:-1])] + quoted[-1:])) or "no extras"
    )
    in_image = sum(1 for package in packages if package.in_image)
    everything = in_image == len(packages)
    image_scope = "every package below" if everything else "the packages marked *yes* below"
    # Only describe an informational set when one exists — with every extra
    # installed there is nothing outside the image, and the paragraph would be
    # describing an empty set.
    outside_image: list[str] = (
        []
        if everything
        else [
            "The remaining packages are reachable only through an extra the image does",
            "not install. They are credited here because `pip install orionbelt-runner[pdf]`",
            "can pull them in and because a future artifact may bundle them — but no",
            "artifact we publish redistributes them today.",
            "",
        ]
    )
    lines = [
        "# Third-party notices",
        "",
        "<!-- Generated by scripts/third_party_notices.py — do not edit by hand. -->",
        "",
        "OrionBelt Runner itself is licensed under the",
        "[Business Source License 1.1](LICENSE). This file covers the third-party",
        "packages it depends on and reproduces the attribution each one requires.",
        "",
        "## Scope",
        "",
        "The **wheel** on PyPI contains only `orionbelt_runner`. The **sdist** also",
        "carries this repository's own sources — tests, examples, the vendored OBML",
        "schema, CI config. Neither contains a single third-party package: pip resolves",
        "and downloads everything below from PyPI itself, so both artifacts redistribute",
        "nothing and this file is informational for them.",
        "",
        f"The **Docker image redistributes** {image_scope}. It copies a populated",
        f"virtualenv built with {extras_phrase}, handing those packages over as",
        "binaries, which is what makes their notices mandatory rather than courteous.",
        "Their license texts are present inside the image too, at",
        "`/app/.venv/lib/python*/site-packages/*.dist-info/licenses/`, alongside",
        "`/app/LICENSE` and `/app/THIRD-PARTY-NOTICES.md`.",
        "",
        *vendored_note,
        *outside_image,
        "The list is the dependency closure of `orionbelt-runner` plus the `arrow` and",
        "`pdf` extras, resolved from `uv.lock` for linux/CPython on Python 3.12–3.14.",
        "Development-only dependencies (pytest, ruff, mypy, respx) are excluded: they",
        "are tools the project runs, not code it ships. Windows- and PyPy-only",
        "resolutions (colorama, brotlicffi) are excluded because no artifact can",
        "reach them: the image is linux/CPython, and so is every environment the",
        "wheel supports being installed into here.",
        "",
        "`uv.lock` sees only PyPI packages, so the interpreter and the operating",
        "system the Docker image is built on are covered separately under",
        "[Platform layer](#platform-layer-docker-image) below.",
        "",
        "Regenerate with:",
        "",
        "```bash",
        "uv sync --locked --extra dev",
        "uv run --no-sync python scripts/third_party_notices.py",
        "```",
        "",
        "## Summary",
        "",
        (
            f"{len(packages)} packages, all of them redistributed by the published image."
            if everything
            else f"{len(packages)} packages: {in_image} in the published image, "
            f"{len(packages) - in_image} reachable only through an extra it does not install."
        ),
        "",
        "| Package | Version | License | In image |",
        "| --- | --- | --- | --- |",
    ]
    for package in packages:
        flag = "" if package.verdict == "permissive" else " ⚠️"
        shipped = "yes" if package.in_image else "no"
        lines.append(
            f"| {package.name} | {package.version} "
            f"| {package.license_expression}{flag} | {shipped} |"
        )

    flagged = [p for p in packages if p.name in ACKNOWLEDGED]
    if flagged:
        lines += ["", "## Conditions worth knowing (⚠️ above)", ""]
        for package in flagged:
            lines += [
                f"### {package.name} — {package.license_expression}",
                "",
                ACKNOWLEDGED[package.name].note,
                "",
            ]
    else:
        lines.append("")

    image, python_version = runtime_base_image()
    lines += [
        PLATFORM_SECTION.replace("{{IMAGE}}", image).replace("{{PYVER}}", python_version),
    ]

    lines += ["## Full license texts", ""]
    for package in packages:
        lines += [f"### {package.name} {package.version}", ""]
        for filename, body in package.texts:
            lines += [f"*{filename}*", "", "```text", body, "```", ""]
    return "\n".join(lines).rstrip() + "\n"


def dockerfile_extras() -> frozenset[str]:
    """The extras the Dockerfile installs — that is, what the image really contains.

    This is the seam that decides which packages a published artifact redistributes
    and whether pyphen needs an election, so it parses rather than pattern-matches:
    comment lines are dropped, line continuations joined, and `--extra=pdf` is
    accepted alongside `--extra pdf`. A guard defeated by reformatting a RUN line
    would be worse than no guard, because it would still look like one.
    """
    text = DOCKERFILE_PATH.read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    text = re.sub(r"\\\s*\n\s*", " ", text)
    extras: set[str] = set()
    for command in re.findall(r"uv sync[^\n]*", text):
        extras.update(re.findall(r"--extra[=\s]+([A-Za-z0-9_.-]+)", command))
    return frozenset(extras)


def enforce_pyphen_election(ships_pdf: bool, election: str | None) -> list[str]:
    """Force the election at the moment it starts to matter, and not before.

    Writing an election down while nothing we publish contains pyphen would be
    committing the company to terms it has no need to accept. Shipping pyphen
    without one would be the opposite mistake: distributing a GPL-optional package
    with no record of which option was taken.
    """
    if ships_pdf and election is None:
        return [
            "the Dockerfile now installs the `pdf` extra, so a published artifact "
            "redistributes pyphen (GPL-2.0+ OR LGPL-2.1+ OR MPL-1.1). Distributing it "
            "means choosing one of those, and the choice has to be recorded: set "
            "PYPHEN_ELECTION in " + Path(__file__).name + ' to "LGPL-2.1-or-later" '
            '(the usual choice for an unmodified imported library) or "MPL-1.1". Never '
            "the GPL option — it conflicts with the runner's own license."
        ]
    return []


def enforce_policy(packages: list[Package]) -> list[str]:
    failures = []
    for package in packages:
        if package.verdict == "permissive":
            continue
        acknowledgement = ACKNOWLEDGED.get(package.name)
        if acknowledgement is None:
            failures.append(
                f"{package.name} {package.version} is {package.verdict} "
                f"({package.license_expression or 'no license declared'}). "
                f"Distributing it is a decision, not a lockfile bump: either drop the "
                f"dependency or add it to ACKNOWLEDGED in {Path(__file__).name} with the "
                f"reason it is acceptable."
            )
        elif acknowledgement.license_expression != package.license_expression:
            failures.append(
                f"{package.name} {package.version} is acknowledged under "
                f"{acknowledgement.license_expression!r} but now declares "
                f"{package.license_expression!r}. The exemption covers the terms that "
                f"were reviewed, not the package name: re-read the new license and "
                f"update its ACKNOWLEDGED entry, or drop the dependency."
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the license policy and that the notices file is current",
    )
    args = parser.parse_args()

    image_extras = dockerfile_extras()
    packages = collect(tomllib.loads(LOCK_PATH.read_text(encoding="utf-8")), image_extras)
    rendered = render(packages, image_extras)

    failures = enforce_policy(packages)
    failures += enforce_pyphen_election("pdf" in image_extras, PYPHEN_ELECTION)
    if failures:
        for failure in failures:
            print(f"error: {failure}", file=sys.stderr)
        return 1

    if not args.check:
        NOTICES_PATH.write_text(rendered, encoding="utf-8")
        print(f"wrote {NOTICES_PATH.relative_to(ROOT)} ({len(packages)} packages)")
        return 0

    current = NOTICES_PATH.read_text(encoding="utf-8") if NOTICES_PATH.exists() else ""
    if current != rendered:
        print(
            "error: THIRD-PARTY-NOTICES.md is out of date — regenerate it with\n"
            "  uv run --no-sync python scripts/third_party_notices.py",
            file=sys.stderr,
        )
        diff = unified_diff(
            current.splitlines(), rendered.splitlines(), "on disk", "expected", lineterm=""
        )
        print("\n".join(list(diff)[:40]), file=sys.stderr)
        return 1
    print(f"THIRD-PARTY-NOTICES.md is current ({len(packages)} packages, policy ok)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
