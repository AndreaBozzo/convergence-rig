"""Structural checks for Milestone R2: the catalog-restore reproduction package."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
REPRO_DIR = REPO_ROOT / "repro" / "pg2iceberg-catalog-restore"
LAB_ENV_PATH = REPO_ROOT / "compose" / ".env"
R1_COMPOSE_PATH = REPO_ROOT / "repro" / "pg2iceberg-catalog-outage" / "docker-compose.yml"
DRAFT_PATH = REPO_ROOT / "drafts" / "pg2iceberg-catalog-rollback-silent-skip.md"

RESULT_PREFIX = "RESULT: gap in the applied log range after the catalog restore:"


def _default_ports(compose_text: str) -> set[int]:
    return {int(p) for p in re.findall(r"\$\{P2I_[A-Z_]+:-(\d+)\}", compose_text)}


def _lab_ports() -> set[int]:
    ports: set[int] = set()
    for line in LAB_ENV_PATH.read_text(encoding="utf-8").splitlines():
        key, sep, val = line.strip().partition("=")
        if sep and key.endswith("_PORT") and val.isdigit():
            ports.add(int(val))
    return ports


def test_r2_required_files_exist_with_lf_endings() -> None:
    for name in ("docker-compose.yml", "pg2iceberg.yaml", "repro.sh", "README.md"):
        path = REPRO_DIR / name
        assert path.is_file(), f"Missing required R2 file: {path}"
        assert b"\r\n" not in path.read_bytes(), f"{name} must use LF line endings"


def test_r2_compose_project_pins_and_disjoint_localhost_ports() -> None:
    text = (REPRO_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    assert re.search(r"^name:\s*p2irestore\s*$", text, re.MULTILINE)
    assert "cdclab" not in text and "p2irepro" not in text

    images = re.findall(r"^\s*image:\s*(.+?)\s*$", text, re.MULTILINE)
    fixed = [i for i in images if not i.startswith("${")]
    assert len(fixed) >= 4
    for img in fixed:
        assert re.search(r"@sha256:[0-9a-f]{64}$", img), f"Image must be pinned by digest: {img}"
    # The pg2iceberg image is chosen through one variable, set by repro.sh.
    assert any(i.startswith("${P2I_PG2ICEBERG_IMAGE") for i in images)

    mappings = re.findall(r'^\s*-\s*"(127\.0\.0\.1:[^"]+)"\s*$', text, re.MULTILINE)
    assert len(mappings) == 3, f"Expected 3 published ports on 127.0.0.1, got {mappings}"
    assert not re.search(r'^\s*-\s*"(?!127\.0\.0\.1:)\$?\{?\d+:\d+"', text, re.MULTILINE)

    defaults = _default_ports(text)
    assert len(defaults) == 3
    assert not defaults & _lab_ports(), "R2 ports collide with the lab's compose/.env"
    assert not defaults & _default_ports(R1_COMPOSE_PATH.read_text(encoding="utf-8")), (
        "R2 ports collide with R1's"
    )


def test_r2_infrastructure_pins_match_r1() -> None:
    """Only the project name, ports and the pg2iceberg image differ from R1."""
    pin = re.compile(r"image:\s*((?!\$\{)\S+@sha256:[0-9a-f]{64})")
    r1 = set(pin.findall(R1_COMPOSE_PATH.read_text(encoding="utf-8")))
    r2 = set(pin.findall((REPRO_DIR / "docker-compose.yml").read_text(encoding="utf-8")))
    infra_r1 = {i for i in r1 if "pg2iceberg" not in i}
    assert infra_r1 <= r2, f"R2 changed an infrastructure pin: {infra_r1 - r2}"
    assert not {i for i in r2 if "pg2iceberg" in i}, "pg2iceberg images belong in repro.sh"


def test_r2_repro_script_is_posix_self_contained() -> None:
    script = REPRO_DIR / "repro.sh"
    text = script.read_text(encoding="utf-8")
    assert text.startswith("#!/bin/sh\n")
    assert 'PROJECT="p2irestore"' in text
    assert RESULT_PREFIX in text
    assert "exit 2" in text and "invalid()" in text

    for forbidden in ("python", "pytest", "uv ", "cdclab", "../", "lab/", "docker system", "prune"):
        assert forbidden not in text.lower(), f"repro.sh must not use {forbidden!r}"
    for bashism in ("[[ ", "pipefail"):
        assert bashism not in text
    # (The awk helper inside repro.sh declares `function name(args)`; a bash
    # function keyword has no argument list.)
    assert not re.search(r"^[ 	]*(?:local|declare)[ 	]+", text, re.MULTILINE)
    assert not re.search(r"^[ 	]*function[ 	]+\w+[ 	]*(?:\(\)[ 	]*)?\{", text, re.MULTILINE)

    # Both pg2iceberg images are pinned by digest and chosen by one variable.
    refs = re.findall(r'^IMAGE_[A-Z0-9_]+="(polynyadev/pg2iceberg:[^"]+)"', text, re.MULTILINE)
    assert len(refs) == 2
    assert any(":0.12.0@sha256:" in r for r in refs)
    assert any(":sha-4344888@sha256:" in r for r in refs)
    for ref in refs:
        assert re.search(r"@sha256:[0-9a-f]{64}$", ref)
    assert "P2I_VERSION" in text and "P2I_PG2ICEBERG_IMAGE" in text

    sh_bin = shutil.which("sh") or (
        r"C:\Program Files\Git\usr\bin\sh.exe"
        if Path(r"C:\Program Files\Git\usr\bin\sh.exe").is_file()
        else None
    )
    if sh_bin is not None:
        probe = subprocess.run([sh_bin, "-c", "true"], capture_output=True, text=True, check=False)
        if probe.returncode == 0:
            proc = subprocess.run([sh_bin, "-n", str(script)], capture_output=True, text=True, check=False)
            assert proc.returncode == 0, f"sh -n failed: {proc.stderr}"


def _awk_gap_detector() -> str:
    """The gap detector awk program of repro.sh, as one string."""
    text = (REPRO_DIR / "repro.sh").read_text(encoding="utf-8")
    start = text.index("GAPS=$(printf '%s\\n' \"$ROWS\" | awk '") + len("GAPS=$(printf '%s\\n' \"$ROWS\" | awk '")
    end = text.index("')\n", start)
    return text[start:end]


def test_r2_gap_detector_fails_on_a_gap_and_passes_on_a_continuous_range() -> None:
    """The detector must flag the K2 shape and stay quiet on an unforked table."""
    git_awk = Path(r"C:\Program Files\Git\usr\bin\awk.exe")
    awk = shutil.which("awk") or (str(git_awk) if git_awk.is_file() else None)
    if awk is None:
        pytest.skip("no awk available")
    program = _awk_gap_detector()

    def run(rows: str) -> str:
        proc = subprocess.run([awk, program], input=rows, capture_output=True, text=True, check=False)
        assert proc.returncode == 0, proc.stderr
        return proc.stdout.strip()

    # seq id parent log-start log-end
    forked = "1 a - - -\n2 b a 0 6\n3 c b 29 31\n"
    assert run(forked) == "gaps=(6,29] overlaps=none"
    continuous = "1 a - - -\n2 b a 0 6\n3 c b 6 21\n4 d c 21 29\n5 e d 29 31\n"
    assert run(continuous) == "gaps=none overlaps=none"
    reapplied = "1 a - - -\n2 b a 0 6\n3 c b 6 21\n4 d c 10 29\n"
    assert run(reapplied) == "gaps=none overlaps=(10,21]"


def test_r2_draft_points_at_the_folder_and_states_main_behaviour() -> None:
    draft = DRAFT_PATH.read_text(encoding="utf-8")
    assert "repro/pg2iceberg-catalog-restore/" in draft
    assert RESULT_PREFIX in draft
    assert "4344888" in draft
