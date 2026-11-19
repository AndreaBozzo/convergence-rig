"""Unit checks for Milestone R1 standalone reproduction package."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess

REPO_ROOT = Path(__file__).resolve().parent.parent
REPRO_DIR = REPO_ROOT / "repro" / "pg2iceberg-catalog-outage"
LAB_ENV_PATH = REPO_ROOT / "compose" / ".env"
DRAFT_PATH = REPO_ROOT / "drafts" / "pg2iceberg-catalog-outage-exit.md"


def _parse_lab_ports() -> set[int]:
    ports: set[int] = set()
    for line in LAB_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        if key.endswith("_PORT") and val.isdigit():
            ports.add(int(val))
    return ports


def _extract_compose_port_mappings(compose_text: str) -> list[str]:
    mappings: list[str] = []
    in_ports = False
    ports_indent = 0
    for line in compose_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if stripped == "ports:":
            in_ports = True
            ports_indent = indent
            continue
        if in_ports:
            if indent <= ports_indent:
                in_ports = False
            elif stripped.startswith("- "):
                val = stripped[2:].strip().strip('"').strip("'")
                mappings.append(val)
    return mappings


def test_r1_required_files_exist_with_lf_endings() -> None:
    required = ("docker-compose.yml", "pg2iceberg.yaml", "repro.sh", "README.md")
    for name in required:
        path = REPRO_DIR / name
        assert path.is_file(), f"Missing required R1 file: {path}"
        raw = path.read_bytes()
        assert b"\r\n" not in raw, f"{name} must use LF line endings for POSIX portability"


def test_r1_compose_pinned_digests_project_and_disjoint_localhost_ports() -> None:
    text = (REPRO_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    assert re.search(r"^name:\s*p2irepro\s*$", text, re.MULTILINE), "Compose project name must be p2irepro"
    assert "cdclab" not in text, "Standalone compose file must not reference cdclab"

    images = re.findall(r"^\s*image:\s*(\S+)\s*$", text, re.MULTILINE)
    assert len(images) >= 5, "Expected at least 5 service image entries"
    for img in images:
        assert "@sha256:" in img, f"Image must be pinned by sha256 digest: {img}"
        digest = img.split("@sha256:", 1)[1]
        assert re.fullmatch(r"[0-9a-f]{64}", digest), f"Invalid sha256 digest in {img}"

    lab_ports = _parse_lab_ports()
    port_lines = _extract_compose_port_mappings(text)
    assert len(port_lines) == 3, f"Expected 3 published ports (pg, silo, lakekeeper), got {port_lines}"
    for mapping in port_lines:
        assert mapping.startswith("127.0.0.1:"), f"Port mapping must bind 127.0.0.1: {mapping}"
        m = re.search(r":-(\d+)\}", mapping)
        assert m, f"Expected configurable default port in {mapping}"
        default_port = int(m.group(1))
        assert default_port not in lab_ports, (
            f"Port {default_port} collides with cdclab compose/.env ports {sorted(lab_ports)}"
        )


def test_r1_repro_script_is_posix_self_contained() -> None:
    script = REPRO_DIR / "repro.sh"
    text = script.read_text(encoding="utf-8")
    assert text.startswith("#!/bin/sh\n"), "repro.sh must start with POSIX #!/bin/sh shebang"
    assert 'PROJECT="p2irepro"' in text
    assert "RESULT: pg2iceberg exited with code" in text
    assert "run_split()" in text

    # Self-contained: no Python, no lab modules, no parent-directory traversal.
    for forbidden in ("python", "pytest", "uv ", "cdclab", "../", "lab/"):
        assert forbidden not in text.lower(), f"repro.sh must not depend on {forbidden!r}"

    # No common bashisms in a POSIX sh script.
    for bashism in ("[[ ", "pipefail"):
        assert bashism not in text, f"Non-POSIX construct {bashism!r} found in repro.sh"
    assert not re.search(r"^\s*(?:local|declare|function)\s+", text, re.MULTILINE), (
        "Non-POSIX shell keyword (local/declare/function) found in repro.sh"
    )

    # Run `sh -n` when sh can initialize in the current process environment.
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


def test_r1_pg2iceberg_config_and_draft_updated() -> None:
    cfg_text = (REPRO_DIR / "pg2iceberg.yaml").read_text(encoding="utf-8")
    assert "cdclab" not in cfg_text
    assert "public.items" in cfg_text

    draft_text = DRAFT_PATH.read_text(encoding="utf-8")
    assert "repro/pg2iceberg-catalog-outage/" in draft_text
    assert "RESULT: pg2iceberg exited with code 1 while the catalog was down; slot active=false" in draft_text
    assert "RESULT: split mode survived catalog outage" in draft_text
