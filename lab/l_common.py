"""Shared multi-profile Compose and digest helpers for Lakekeeper follow-up milestones (L1-L3)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
import json
from typing import Any

from lab.config import LabConfig, REPO_ROOT
from lab.pipeline import list_non_project_containers, list_project_resources, run_cmd

ALL_LAB_PROFILES: tuple[str, ...] = ("connector", "spark", "authz")


def compose_cmd_with_profiles(
    cfg: LabConfig,
    profiles: Sequence[str] = (),
) -> list[str]:
    """Build a docker compose command for project cdclab with explicit profiles."""
    cmd = [
        "docker",
        "compose",
        "-p",
        cfg.compose_project,
        "--env-file",
        str(cfg.compose_env_file.relative_to(REPO_ROOT).as_posix()),
        "-f",
        str(cfg.compose_file.relative_to(REPO_ROOT).as_posix()),
    ]
    for profile in profiles:
        cmd.extend(["--profile", profile])
    return cmd


def collect_digests_for_profiles(
    cfg: LabConfig,
    profiles: Sequence[str] = ALL_LAB_PROFILES,
) -> list[dict[str, Any]]:
    """Inspect all running containers in project cdclab across the requested profiles."""
    ids = run_cmd([*compose_cmd_with_profiles(cfg, profiles), "ps", "-q"]).stdout.splitlines()
    ids = [cid.strip() for cid in ids if cid.strip()]
    if not ids:
        return []
    raw = run_cmd(["docker", "inspect", *ids]).stdout
    inspected = json.loads(raw)
    result = []
    for item in inspected:
        img_id = item.get("Image")
        img_info = json.loads(run_cmd(["docker", "image", "inspect", img_id]).stdout)[0]
        result.append(
            {
                "service": (item.get("Config", {}).get("Labels") or {}).get(
                    "com.docker.compose.service"
                ),
                "container_name": item.get("Name", "").lstrip("/"),
                "image_ref": item.get("Config", {}).get("Image"),
                "image_id": img_id,
                "repo_digests": img_info.get("RepoDigests") or [],
                "architecture": img_info.get("Architecture"),
                "os": img_info.get("Os"),
            }
        )
    return sorted(result, key=lambda x: str(x["service"]))


def project_teardown_and_verify(
    cfg: LabConfig,
    logged_cmd: Callable[[list[str]], Any],
    non_project_before: list[str],
    *,
    profiles: Sequence[str] = ALL_LAB_PROFILES,
    keep_up: bool = False,
) -> dict[str, Any]:
    """Tear down all cdclab project resources and verify unrelated containers are untouched."""
    if keep_up:
        return {"skipped_keep_up": True}
    logged_cmd([*compose_cmd_with_profiles(cfg, profiles), "down", "-v", "--remove-orphans"])
    resources_after = list_project_resources(cfg.compose_project)
    non_project_after = list_non_project_containers(cfg.compose_project)
    return {
        "skipped_keep_up": False,
        "resources_after_cleanup": resources_after,
        "all_project_resources_removed": (
            not resources_after["containers"]
            and not resources_after["volumes"]
            and not resources_after["networks"]
        ),
        "non_project_containers_unchanged": non_project_before == non_project_after,
        "non_project_count_before": len(non_project_before),
        "non_project_count_after": len(non_project_after),
    }
