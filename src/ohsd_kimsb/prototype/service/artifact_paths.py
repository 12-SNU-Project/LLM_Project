from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PrototypeArtifactPaths:
    sqlite_db_path: Path
    chroma_directory: Path
    manifest_path: Path


def default_artifact_paths(repo_root: Path) -> PrototypeArtifactPaths:
    runtime_root = repo_root / ".runtime" / "audit_qa"
    return PrototypeArtifactPaths(
        sqlite_db_path=runtime_root / "sqlite" / "audit_reports.sqlite3",
        chroma_directory=runtime_root / "chroma" / "audit_chunks",
        manifest_path=runtime_root / "manifests" / "offline_ingest.json",
    )
