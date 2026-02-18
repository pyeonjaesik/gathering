"""
SQLite DB 백업/복원 유틸.
"""

from __future__ import annotations

import shutil
import os
from datetime import datetime
from pathlib import Path


def _default_backup_dir(db_path: str) -> Path:
    db_file = Path(db_path).resolve()
    return db_file.parent / "backups"


def _mirror_backup(local_backup_path: Path) -> None:
    """환경변수 GOOGLE_DRIVE_BACKUP_DIR가 설정되면 Google Drive 폴더로 복사."""
    drive_root = os.getenv("GOOGLE_DRIVE_BACKUP_DIR", "").strip()
    if not drive_root:
        return
    drive_dir = Path(drive_root).expanduser().resolve()
    drive_dir.mkdir(parents=True, exist_ok=True)
    drive_path = drive_dir / local_backup_path.name
    shutil.copy2(local_backup_path, drive_path)


def create_backup(db_path: str, label: str = "manual", backup_dir: str | None = None) -> str:
    src = Path(db_path).resolve()
    if not src.exists():
        raise FileNotFoundError(f"DB 파일이 없습니다: {src}")

    out_dir = Path(backup_dir).resolve() if backup_dir else _default_backup_dir(db_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = out_dir / f"{src.stem}_{label}_{ts}{src.suffix}"
    shutil.copy2(src, dst)
    _mirror_backup(dst)
    return str(dst)


def list_backups(db_path: str, backup_dir: str | None = None) -> list[str]:
    src = Path(db_path).resolve()
    out_dir = Path(backup_dir).resolve() if backup_dir else _default_backup_dir(db_path)
    if not out_dir.exists():
        return []
    pattern = f"{src.stem}_*{src.suffix}"
    files = sorted(out_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return [str(p) for p in files]


def restore_backup(
    backup_path: str,
    db_path: str,
    keep_current_snapshot: bool = True,
) -> str:
    src_backup = Path(backup_path).resolve()
    if not src_backup.exists():
        raise FileNotFoundError(f"백업 파일이 없습니다: {src_backup}")

    dst_db = Path(db_path).resolve()
    if keep_current_snapshot and dst_db.exists():
        create_backup(str(dst_db), label="pre_restore")

    shutil.copy2(src_backup, dst_db)
    return str(dst_db)
