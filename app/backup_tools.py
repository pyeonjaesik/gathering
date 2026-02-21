"""
SQLite DB 백업/복원 유틸.
"""

from __future__ import annotations

import shutil
import os
import json
import hashlib
import sqlite3
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


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _collect_db_snapshot(db_path: Path) -> dict:
    snapshot: dict = {
        "db_path": str(db_path),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        conn = sqlite3.connect(str(db_path))
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        snapshot["integrity_check"] = (integrity[0] if integrity else "unknown")
        snapshot["page_count"] = int(conn.execute("PRAGMA page_count").fetchone()[0])
        snapshot["page_size"] = int(conn.execute("PRAGMA page_size").fetchone()[0])
        snapshot["user_version"] = int(conn.execute("PRAGMA user_version").fetchone()[0])

        critical_tables = [
            "processed_food_info",
            "query_pool",
            "query_runs",
            "serp_cache",
            "query_image_analysis_cache",
            "food_final",
        ]
        counts: dict[str, int | None] = {}
        for table in critical_tables:
            row = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            exists = bool(row and row[0] > 0)
            if exists:
                counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            else:
                counts[table] = None
        snapshot["table_counts"] = counts
    except Exception as exc:  # pylint: disable=broad-except
        snapshot["snapshot_error"] = str(exc)
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:  # pylint: disable=broad-except
            pass
    return snapshot


def _write_backup_metadata(backup_path: Path, src_db_path: Path) -> Path:
    meta_path = backup_path.with_suffix(backup_path.suffix + ".meta.json")
    payload = {
        "backup_file": str(backup_path),
        "backup_filename": backup_path.name,
        "backup_size_bytes": backup_path.stat().st_size,
        "backup_mtime": datetime.fromtimestamp(backup_path.stat().st_mtime).isoformat(timespec="seconds"),
        "backup_sha256": _sha256_file(backup_path),
        "source_db_snapshot": _collect_db_snapshot(src_db_path),
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta_path


def read_backup_metadata(backup_path: str) -> dict | None:
    p = Path(backup_path).resolve()
    meta_path = p.with_suffix(p.suffix + ".meta.json")
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:  # pylint: disable=broad-except
        return None


def verify_backup(backup_path: str) -> dict:
    p = Path(backup_path).resolve()
    result = {
        "path": str(p),
        "exists": p.exists(),
        "checksum_match": None,
        "sqlite_integrity_ok": None,
        "errors": [],
    }
    if not p.exists():
        result["errors"].append("backup_file_missing")
        return result

    meta = read_backup_metadata(str(p))
    if meta and meta.get("backup_sha256"):
        try:
            current_hash = _sha256_file(p)
            result["checksum_match"] = (current_hash == str(meta["backup_sha256"]))
            if not result["checksum_match"]:
                result["errors"].append("checksum_mismatch")
        except Exception as exc:  # pylint: disable=broad-except
            result["errors"].append(f"checksum_error:{exc}")

    try:
        conn = sqlite3.connect(str(p))
        row = conn.execute("PRAGMA integrity_check").fetchone()
        ok = bool(row and str(row[0]).lower() == "ok")
        result["sqlite_integrity_ok"] = ok
        if not ok:
            result["errors"].append(f"sqlite_integrity_fail:{row[0] if row else 'unknown'}")
    except Exception as exc:  # pylint: disable=broad-except
        result["errors"].append(f"sqlite_open_error:{exc}")
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:  # pylint: disable=broad-except
            pass
    return result


def create_backup(db_path: str, label: str = "manual", backup_dir: str | None = None) -> str:
    src = Path(db_path).resolve()
    if not src.exists():
        raise FileNotFoundError(f"DB 파일이 없습니다: {src}")

    out_dir = Path(backup_dir).resolve() if backup_dir else _default_backup_dir(db_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = out_dir / f"{src.stem}_{label}_{ts}{src.suffix}"
    shutil.copy2(src, dst)
    meta_path = _write_backup_metadata(dst, src)
    _mirror_backup(dst)
    _mirror_backup(meta_path)
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
    verify_before_restore: bool = True,
) -> str:
    src_backup = Path(backup_path).resolve()
    if not src_backup.exists():
        raise FileNotFoundError(f"백업 파일이 없습니다: {src_backup}")

    if verify_before_restore:
        check = verify_backup(str(src_backup))
        if check["sqlite_integrity_ok"] is False:
            raise RuntimeError(f"복원 전 검증 실패: SQLite 무결성 오류 ({check['errors']})")
        if check["checksum_match"] is False:
            raise RuntimeError(f"복원 전 검증 실패: 체크섬 불일치 ({check['errors']})")

    dst_db = Path(db_path).resolve()
    if keep_current_snapshot and dst_db.exists():
        create_backup(str(dst_db), label="pre_restore")

    shutil.copy2(src_backup, dst_db)
    return str(dst_db)
