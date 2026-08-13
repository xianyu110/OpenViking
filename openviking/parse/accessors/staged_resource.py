# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Durable source snapshots for asynchronous resource ingestion."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict

from openviking.server.identity import RequestContext
from openviking.utils.path_safety import (
    safe_join_viking_uri,
    sanitize_relative_viking_path,
)
from openviking_cli.exceptions import InvalidArgumentError

from .base import LocalResource

_COPY_CONCURRENCY = 8
_PRIVATE_META_KEYS = {
    "auth_config",
    "feishu_access_token",
    "feishu_refresh_token",
}


@dataclass(frozen=True)
class StagedResource:
    """Serializable reference to one queue-owned source snapshot."""

    temp_uri: str
    source_uri: str
    source_name: str
    source_type: str
    original_source: str
    is_directory: bool
    meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StagedResource":
        if not isinstance(data, dict):
            raise ValueError("staged_source must be an object")
        temp_uri = data.get("temp_uri")
        source_uri = data.get("source_uri")
        source_name = data.get("source_name")
        if not isinstance(temp_uri, str) or not temp_uri.startswith("viking://temp/"):
            raise ValueError("staged_source.temp_uri must be a concrete temp URI")
        bundle_uri = f"{temp_uri.rstrip('/')}/source"
        if not isinstance(source_uri, str) or not source_uri.startswith(f"{bundle_uri}/"):
            raise ValueError("staged_source.source_uri must be inside its source bundle")
        if not isinstance(source_name, str) or not source_name:
            raise ValueError("staged_source.source_name is required")
        meta = data.get("meta")
        if not isinstance(meta, dict):
            raise ValueError("staged_source.meta must be an object")
        return cls(
            temp_uri=temp_uri.rstrip("/"),
            source_uri=source_uri.rstrip("/"),
            source_name=source_name,
            source_type=str(data.get("source_type") or "local"),
            original_source=str(data.get("original_source") or ""),
            is_directory=bool(data.get("is_directory", False)),
            meta=dict(meta),
        )


def public_resource_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Keep accessor metadata that is safe to expose and persist."""
    sanitized: Dict[str, Any] = {}
    for key, value in meta.items():
        if key.startswith("_") or key in _PRIVATE_META_KEYS:
            continue
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            continue
        sanitized[key] = value
    return sanitized


async def stage_resource(
    resource: LocalResource,
    *,
    viking_fs: Any,
    ctx: RequestContext,
    original_source: str,
) -> StagedResource:
    """Copy an accessor result into user-scoped VikingFS temp storage."""
    path = resource.path
    if not path.is_file() and not path.is_dir():
        raise InvalidArgumentError(f"Resource source is not a regular file or directory: {path}")

    temp_uri = viking_fs.create_temp_uri(ctx=ctx).rstrip("/")
    is_directory = path.is_dir()
    source_name = path.name or "resource"
    bundle_uri = safe_join_viking_uri(temp_uri, "source")
    source_uri = safe_join_viking_uri(bundle_uri, source_name)

    try:
        if is_directory:
            await _stage_directory(path, source_uri, viking_fs, ctx)
        else:
            cleanup_path = resource.meta.get("_cleanup_path")
            bundle_path = Path(cleanup_path) if cleanup_path else None
            if (
                resource.is_temporary
                and bundle_path is not None
                and bundle_path.is_dir()
                and path.is_relative_to(bundle_path)
            ):
                await _stage_directory(bundle_path, bundle_uri, viking_fs, ctx)
                source_uri = safe_join_viking_uri(
                    bundle_uri,
                    path.relative_to(bundle_path).as_posix(),
                )
            else:
                content = await asyncio.to_thread(path.read_bytes)
                await viking_fs.write_file_bytes(source_uri, content, ctx=ctx)
    except BaseException:
        await viking_fs.delete_temp(temp_uri, ctx=ctx)
        raise

    return StagedResource(
        temp_uri=temp_uri,
        source_uri=source_uri,
        source_name=source_name,
        source_type=resource.source_type,
        original_source=original_source,
        is_directory=is_directory,
        meta=public_resource_meta(resource.meta),
    )


async def _stage_directory(
    local_dir: Path,
    source_uri: str,
    viking_fs: Any,
    ctx: RequestContext,
) -> None:
    directories = {source_uri}
    files: list[tuple[Path, str]] = []
    for root, dir_names, file_names in os.walk(local_dir, followlinks=False):
        root_path = Path(root)
        dir_names[:] = [name for name in dir_names if not (root_path / name).is_symlink()]
        rel_root = root_path.relative_to(local_dir)
        if rel_root.parts:
            directories.add(safe_join_viking_uri(source_uri, rel_root.as_posix()))
        for name in file_names:
            local_path = root_path / name
            if local_path.is_symlink() or not local_path.is_file():
                continue
            rel_path = local_path.relative_to(local_dir).as_posix()
            target_uri = safe_join_viking_uri(source_uri, rel_path)
            directories.add(target_uri.rsplit("/", 1)[0])
            files.append((local_path, target_uri))

    for uri in sorted(directories):
        await viking_fs.mkdir(uri, exist_ok=True, ctx=ctx)

    semaphore = asyncio.Semaphore(_COPY_CONCURRENCY)

    async def _copy_file(local_path: Path, target_uri: str) -> None:
        async with semaphore:
            content = await asyncio.to_thread(local_path.read_bytes)
            await viking_fs.write_file_bytes(target_uri, content, ctx=ctx)

    await asyncio.gather(*(_copy_file(path, uri) for path, uri in files))


async def materialize_resource(
    staged: StagedResource,
    *,
    viking_fs: Any,
    ctx: RequestContext,
) -> LocalResource:
    """Restore a durable snapshot to worker-local storage for the parser."""
    temp_root = Path(tempfile.mkdtemp(prefix="ov_staged_resource_"))
    bundle_uri = safe_join_viking_uri(staged.temp_uri, "source")
    source_relpath = staged.source_uri.removeprefix(f"{bundle_uri}/")
    try:
        entries = await viking_fs.tree(
            bundle_uri,
            output="original",
            show_all_hidden=True,
            node_limit=None,
            level_limit=None,
            ctx=ctx,
        )
        directories = [entry for entry in entries if entry.get("isDir", False)]
        files = [entry for entry in entries if not entry.get("isDir", False)]
        for entry in directories:
            _safe_local_target(temp_root, entry.get("rel_path")).mkdir(
                parents=True,
                exist_ok=True,
            )

        semaphore = asyncio.Semaphore(_COPY_CONCURRENCY)

        async def _copy_file(entry: Dict[str, Any]) -> None:
            async with semaphore:
                target = _safe_local_target(temp_root, entry.get("rel_path"))
                target.parent.mkdir(parents=True, exist_ok=True)
                content = await viking_fs.read_file_bytes(str(entry["uri"]), ctx=ctx)
                await asyncio.to_thread(target.write_bytes, content)

        await asyncio.gather(*(_copy_file(entry) for entry in files))
        local_path = _safe_local_target(temp_root, source_relpath)
        if staged.is_directory and not local_path.is_dir():
            raise ValueError("Staged resource directory is missing")
        if not staged.is_directory and not local_path.is_file():
            raise ValueError("Staged resource file is missing")
    except BaseException:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise

    meta = dict(staged.meta)
    meta["_cleanup_path"] = str(temp_root)
    return LocalResource(
        path=local_path,
        source_type=staged.source_type,
        original_source=staged.original_source,
        meta=meta,
        is_temporary=True,
    )


def _safe_local_target(root: Path, rel_path: Any) -> Path:
    if not isinstance(rel_path, str):
        raise ValueError("Staged resource entry is missing rel_path")
    normalized = sanitize_relative_viking_path(rel_path)
    parts = Path(normalized).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Unsafe staged resource path: {rel_path}")
    return root.joinpath(*parts)
