import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import ujson

from src.config import get_settings
from src.http_clients import get_http_client
from src.logging_setup import get_logger


@dataclass
class StoredObject:
    key: str
    size: int
    created_at: datetime


class StorageError(Exception):
    pass


class Storage:
    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(component="storage")
        self.mode = "remote" if self.settings.is_production() and self.settings.blob_base_url else "local"
        self.client = get_http_client()
        self.local_root = Path(os.environ.get("OZEL_GPT_STORAGE_DIR", "/tmp/ozel-gpt"))

    def put_json(self, key: str, data: Dict) -> None:
        payload = ujson.dumps(data, ensure_ascii=True)
        if self.mode == "local":
            path = self.local_root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload)
            return
        url = f"{self.settings.blob_base_url.rstrip('/')}/{key}"
        resp = self.client.put(url, data=payload.encode("utf-8"), headers={"Content-Type": "application/json"})
        if resp.status_code >= 400:
            raise StorageError(f"failed to upload {key}: {resp.text}")

    def get_json(self, key: str) -> Optional[Dict]:
        if self.mode == "local":
            path = self.local_root / key
            if not path.exists():
                return None
            return ujson.loads(path.read_text())
        url = f"{self.settings.blob_base_url.rstrip('/')}/{key}"
        resp = self.client.get(url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def list(self, prefix: str, limit: int = 100) -> List[StoredObject]:
        if self.mode == "local":
            base = self.local_root / prefix
            results: List[StoredObject] = []
            if not base.exists():
                return results
            for path in sorted(base.rglob("*.json"), reverse=True):
                stat = path.stat()
                results.append(
                    StoredObject(
                        key=str(path.relative_to(self.local_root)),
                        size=stat.st_size,
                        created_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                    )
                )
                if len(results) >= limit:
                    break
            return results
        url = f"{self.settings.blob_base_url.rstrip('/')}/list"
        resp = self.client.get(url, params={"prefix": prefix, "limit": limit})
        resp.raise_for_status()
        payload = resp.json()
        results: List[StoredObject] = []
        for item in payload.get("items", []):
            created_raw = item.get("created_at")
            created = datetime.fromisoformat(created_raw) if created_raw else datetime.now(timezone.utc)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            results.append(
                StoredObject(
                    key=item["key"],
                    size=item.get("size", 0),
                    created_at=created,
                )
            )
        return results

    def delete(self, key: str) -> None:
        if self.mode == "local":
            path = self.local_root / key
            if path.exists():
                path.unlink()
            return
        url = f"{self.settings.blob_base_url.rstrip('/')}/{key}"
        resp = self.client.delete(url)
        if resp.status_code not in (200, 204, 404):
            raise StorageError(f"failed to delete {key}: {resp.text}")


@lru_cache()
def get_storage() -> Storage:
    return Storage()
