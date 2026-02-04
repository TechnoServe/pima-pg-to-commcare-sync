from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class SyncResult:
    picked: int
    sent: int
    groups: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {"picked": self.picked, "sent": self.sent, "groups": self.groups}


class SyncHandler:
    """A small interface implemented by each entity sync handler."""

    entity: str
    batch_limit: int

    def sync_batch(self) -> Dict[str, Any]:
        raise NotImplementedError

    def sync_one(self, record_id: str) -> Dict[str, Any]:
        raise NotImplementedError
