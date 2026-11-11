from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json


@dataclass(frozen=True)
class SourceRecord:
    source: str
    observed_at: str
    payload: dict

    @classmethod
    def parse(cls, value: dict):
        if not isinstance(value, dict):
            raise ValueError("记录必须是 JSON 对象")
        source = str(value.get("source", "")).strip()
        observed_at = str(value.get("observed_at", "")).strip()
        payload = value.get("payload")
        if not source or not observed_at or not isinstance(payload, dict):
            raise ValueError("记录缺少来源、观测时间或对象型 payload")
        try:
            datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("观测时间必须为 ISO-8601") from error
        return cls(source, observed_at, payload)

    def fingerprint(self) -> str:
        canonical = json.dumps(self.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return sha256(f"{self.source}\0{self.observed_at}\0{canonical}".encode()).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
