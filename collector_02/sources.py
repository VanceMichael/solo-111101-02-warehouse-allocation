import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict
    body: bytes


class FixtureTransport:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.routes = json.loads((self.root / "routes.json").read_text())
        self.calls = []
        self.positions = {}

    def request(self, route, headers=None):
        attempts = self.routes[route]
        position = self.positions.get(route, 0)
        response = attempts[min(position, len(attempts) - 1)]
        self.positions[route] = position + 1
        self.calls.append({"route": route, "headers": dict(headers or {}), "attempt": position + 1})
        body = b""
        if "file" in response:
            path = (self.root / response["file"]).resolve()
            if not path.is_relative_to(self.root):
                raise ValueError("fixture path leaves source root")
            body = path.read_bytes()
        return Response(response["status"], dict(response.get("headers", {})), body)
