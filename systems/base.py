from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Protocol, Callable, Optional

State = Dict[str, Any]

class SystemPack(Protocol):
    system_id: str
    latest_schema: str

    def default_state(self, campaign_id: str) -> State: ...
    def migrate_state(self, state: State) -> State: ...
    def post_load_normalize(self, state: State) -> State: ...