from typing import Any, Dict
from ..base import State
from .migrations import migrate_to_latest

class DND5EPack:
    system_id = "dnd5e"
    latest_schema = "dnd5e.campaign_state.v2"

    def default_state(self, campaign_id: str) -> State:
        return {
            "schema": self.latest_schema,
            "campaign_id": campaign_id,
            "scene": {"scene_id": "scene_001", "location": "Unknown", "light": "dim", "threat": "low",
                      "objectives": [], "effects": []},
            "time": {"in_world": "Unknown", "round": 0, "turn_index": 0},
            "initiative": {"active": False, "order": [], "current": 0},
            "pcs": {},
            "npcs": {},
            "flags": {"revealed": [], "consequences": []},
            "meta": {"system_id": self.system_id, "updated_utc": "1970-01-01T00:00:00Z"},
        }

    def migrate_state(self, state: State) -> State:
        return migrate_to_latest(state, latest_schema=self.latest_schema, system_id=self.system_id)

    def post_load_normalize(self, state: State) -> State:
        # Normalizaciones suaves
        state.setdefault("flags", {})
        state["flags"].setdefault("revealed", [])
        state["flags"].setdefault("consequences", [])
        return state