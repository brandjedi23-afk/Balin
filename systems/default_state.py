from datetime import datetime, timezone

def default_state(campaign_id: str) -> dict:
    return {
        "schema": "balin.state.v1",
        "campaign_id": campaign_id,
        "scene": {},
        "time": {},
        "initiative": {},
        "pcs": {},
        "npcs": {},
        "flags": {},
        "meta": {"created_utc": datetime.now(timezone.utc).isoformat()},
    }