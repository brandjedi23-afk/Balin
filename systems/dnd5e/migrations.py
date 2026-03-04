from typing import Any, Dict

State = Dict[str, Any]

def _schema(state: State) -> str:
    v = state.get("schema")
    return v if isinstance(v, str) else ""

def migrate_to_latest(state: State, latest_schema: str, system_id: str) -> State:
    s = dict(state)  # copia superficial

    # 0) Si no hay schema -> asumimos v1 legacy
    if not _schema(s):
        s["schema"] = "dnd5e.campaign_state.v1"

    # 1) v1 -> v2 (ejemplo: asegurar flags/meta y mover campos si hiciera falta)
    if s["schema"] == "dnd5e.campaign_state.v1":
        s.setdefault("flags", {})
        s["flags"].setdefault("revealed", [])
        s["flags"].setdefault("consequences", [])

        s.setdefault("meta", {})
        if not isinstance(s["meta"], dict):
            s["meta"] = {}
        s["meta"].setdefault("system_id", system_id)
        s["meta"].setdefault("updated_utc", "1970-01-01T00:00:00Z")

        # marca schema nuevo
        s["schema"] = "dnd5e.campaign_state.v2"

    # 2) Si ya está en v2 pero falta system_id, lo repones
    if s["schema"] == "dnd5e.campaign_state.v2":
        s.setdefault("meta", {})
        if not isinstance(s["meta"], dict):
            s["meta"] = {}
        s["meta"].setdefault("system_id", system_id)

    # 3) Guard rail: si hay un schema raro, no lo rompas; solo fuerza meta
    s.setdefault("meta", {})
    if not isinstance(s["meta"], dict):
        s["meta"] = {}
    s["meta"].setdefault("system_id", system_id)

    # Finalmente, fuerza “latest” solo si es tu sistema (opcional)
    # (yo lo haría solo si has aplicado migraciones conocidas)
    if s["schema"].startswith("dnd5e.campaign_state.") and s["schema"] != latest_schema:
        s["schema"] = latest_schema

    return s