from typing import Dict
from .base import SystemPack

_REGISTRY: Dict[str, SystemPack] = {}

def register(pack: SystemPack) -> None:
    _REGISTRY[pack.system_id] = pack

def get_pack(system_id: str) -> SystemPack:
    sid = (system_id or "").strip().lower()
    if sid in _REGISTRY:
        return _REGISTRY[sid]
    # fallback: si no existe, usa dnd5e o el que quieras
    return _REGISTRY["dnd5e"]