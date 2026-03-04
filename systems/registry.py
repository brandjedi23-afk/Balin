from typing import Dict, Any
from .base import SystemPack

_REGISTRY: Dict[str, SystemPack] = {}

def register(pack: SystemPack) -> None:
    _REGISTRY[pack.system_id] = pack

def get_pack(system_id: str) -> Any:
    system_id = system_id.lower()

    if system_id in ("wot", "wheel_of_time"):
        from systems.wot.pack import WoTPack
        return WoTPack()

    if system_id in ("dnd5e", "5e"):
        from systems.dnd5e.pack import DnD5ePack
        return DnD5ePack()

    if system_id in ("greyhawk",):
        from systems.greyhawk.pack import GreyhawkPack
        return GreyhawkPack()
    
    raise ValueError(f"Unknown system_id: {system_id}")