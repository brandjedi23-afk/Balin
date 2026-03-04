from .registry import register
from .dnd5e.pack import DND5EPack
from .wot.pack import WoTPack
from .greyhawk.pack import GreyhawkPack

# registra SIEMPRE dnd5e primero
register(DND5EPack())

# el resto: si falla, que falle al arrancar (preferible) o loguea y sigue
register(WoTPack())
register(GreyhawkPack())