# systems/wot/pack.py

class WoTPack:
    system_id = "wot"
    name = "Wheel of Time"

    def __init__(self):
        pass

    def migrations(self):
        return []

    def info(self):
        return {"system_id": self.system_id, "name": self.name}