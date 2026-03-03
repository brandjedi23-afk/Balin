import os
import json
import re
import uuid
import random
import datetime
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Union

import fcntl
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

import hashlib

# =========================
# ENV / PATHS
# =========================

SERVICE_NAME = os.getenv("SERVICE_NAME", "dnd5e-dm-compact")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CAMPAIGNS_DIR = Path(os.getenv("CAMPAIGNS_DIR", str(DATA_DIR / "campaigns")))

DM_API_TOKEN = os.getenv("DM_API_TOKEN", "")

DEBUG_MECHANICS = os.getenv("DEBUG_MECHANICS", "true").lower() == "true"
STRICT_DICE = os.getenv("STRICT_DICE", "true").lower() == "true"
AUTO_CREATE_CAMPAIGN = os.getenv("AUTO_CREATE_CAMPAIGN", "true").lower() == "true"

# Relative paths inside each campaign folder
PC_DIR = os.getenv("PC_DIR", "pc")
NPC_DIR = os.getenv("NPC_DIR", "npc")
STATE_FILE = os.getenv("STATE_FILE", "state/state.json")
LOG_FILE = os.getenv("LOG_FILE", "log/log.jsonl")
RULINGS_FILE = os.getenv("RULINGS_FILE", "rulings/rulings.jsonl")

app = FastAPI(title="D&D 5e DM Compact API", version="1.0.0")


# =========================
# HELPERS
# =========================

def utcnow_z() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def state_hash(state: Dict[str, Any]) -> str:
    """
    Stable hash of the full campaign state.
    Uses canonical JSON encoding (sorted keys) so the hash is deterministic.
    """
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def require_api_key(x_api_key: Optional[str]) -> None:
    if not DM_API_TOKEN:
        # Allow running locally without token (optional)
        return
    if not x_api_key or x_api_key != DM_API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-API-KEY)")


def campaign_root(campaign_id: str) -> Path:
    return CAMPAIGNS_DIR / norm_campaign_id(campaign_id)


def norm_campaign_id(campaign_id: str) -> str:
    return campaign_id.strip().lower()


def norm_pc_id(pc_id: str) -> str:
    return pc_id.strip().lower()


def norm_npc_id(npc_id: str) -> str:
    return npc_id.strip().lower()


def ensure_campaign_dirs(campaign_id: str) -> Dict[str, Path]:
    root = campaign_root(campaign_id)
    pc = root / PC_DIR
    npc = root / NPC_DIR
    state = root / Path(STATE_FILE).parent
    log = root / Path(LOG_FILE).parent
    rulings = root / Path(RULINGS_FILE).parent

    for p in (pc, npc, state, log, rulings):
        p.mkdir(parents=True, exist_ok=True)

    return {
        "root": root,
        "pc_dir": pc,
        "npc_dir": npc,
        "state_path": root / STATE_FILE,
        "log_path": root / LOG_FILE,
        "rulings_path": root / RULINGS_FILE,
    }


def find_ruling(rulings_path: Path, scene_id: Optional[str], key: str) -> Optional[dict]:
    """
    Returns the most recent ruling matching (scene_id, key).
    If scene_id is None, matches any scene.
    """
    if not rulings_path.exists():
        return None

    lock_path = rulings_path.with_suffix(rulings_path.suffix + ".lock")

    with locked_open(lock_path, "w") as _lock:
        try:
            with open(rulings_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            return None

    key = key.strip()

    # Search newest first
    for line in reversed(lines):
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue

        if obj.get("key") != key:
            continue

        if scene_id is not None and obj.get("scene_id") != scene_id:
            continue

        return obj

    return None


def read_log_tail(log_path: Path, n: int) -> list[dict]:
    """
    Reads the last n JSONL entries from log_path safely.
    Simple implementation: read all lines, slice tail.
    """
    if n < 1:
        n = 1
    if n > 500:
        n = 500

    if not log_path.exists():
        return []

    # Lock via sidecar lock file to avoid partial reads during writes
    lock_path = log_path.with_suffix(log_path.suffix + ".lock")
    with locked_open(lock_path, "w") as _lock:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            return []

    tail = lines[-n:]
    out: list[dict] = []
    for line in tail:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            # If a line is corrupted, skip it (keep endpoint resilient)
            continue
    return out


def find_in_log(log_path: Path, q: str, n: int) -> list[dict]:
    """
    Naive JSONL search:
      - reads log file safely
      - returns up to n entries where `q` is a substring of the raw JSON line
    Notes:
      - case-insensitive
      - good enough for debug; later we can index if needed
    """
    q = (q or "").strip()
    if not q:
        return []

    if n < 1:
        n = 1
    if n > 500:
        n = 500

    if not log_path.exists():
        return []

    q_low = q.lower()

    lock_path = log_path.with_suffix(log_path.suffix + ".lock")
    with locked_open(lock_path, "w") as _lock:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            return []

    # Search from newest to oldest (most useful for debugging)
    out: list[dict] = []
    for line in reversed(lines):
        if len(out) >= n:
            break
        if not line:
            continue
        if q_low not in line.lower():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue

    # Return in chronological order (oldest->newest) for readability
    out.reverse()
    return out


def recent_hashes_from_log(log_path: Path, n: int = 20) -> list[dict]:
    """
    Returns last n entries that contain state hashes from /turn logs.
    Output items: {turn_id, ts_utc, prev_state_hash, new_state_hash}
    """
    if n < 1:
        n = 1
    if n > 200:
        n = 200

    if not log_path.exists():
        return []

    lock_path = log_path.with_suffix(log_path.suffix + ".lock")
    with locked_open(lock_path, "w") as _lock:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            return []

    out: list[dict] = []
    for line in reversed(lines):
        if len(out) >= n:
            break
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue

        # Only keep entries that have hashes (i.e., /turn entries after your upgrade)
        if "prev_state_hash" in obj and "new_state_hash" in obj:
            out.append({
                "turn_id": obj.get("turn_id"),
                "ts_utc": obj.get("ts_utc"),
                "prev_state_hash": obj.get("prev_state_hash"),
                "new_state_hash": obj.get("new_state_hash"),
            })

    out.reverse()
    return out


def locked_open(path: Path, mode: str):
    """
    Cross-process lock using fcntl (Linux). Use for state/log writes.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, mode, encoding="utf-8")
    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    return f


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def safe_read_json(path: Path, default: Any) -> Any:
    """
    Read JSON under an exclusive lock to avoid partial reads during writes.
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    with locked_open(lock_path, "w") as _lock:
        return load_json(path, default)


def safe_write_json(path: Path, obj: Any) -> None:
    """
    Write JSON under an exclusive lock + atomic replace.
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    with locked_open(lock_path, "w") as _lock:
        save_json_atomic(path, obj)


def pc_path(campaign_id: str, pc_id: str) -> Path:
    paths = ensure_campaign_dirs(campaign_id)
    return paths["pc_dir"] / f"pc_{pc_id}.json"


def npc_path(campaign_id: str, npc_id: str) -> Path:
    paths = ensure_campaign_dirs(campaign_id)
    return paths["npc_dir"] / f"npc_{npc_id}.json"


def _as_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return default
        return int(x)
    except Exception:
        return default


def _dig(d: Any, path: list[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _extract_max_hp_from_sheet(sheet: Dict[str, Any]) -> Optional[int]:
    """
    Tries multiple common locations for max HP without enforcing a rigid schema.
    Returns None if not found.
    """
    candidates = [
        ["hp", "max"],
        ["hp", "max_hp"],
        ["hit_points", "max"],
        ["hit_points", "max_hp"],
        ["combat", "hp", "max"],
        ["combat", "hp", "max_hp"],
    ]
    for p in candidates:
        v = _dig(sheet, p)
        m = _as_int(v, None)
        if m is not None and m >= 0:
            return m
    return None


def get_pc_max_hp(campaign_id: str, pc_id: str) -> Optional[int]:
    """
    Reads PC sheet (if present) and extracts max HP.
    """
    path = pc_path(campaign_id, pc_id)
    if not path.exists():
        return None
    sheet = safe_read_json(path, default={})
    if not isinstance(sheet, dict):
        return None
    return _extract_max_hp_from_sheet(sheet)


def get_npc_max_hp(campaign_id: str, npc_id: str) -> Optional[int]:
    """
    Reads NPC sheet (if present) and extracts max HP.
    """
    path = npc_path(campaign_id, npc_id)
    if not path.exists():
        return None
    sheet = safe_read_json(path, default={})
    if not isinstance(sheet, dict):
        return None
    return _extract_max_hp_from_sheet(sheet)


def clamp_hp_value(current: Any, max_hp: Optional[int]) -> Optional[int]:
    c = _as_int(current, None)
    if c is None:
        return None
    if c < 0:
        c = 0
    if max_hp is not None and max_hp >= 0 and c > max_hp:
        c = max_hp
    return c


def enforce_hp_clamps(campaign_id: str, state: Dict[str, Any]) -> None:
    """
    Clamps HP for pcs/npcs in TEMPORARY state layer:
      - hp.current: [0..max_hp] if max_hp known
      - hp.temp: >= 0
    max_hp is taken from sheet if available, otherwise from entity.hp.max if present.
    """
    # PCs
    pcs = state.get("pcs")
    if isinstance(pcs, dict):
        for pc_id, pc_state in pcs.items():
            if not isinstance(pc_state, dict):
                continue
            hp = pc_state.get("hp")
            if not isinstance(hp, dict):
                continue

            # Determine max_hp: prefer sheet, fallback to state
            max_hp = get_pc_max_hp(campaign_id, str(pc_id))
            if max_hp is None:
                max_hp = _as_int(hp.get("max") or hp.get("max_hp"), None)

            new_cur = clamp_hp_value(hp.get("current"), max_hp)
            if new_cur is not None:
                hp["current"] = new_cur

            # temp hp clamp
            tmp = _as_int(hp.get("temp") or hp.get("temp_hp"), None)
            if tmp is not None and tmp < 0:
                hp["temp"] = 0

    # NPCs
    npcs = state.get("npcs")
    if isinstance(npcs, dict):
        for npc_id, npc_state in npcs.items():
            if not isinstance(npc_state, dict):
                continue
            hp = npc_state.get("hp")
            if not isinstance(hp, dict):
                continue

            max_hp = get_npc_max_hp(campaign_id, str(npc_id))
            if max_hp is None:
                max_hp = _as_int(hp.get("max") or hp.get("max_hp"), None)

            new_cur = clamp_hp_value(hp.get("current"), max_hp)
            if new_cur is not None:
                hp["current"] = new_cur

            tmp = _as_int(hp.get("temp") or hp.get("temp_hp"), None)
            if tmp is not None and tmp < 0:
                hp["temp"] = 0


def list_ids_in_dir(dir_path: Path, prefix: str) -> list[str]:
    """
    Lists ids from files like '{prefix}_{id}.json' inside dir_path.
    Example: prefix='pc' -> files pc_mira.json -> id 'mira'
    """
    if not dir_path.exists():
        return []
    ids: list[str] = []
    for p in dir_path.glob(f"{prefix}_*.json"):
        stem = p.stem  # e.g. "pc_mira"
        if stem.startswith(prefix + "_"):
            ids.append(stem[len(prefix) + 1 :])
    ids.sort()
    return ids


PROTECTED_ROOT_KEYS = {
    "schema",
    "campaign_id",
    "meta",
}


# ---------
# DOT-PATH PATCHING
# ---------

PatchValue = Union[
    Any,
    Dict[str, Any],  # {"op": "inc"/"append"/"set", ...}
]

def _get_parent_and_key(root: Dict[str, Any], path: str) -> (Dict[str, Any], str):
    parts = [p for p in path.split(".") if p]
    if not parts:
        raise ValueError("Empty path")
    cur: Dict[str, Any] = root
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    return cur, parts[-1]


def apply_patch(state: Dict[str, Any], patch: Dict[str, PatchValue]) -> Dict[str, Any]:
    """
    Supports:
      - set: {"some.path": value}
      - inc: {"some.num": {"op":"inc","by":-4}}
      - append: {"some.list": {"op":"append","value":X}}
    """
    for path, v in patch.items():
        parent, key = _get_parent_and_key(state, path)

        # op form
        if isinstance(v, dict) and "op" in v:
            op = v.get("op")
            if op == "set":
                parent[key] = v.get("value")
            elif op == "inc":
                by = v.get("by", 0)
                cur = parent.get(key, 0)
                if not isinstance(cur, (int, float)):
                    cur = 0
                parent[key] = cur + by
            elif op == "append":
                item = v.get("value")
                cur = parent.get(key)
                if cur is None:
                    parent[key] = [item]
                elif isinstance(cur, list):
                    cur.append(item)
                else:
                    parent[key] = [cur, item]
            else:
                raise ValueError(f"Unknown op: {op}")
        else:
            # direct set
            parent[key] = v
    return state


def validate_state_patch(patch: Dict[str, Any]) -> None:
    """
    Minimal safety validation:
      - patch must be dict
      - cannot override protected root keys
      - cannot override full pcs/npcs/initiative objects
      - cannot override time.turn_index (authoritative)
    """
    if not isinstance(patch, dict):
        raise ValueError("state_patch must be an object")

    for path in patch.keys():

        # Root key detection
        root_key = path.split(".")[0]

        # Block protected top-level keys
        if root_key in PROTECTED_ROOT_KEYS:
            raise ValueError(f"Modification of '{root_key}' is not allowed")

        # Prevent overwriting full structures
        if path in ("pcs", "npcs", "initiative"):
            raise ValueError(f"Overwriting '{path}' root object is not allowed")

        # Prevent overriding authoritative turn counter
        if path == "time.turn_index":
            raise ValueError("time.turn_index is managed by the server")


def ensure_time_and_inc_turn_index(state: Dict[str, Any]) -> None:
    """
    Ensures state.time exists and increments time.turn_index by 1.
    This is enforced on every /turn call (authoritative monotonic counter).
    """
    time_obj = state.get("time")
    if not isinstance(time_obj, dict):
        time_obj = {}
        state["time"] = time_obj

    cur = time_obj.get("turn_index", 0)
    if not isinstance(cur, int):
        cur = 0
    time_obj["turn_index"] = cur + 1

    # Optional: keep round key present for consistency (do not auto-increment round)
    if "round" not in time_obj or not isinstance(time_obj.get("round"), int):
        time_obj["round"] = 0


# ---------
# DICE ROLLER
# ---------

DICE_RE = re.compile(r"^\s*(\d*)d(\d+)\s*([+-]\s*\d+)?\s*$", re.IGNORECASE)

def roll_expr(expr: str) -> Dict[str, Any]:
    """
    Minimal dice: NdM(+/-)K  e.g. '1d20+5', '2d6-1'
    Returns: total, detail, raw list, text.
    """
    m = DICE_RE.match(expr)
    if not m:
        raise ValueError("Unsupported dice expression. Use NdM+K, e.g. 1d20+5")

    n_str, die_str, mod_str = m.groups()
    n = int(n_str) if n_str else 1
    die = int(die_str)
    mod = int(mod_str.replace(" ", "")) if mod_str else 0

    if n < 1 or n > 200:
        raise ValueError("Dice count out of range (1..200)")
    if die < 2 or die > 1000:
        raise ValueError("Die size out of range (2..1000)")

    rolls = [random.randint(1, die) for _ in range(n)]
    subtotal = sum(rolls)
    total = subtotal + mod

    # detail string: "4+2+6+3"
    detail = "+".join(str(r) for r in rolls)
    if mod != 0:
        sign = "+" if mod > 0 else "-"
        detail = f"{detail}{sign}{abs(mod)}"

    text = f"{expr.strip()} = {total} ({detail})"
    return {"total": total, "detail": detail, "raw": rolls, "text": text}


# =========================
# MODELS
# =========================

class RollReq(BaseModel):
    expr: str = Field(..., description="Dice expression, e.g. 1d20+5")
    label: Optional[str] = None
    campaign_id: Optional[str] = None  # optional for log linkage


class RollResp(BaseModel):
    roll_id: str
    total: int
    detail: str
    raw: list[int]
    text: str
    expr: str
    label: Optional[str] = None
    ts_utc: str


class TurnReq(BaseModel):
    campaign_id: str
    speaker: Literal["player", "dm"] = "player"
    input_text: str

    # Rolls already performed (from /roll)
    rolls: list[Dict[str, Any]] = Field(default_factory=list)

    # Rules / adjudication notes
    rulings: list[str] = Field(default_factory=list)

    # Patch to apply to campaign state.json
    # Example:
    # {
    #   "time.turn_index": {"op":"inc","by":1},
    #   "flags.revealed": {"op":"append","value":"Tripwire found"},
    #   "pcs.pc_mira.hp.current": {"op":"inc","by":-4}
    # }
    state_patch: Dict[str, Any] = Field(default_factory=dict)

    output_summary: str = ""


class TurnResp(BaseModel):
    campaign_id: str
    turn_id: str
    state: Dict[str, Any]


class UpsertPCReq(BaseModel):
    campaign_id: str
    pc_id: str
    sheet: Dict[str, Any] = Field(..., description="PC sheet JSON (pc_sheet.v1)")


class UpsertNPCReq(BaseModel):
    campaign_id: str
    npc_id: str
    sheet: Dict[str, Any] = Field(..., description="NPC sheet JSON (npc_sheet.v1)")


class ListResp(BaseModel):
    campaign_id: str
    ids: list[str]


class RulingReq(BaseModel):
    campaign_id: str
    scene_id: Optional[str] = None
    type: str  # e.g. "dc", "interpretation", "environment", etc.
    key: str   # unique identifier, e.g. "climb_wall_dc"
    value: Any
    notes: Optional[str] = None


class RulingResp(BaseModel):
    campaign_id: str
    scene_id: Optional[str]
    key: str
    stored: bool
    ts_utc: str


class RulingFindResp(BaseModel):
    campaign_id: str
    scene_id: Optional[str]
    key: str
    found: bool
    ruling: Optional[Dict[str, Any]] = None


class LogTailResp(BaseModel):
    campaign_id: str
    n: int
    entries: list[Dict[str, Any]]


class LogFindResp(BaseModel):
    campaign_id: str
    q: str
    n: int
    matches: int
    entries: list[Dict[str, Any]]


class SnapshotResp(BaseModel):
    campaign_id: str
    state: Dict[str, Any]
    pc_ids: list[str]
    npc_ids: list[str]
    recent_hashes: list[Dict[str, Any]]


class CampaignState(BaseModel):
    schema: str
    campaign_id: str
    scene: Dict[str, Any]
    time: Dict[str, Any]
    initiative: Dict[str, Any]
    pcs: Dict[str, Any]
    npcs: Dict[str, Any]
    flags: Dict[str, Any]
    meta: Dict[str, Any]


# =========================
# ROUTES
# =========================

@app.get("/health")
def health():
    return {"ok": True, "service": SERVICE_NAME, "ts_utc": utcnow_z()}


@app.post("/roll", response_model=RollResp)
def api_roll(req: RollReq, x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    require_api_key(x_api_key)

    try:
        result = roll_expr(req.expr)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    roll_id = "r_" + uuid.uuid4().hex[:12]
    resp = {
        "roll_id": roll_id,
        "total": int(result["total"]),
        "detail": result["detail"],
        "raw": result["raw"],
        "text": result["text"],
        "expr": req.expr.strip(),
        "label": req.label,
        "ts_utc": utcnow_z(),
    }

    # Optional: if campaign_id provided, append roll to log (audit)
    if req.campaign_id:
        req.campaign_id = norm_campaign_id(req.campaign_id)
        req.pc_id = norm_pc_id(req.pc_id)
        paths = ensure_campaign_dirs(req.campaign_id)
        entry = {
            "ts_utc": resp["ts_utc"],
            "campaign_id": req.campaign_id,
            "turn_id": "roll_" + roll_id,
            "speaker": "dm",
            "input_text": f"ROLL: {req.label or ''}".strip(),
            "rolls": [resp],
            "rulings": [],
            "state_patch": {},
            "output_summary": resp["text"],
        }
        with locked_open(paths["log_path"], "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()

    return resp


@app.get("/state", response_model=CampaignState)
def state(
    campaign_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)
    default_state = {
        "schema": "dnd5e.campaign_state.v1",
        "campaign_id": campaign_id,
        "scene": {
            "scene_id": "scene_001",
            "location": "Unknown",
            "light": "dim",
            "threat": "low",
            "objectives": [],
            "effects": [],
        },
        "time": {"in_world": "Unknown", "round": 0, "turn_index": 0},
        "initiative": {"active": False, "order": [], "current": 0},
        "pcs": {},
        "npcs": {},
        "flags": {"revealed": [], "consequences": []},
        "meta": {"updated_utc": utcnow_z()},
    }
    state = safe_read_json(paths["state_path"], default_state)
    return state


@app.get("/log/tail", response_model=LogTailResp)
def log_tail(
    campaign_id: str,
    n: int = 50,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)

    entries = read_log_tail(paths["log_path"], n)
    return {"campaign_id": campaign_id, "n": min(max(int(n), 1), 500), "entries": entries}


@app.get("/pc")
def get_pc(
    campaign_id: str,
    pc_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)
    pc_id = norm_pc_id(pc_id)

    # Auto-create campaign dirs if enabled
    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    path = pc_path(campaign_id, pc_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="PC not found")

    return safe_read_json(path, default={})


@app.put("/pc")
def upsert_pc(
    req: UpsertPCReq,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    req.campaign_id = norm_campaign_id(req.campaign_id)

    # Auto-create campaign dirs if enabled
    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    # Minimal sanity checks (lightweight, schema validation optional)
    req.sheet.setdefault("campaign_id", req.campaign_id)
    req.sheet.setdefault("pc_id", req.pc_id)

    path = pc_path(req.campaign_id, req.pc_id)
    safe_write_json(path, req.sheet)

    # Optional: append audit log entry (recommended)
    paths = ensure_campaign_dirs(req.campaign_id)
    entry = {
        "ts_utc": utcnow_z(),
        "campaign_id": req.campaign_id,
        "turn_id": "pc_upsert_" + uuid.uuid4().hex[:12],
        "speaker": "dm",
        "input_text": f"UPSERT PC {req.pc_id}",
        "rolls": [],
        "rulings": [],
        "state_patch": {"pc_sheet_upserted": req.pc_id},
        "output_summary": "PC sheet persisted.",
    }
    with locked_open(paths["log_path"], "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()

    return {"ok": True, "campaign_id": req.campaign_id, "pc_id": req.pc_id}


@app.get("/npc")
def get_npc(
    campaign_id: str,
    npc_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)
    npc_id = norm_npc_id(npc_id)

    # Auto-create campaign dirs if enabled
    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    path = npc_path(campaign_id, npc_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="NPC not found")

    return safe_read_json(path, default={})


@app.put("/npc")
def upsert_npc(
    req: UpsertNPCReq,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    req.campaign_id = norm_campaign_id(req.campaign_id)
    req.npc_id = norm_npc_id(req.npc_id)

    # Auto-create campaign dirs if enabled
    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    # Minimal sanity checks
    req.sheet.setdefault("campaign_id", req.campaign_id)
    req.sheet.setdefault("npc_id", req.npc_id)

    path = npc_path(req.campaign_id, req.npc_id)
    safe_write_json(path, req.sheet)

    # Optional: append audit log entry
    paths = ensure_campaign_dirs(req.campaign_id)
    entry = {
        "ts_utc": utcnow_z(),
        "campaign_id": req.campaign_id,
        "turn_id": "npc_upsert_" + uuid.uuid4().hex[:12],
        "speaker": "dm",
        "input_text": f"UPSERT NPC {req.npc_id}",
        "rolls": [],
        "rulings": [],
        "state_patch": {"npc_sheet_upserted": req.npc_id},
        "output_summary": "NPC sheet persisted.",
    }
    with locked_open(paths["log_path"], "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()

    return {"ok": True, "campaign_id": req.campaign_id, "npc_id": req.npc_id}


@app.get("/pc/list", response_model=ListResp)
def list_pcs(
    campaign_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)
    ids = list_ids_in_dir(paths["pc_dir"], "pc")
    return {"campaign_id": campaign_id, "ids": ids}


@app.get("/npc/list", response_model=ListResp)
def list_npcs(
    campaign_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)
    ids = list_ids_in_dir(paths["npc_dir"], "npc")
    return {"campaign_id": campaign_id, "ids": ids}


@app.post("/turn", response_model=TurnResp)
def api_turn(req: TurnReq, x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    require_api_key(x_api_key)
    req.campaign_id = norm_campaign_id(req.campaign_id)

    # Create campaign if allowed
    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    paths = ensure_campaign_dirs(req.campaign_id)

    # Load state
    default_state = {
        "schema": "dnd5e.campaign_state.v1",
        "campaign_id": req.campaign_id,
        "scene": {
            "scene_id": "scene_001",
            "location": "Unknown",
            "light": "dim",
            "threat": "low",
            "objectives": [],
            "effects": []
        },
        "time": {"in_world": "Unknown", "round": 0, "turn_index": 0},
        "initiative": {"active": False, "order": [], "current": 0},
        "pcs": {},
        "npcs": {},
        "flags": {"revealed": [], "consequences": []},
        "meta": {"updated_utc": utcnow_z()},
    }

    # Lock state while patching
    lock_path = paths["state_path"].with_suffix(".lock")

    with locked_open(lock_path, "w") as _lock:
        state = load_json(paths["state_path"], default_state)

        # Hash BEFORE any changes
        prev_hash = state_hash(state)

        # Prevent clients from overriding authoritative turn_index
        if "time.turn_index" in req.state_patch:
            req.state_patch.pop("time.turn_index", None)

        # Validate patch safety
        if req.state_patch:
            try:
                validate_state_patch(req.state_patch)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid state_patch: {e}")

            try:
                state = apply_patch(state, req.state_patch)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid state_patch: {e}")

        # Enforce turn counter increment (authoritative)
        ensure_time_and_inc_turn_index(state)

        # Clamp HP values
        enforce_hp_clamps(req.campaign_id, state)

        # Always bump updated time
        state.setdefault("meta", {})
        state["meta"]["updated_utc"] = utcnow_z()

        # Hash AFTER all changes
        new_hash = state_hash(state)

        save_json_atomic(paths["state_path"], state)

    # Append to log (JSONL)
    turn_id = "turn_" + uuid.uuid4().hex[:12]
    entry = {
        "ts_utc": utcnow_z(),
        "campaign_id": req.campaign_id,
        "turn_id": turn_id,
        "speaker": req.speaker,
        "input_text": req.input_text,
        "rolls": req.rolls,
        "rulings": req.rulings,
        "state_patch": req.state_patch,
        "output_summary": req.output_summary,
        "prev_state_hash": prev_hash,
        "new_state_hash": new_hash,
    }

    with locked_open(paths["log_path"], "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()

    # Return updated state snapshot
    state = load_json(paths["state_path"], default_state)
    return {"campaign_id": req.campaign_id, "turn_id": turn_id, "state": state}


@app.get("/log/find", response_model=LogFindResp)
def log_find(
    campaign_id: str,
    q: str,
    n: int = 200,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)

    entries = find_in_log(paths["log_path"], q, n)
    return {
        "campaign_id": campaign_id,
        "q": q,
        "n": min(max(int(n), 1), 500),
        "matches": len(entries),
        "entries": entries,
    }


@app.get("/snapshot", response_model=SnapshotResp)
def snapshot(
    campaign_id: str,
    n_hashes: int = 20,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(campaign_id)

    paths = ensure_campaign_dirs(campaign_id)

    # State
    default_state = {
        "schema": "dnd5e.campaign_state.v1",
        "campaign_id": campaign_id,
        "scene": {
            "scene_id": "scene_001",
            "location": "Unknown",
            "light": "dim",
            "threat": "low",
            "objectives": [],
            "effects": [],
        },
        "time": {"in_world": "Unknown", "round": 0, "turn_index": 0},
        "initiative": {"active": False, "order": [], "current": 0},
        "pcs": {},
        "npcs": {},
        "flags": {"revealed": [], "consequences": []},
        "meta": {"updated_utc": utcnow_z()},
    }
    state_obj = safe_read_json(paths["state_path"], default_state)

    # Lists
    pc_ids = list_ids_in_dir(paths["pc_dir"], "pc")
    npc_ids = list_ids_in_dir(paths["npc_dir"], "npc")

    # Recent hashes from log
    hashes = recent_hashes_from_log(paths["log_path"], n_hashes)

    return {
        "campaign_id": campaign_id,
        "state": state_obj,
        "pc_ids": pc_ids,
        "npc_ids": npc_ids,
        "recent_hashes": hashes,
    }


@app.get("/ruling/find", response_model=RulingFindResp)
def ruling_find(
    campaign_id: str,
    key: str,
    scene_id: Optional[str] = None,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    campaign_id = norm_campaign_id(campaign_id)

    root = campaign_root(campaign_id)
    if not root.exists():
        raise HTTPException(status_code=404, detail="Campaign not found")

    paths = ensure_campaign_dirs(campaign_id)

    ruling = find_ruling(paths["rulings_path"], scene_id, key)

    if not ruling:
        return {
            "campaign_id": campaign_id,
            "scene_id": scene_id,
            "key": key,
            "found": False,
            "ruling": None,
        }

    return {
        "campaign_id": campaign_id,
        "scene_id": ruling.get("scene_id"),
        "key": key,
        "found": True,
        "ruling": ruling,
    }


@app.post("/ruling", response_model=RulingResp)
def post_ruling(
    req: RulingReq,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)
    req.campaign_id = norm_campaign_id(req.campaign_id)

    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    paths = ensure_campaign_dirs(req.campaign_id)

    entry = {
        "ts_utc": utcnow_z(),
        "campaign_id": req.campaign_id,
        "scene_id": req.scene_id,
        "type": req.type,
        "key": req.key,
        "value": req.value,
        "notes": req.notes,
    }

    with locked_open(paths["rulings_path"], "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()

    return {
        "campaign_id": req.campaign_id,
        "scene_id": req.scene_id,
        "key": req.key,
        "stored": True,
        "ts_utc": entry["ts_utc"],
    }