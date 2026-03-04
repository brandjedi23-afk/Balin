import os
import json
import re
import uuid
import random
import datetime
import hashlib
import fcntl
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Union, cast, Tuple

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, ConfigDict

from systems.registry import get_pack as registry_get_pack
import systems  # dispara el registro (register(...) en systems/__init__.py)

# =========================
# ENV / PATHS
# =========================

SERVICE_NAME = os.getenv("SERVICE_NAME", "dnd5e-dm-compact")

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CAMPAIGNS_DIR = Path(os.getenv("CAMPAIGNS_DIR", str(DATA_DIR / "campaigns")))

DEFAULT_SYSTEM_ID = os.getenv("DEFAULT_SYSTEM_ID", "dnd5e")  # dnd5e / wot / greyhawk
DM_API_TOKEN = os.getenv("DM_API_TOKEN", "")

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
    payload = json.dumps(
        state,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def require_api_key(x_api_key: Optional[str]) -> None:
    if not DM_API_TOKEN:
        # Allow running locally without token (optional)
        return
    if not x_api_key or x_api_key != DM_API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-API-KEY)")


def lock_for(path: Path) -> Path:
    """
    Single consistent lock naming for ALL paths:
      state.json      -> state.json.lock
      log.jsonl       -> log.jsonl.lock
      rulings.jsonl   -> rulings.jsonl.lock
    """
    return path.with_suffix(path.suffix + ".lock")


def locked_open(path: Path, mode: str, shared: bool = False):
    """
    Lock file using fcntl.
    shared=True  -> LOCK_SH (lectura)
    shared=False -> LOCK_EX (escritura)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, mode, encoding="utf-8")
    lock_type = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
    fcntl.flock(f.fileno(), lock_type)
    return f


ID_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,63}$")


def norm_id(s: Optional[str]) -> str:
    if s is None:
        return ""
    v = s.strip().lower()
    if not v:
        return ""
    if not ID_RE.match(v):
        raise HTTPException(status_code=400, detail="Invalid id (allowed: a-z0-9 _ -; max 64)")
    return v


def norm_campaign_id(campaign_id: str) -> str:
    cid = norm_id(campaign_id)
    if not cid:
        raise HTTPException(status_code=400, detail="campaign_id is required")
    return cid


def norm_pc_id(pc_id: str) -> str:
    pid = norm_id(pc_id)
    if not pid:
        raise HTTPException(status_code=400, detail="pc_id is required")
    return pid


def norm_npc_id(npc_id: str) -> str:
    nid = norm_id(npc_id)
    if not nid:
        raise HTTPException(status_code=400, detail="npc_id is required")
    return nid


def norm_system_id(s: str) -> str:
    sid = norm_id(s)
    if not sid:
        raise HTTPException(status_code=400, detail="system_id is required")
    return sid


def campaign_root(campaign_id: str) -> Path:
    # norm_campaign_id está definido más abajo, esto está OK
    return CAMPAIGNS_DIR / norm_campaign_id(campaign_id)


def ensure_campaign_dirs(campaign_id: str) -> Dict[str, Path]:
    root = campaign_root(campaign_id)
    pc = root / PC_DIR
    npc = root / NPC_DIR
    state_dir = root / Path(STATE_FILE).parent
    log_dir = root / Path(LOG_FILE).parent
    rulings_dir = root / Path(RULINGS_FILE).parent

    for p in (pc, npc, state_dir, log_dir, rulings_dir):
        p.mkdir(parents=True, exist_ok=True)

    return {
        "root": root,
        "pc_dir": pc,
        "npc_dir": npc,
        "state_path": root / STATE_FILE,
        "log_path": root / LOG_FILE,
        "rulings_path": root / RULINGS_FILE,
    }


def ensure_meta_defaults(state: Dict[str, Any]) -> None:
    meta = state.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        state["meta"] = meta

    # NO setear system_id aquí (opción A): lo decide el pack real (o el one-time set en /turn)
    meta.setdefault("updated_utc", utcnow_z())


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
    Read JSON under a shared lock to avoid reading during an active write.
    (Writers must use an exclusive lock on the same lockfile.)
    """
    lock_path = lock_for(path)
    with locked_open(lock_path, "a", shared=True) as _lock:
        return load_json(path, default)


def safe_write_json(path: Path, obj: Any) -> None:
    """
    Write JSON under an exclusive lock + atomic replace.
    """
    lock_path = lock_for(path)
    with locked_open(lock_path, "a") as _lock:
        save_json_atomic(path, obj)


def iter_jsonl_reverse(path: Path, limit: int = 5000) -> list[Dict[str, Any]]:
    """
    Safe-ish read of last N lines (locks using lock_for(path) to coordinate with writers).
    """
    if not path.exists():
        return []
    lock_path = lock_for(path)
    with locked_open(lock_path, "a", shared=True) as _lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return []
    lines = lines[-limit:]
    out: list[Dict[str, Any]] = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(cast(Dict[str, Any], json.loads(line)))
        except Exception:
            continue
    return out


def read_log_tail(log_path: Path, n: int) -> list[dict]:
    """
    Reads the last n JSONL entries from log_path safely.
    """
    if n < 1:
        n = 1
    if n > 500:
        n = 500

    if not log_path.exists():
        return []

    lock_path = lock_for(log_path)
    with locked_open(lock_path, "a", shared=True) as _lock:
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
            continue
    return out


def find_in_log(log_path: Path, q: str, n: int) -> list[dict]:
    """
    Naive JSONL search, newest->oldest, returns up to n entries.
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
    lock_path = lock_for(log_path)
    with locked_open(lock_path, "a", shared=True) as _lock:
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
        if q_low not in line.lower():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue

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

    lock_path = lock_for(log_path)
    with locked_open(lock_path, "a", shared=True) as _lock:
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

        if "prev_state_hash" in obj and "new_state_hash" in obj:
            out.append(
                {
                    "turn_id": obj.get("turn_id"),
                    "ts_utc": obj.get("ts_utc"),
                    "prev_state_hash": obj.get("prev_state_hash"),
                    "new_state_hash": obj.get("new_state_hash"),
                }
            )

    out.reverse()
    return out


def load_state_with_system(paths: Dict[str, Path], campaign_id: str, desired_system_id: str):
    desired_system_id = norm_system_id(desired_system_id)

    # 1) pack deseado (para campañas nuevas)
    desired_pack = registry_get_pack(desired_system_id)

    # 2) carga raw (o default del pack deseado)
    raw = load_json(paths["state_path"], desired_pack.default_state(campaign_id))

    # 3) determinar system real (si raw ya lo trae)
    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    actual_system_id = (
        meta.get("system_id")
        if isinstance(meta.get("system_id"), str) and meta.get("system_id").strip()
        else desired_system_id
    )
    actual_system_id = norm_system_id(actual_system_id)

    pack = registry_get_pack(actual_system_id)

    # 4) migrar + normalizar
    migrated = pack.migrate_state(raw)
    migrated = pack.post_load_normalize(migrated)

    return pack, raw, migrated


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
    path = pc_path(campaign_id, pc_id)
    if not path.exists():
        return None
    sheet = safe_read_json(path, default={})
    if not isinstance(sheet, dict):
        return None
    return _extract_max_hp_from_sheet(sheet)


def get_npc_max_hp(campaign_id: str, npc_id: str) -> Optional[int]:
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
    # PCs
    pcs = state.get("pcs")
    if isinstance(pcs, dict):
        for pc_id, pc_state in pcs.items():
            if not isinstance(pc_state, dict):
                continue
            hp = pc_state.get("hp")
            if not isinstance(hp, dict):
                continue

            max_hp = get_pc_max_hp(campaign_id, str(pc_id))
            if max_hp is None:
                max_hp = _as_int(hp.get("max") or hp.get("max_hp"), None)

            new_cur = clamp_hp_value(hp.get("current"), max_hp)
            if new_cur is not None:
                hp["current"] = new_cur

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
    if not dir_path.exists():
        return []
    ids: list[str] = []
    for p in dir_path.glob(f"{prefix}_*.json"):
        stem = p.stem  # e.g. "pc_mira"
        if stem.startswith(prefix + "_"):
            ids.append(stem[len(prefix) + 1 :])
    ids.sort()
    return ids


PROTECTED_ROOT_KEYS = {"schema", "campaign_id", "meta"}

# ---------
# DOT-PATH PATCHING
# ---------

PatchValue = Union[Any, Dict[str, Any]]  # {"op": "inc"/"append"/"set", ...}


def _get_parent_and_key(root: Dict[str, Any], path: str) -> Tuple[Dict[str, Any], str]:
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
    for path, v in patch.items():
        parent, key = _get_parent_and_key(state, path)

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
            parent[key] = v
    return state


def validate_state_patch(patch: Dict[str, Any]) -> None:
    if not isinstance(patch, dict):
        raise ValueError("state_patch must be an object")

    for path in patch.keys():
        if path == "meta":
            raise ValueError("Modification of 'meta' is not allowed")
        if path.startswith("meta.") and path != "meta.system_id":
            raise ValueError("Modification of 'meta.*' is not allowed (except meta.system_id)")

        if path == "time.turn_index":
            raise ValueError("Modification of 'time.turn_index' is not allowed")

        root_key = path.split(".")[0]
        if root_key in PROTECTED_ROOT_KEYS and path != "meta.system_id":
            raise ValueError(f"Modification of '{root_key}' is not allowed")

        if path in ("pcs", "npcs", "initiative"):
            raise ValueError(f"Overwriting '{path}' root object is not allowed")


def ensure_time_and_inc_turn_index(state: Dict[str, Any]) -> None:
    time_obj = state.get("time")
    if not isinstance(time_obj, dict):
        time_obj = {}
        state["time"] = time_obj

    cur = time_obj.get("turn_index", 0)
    if not isinstance(cur, int):
        cur = 0
    time_obj["turn_index"] = cur + 1

    if "round" not in time_obj or not isinstance(time_obj.get("round"), int):
        time_obj["round"] = 0


# ---------
# DICE ROLLER
# ---------

DICE_RE = re.compile(r"^\s*(\d*)d(\d+)\s*([+-]\s*\d+)?\s*$", re.IGNORECASE)


def roll_expr(expr: str) -> Dict[str, Any]:
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
    rolls: list[Dict[str, Any]] = Field(default_factory=list)
    rulings: list[str] = Field(default_factory=list)
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
    scope: Literal["scene", "global"] = "scene"
    type: str
    key: str
    value: Any
    notes: Optional[str] = None


class RulingResp(BaseModel):
    campaign_id: str
    scene_id: Optional[str] = None
    scope: Literal["scene", "global"]
    key: str
    stored: bool
    ts_utc: str


class RulingFindResp(BaseModel):
    campaign_id: str
    scene_id: Optional[str] = None
    scope: Literal["scene", "global"]
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
    # Permite que cada SystemPack añada claves propias sin romper el response_model
    model_config = ConfigDict(extra="allow")

    # “tolerante”: si un pack no lo garantiza, no revienta
    schema: Optional[str] = None
    campaign_id: Optional[str] = None

    scene: Dict[str, Any] = Field(default_factory=dict)
    time: Dict[str, Any] = Field(default_factory=dict)
    initiative: Dict[str, Any] = Field(default_factory=dict)
    pcs: Dict[str, Any] = Field(default_factory=dict)
    npcs: Dict[str, Any] = Field(default_factory=dict)
    flags: Dict[str, Any] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)


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

        log_lock = lock_for(paths["log_path"])
        with locked_open(log_lock, "w") as _lock:
            with open(paths["log_path"], "a", encoding="utf-8") as f:
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

    pack, _raw, migrated = load_state_with_system(paths, campaign_id, DEFAULT_SYSTEM_ID)

    ensure_meta_defaults(migrated)
    migrated.setdefault("meta", {})
    migrated["meta"].setdefault("system_id", pack.system_id)

    return migrated


@app.post("/ruling", response_model=RulingResp)
def post_ruling(req: RulingReq, x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY")):
    require_api_key(x_api_key)

    cid = norm_campaign_id(req.campaign_id)
    paths = ensure_campaign_dirs(cid)

    scene_id = norm_id(req.scene_id) if req.scene_id else None
    scope = req.scope
    if scope == "global":
        scene_id = None

    entry = {
        "ts_utc": utcnow_z(),
        "campaign_id": cid,
        "scene_id": scene_id,
        "scope": scope,
        "type": req.type,
        "key": req.key.strip(),
        "value": req.value,
        "notes": req.notes,
    }

    rulings_lock = lock_for(paths["rulings_path"])
    with locked_open(rulings_lock, "w") as _lock:
        with open(paths["rulings_path"], "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()

    return {"campaign_id": cid, "scene_id": scene_id, "scope": scope, "key": entry["key"], "stored": True, "ts_utc": entry["ts_utc"]}


@app.get("/ruling/find", response_model=RulingFindResp)
def ruling_find(
    campaign_id: str,
    key: str,
    scene_id: Optional[str] = None,
    scope: Literal["scene", "global"] = "scene",
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)

    cid = norm_campaign_id(campaign_id)
    paths = ensure_campaign_dirs(cid)
    key = key.strip()
    wanted_scene = norm_id(scene_id) if scene_id else None

    entries = iter_jsonl_reverse(paths["rulings_path"], limit=5000)

    if scope == "scene" and wanted_scene:
        for e in entries:
            if e.get("key") == key and e.get("scope") == "scene" and e.get("scene_id") == wanted_scene:
                return {"campaign_id": cid, "scene_id": wanted_scene, "scope": "scene", "key": key, "found": True, "ruling": e}

        for e in entries:
            if e.get("key") == key and e.get("scope") == "global":
                return {"campaign_id": cid, "scene_id": None, "scope": "global", "key": key, "found": True, "ruling": e}

        return {"campaign_id": cid, "scene_id": wanted_scene, "scope": "scene", "key": key, "found": False, "ruling": None}

    for e in entries:
        if e.get("key") == key and e.get("scope") == "global":
            return {"campaign_id": cid, "scene_id": None, "scope": "global", "key": key, "found": True, "ruling": e}

    return {"campaign_id": cid, "scene_id": None, "scope": "global", "key": key, "found": False, "ruling": None}


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

    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    req.sheet.setdefault("campaign_id", req.campaign_id)
    req.sheet.setdefault("pc_id", req.pc_id)

    path = pc_path(req.campaign_id, req.pc_id)
    safe_write_json(path, req.sheet)

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
    log_lock = lock_for(paths["log_path"])
    with locked_open(log_lock, "w") as _lock:
        with open(paths["log_path"], "a", encoding="utf-8") as f:
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

    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    req.sheet.setdefault("campaign_id", req.campaign_id)
    req.sheet.setdefault("npc_id", req.npc_id)

    path = npc_path(req.campaign_id, req.npc_id)
    safe_write_json(path, req.sheet)

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
    log_lock = lock_for(paths["log_path"])
    with locked_open(log_lock, "w") as _lock:
        with open(paths["log_path"], "a", encoding="utf-8") as f:
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

    root = campaign_root(req.campaign_id)
    if not root.exists():
        if not AUTO_CREATE_CAMPAIGN:
            raise HTTPException(status_code=404, detail="Campaign not found")
        ensure_campaign_dirs(req.campaign_id)

    paths = ensure_campaign_dirs(req.campaign_id)

    # Lock state while patching (CONSISTENT lock file)
    state_lock = lock_for(paths["state_path"])

    def _extract_desired_system_id(patch: Any) -> Optional[str]:
        if not isinstance(patch, dict):
            return None
        if "meta.system_id" not in patch:
            return None

        desired = patch.get("meta.system_id")
        if isinstance(desired, dict) and desired.get("op") == "set":
            desired = desired.get("value")

        if isinstance(desired, str) and desired.strip():
            return norm_system_id(desired)

        return None

    with locked_open(state_lock, "w") as _lock:
        desired_from_patch = _extract_desired_system_id(req.state_patch)
        desired_system_id = desired_from_patch or DEFAULT_SYSTEM_ID

        pack, raw_state, state_obj = load_state_with_system(paths, req.campaign_id, desired_system_id)

        state_obj.setdefault("meta", {})
        if not isinstance(state_obj["meta"], dict):
            state_obj["meta"] = {}
        state_obj["meta"]["updated_utc"] = utcnow_z()

        # One-time system_id set
        if isinstance(req.state_patch, dict) and "meta.system_id" in req.state_patch:
            raw_meta = raw_state.get("meta") if isinstance(raw_state.get("meta"), dict) else {}
            current_raw = raw_meta.get("system_id")

            if isinstance(current_raw, str) and current_raw.strip():
                req.state_patch.pop("meta.system_id", None)
            else:
                if desired_from_patch is None:
                    raise HTTPException(status_code=400, detail="meta.system_id must be a non-empty string")

                state_obj["meta"]["system_id"] = desired_from_patch

                pack = registry_get_pack(desired_from_patch)
                state_obj = pack.migrate_state(state_obj)
                state_obj = pack.post_load_normalize(state_obj)

                req.state_patch.pop("meta.system_id", None)

        state_obj.setdefault("meta", {})
        if not isinstance(state_obj["meta"], dict):
            state_obj["meta"] = {}
        state_obj["meta"].setdefault("system_id", pack.system_id)
        state_obj["meta"]["updated_utc"] = utcnow_z()

        prev_hash = state_hash(state_obj)

        if isinstance(req.state_patch, dict) and "time.turn_index" in req.state_patch:
            req.state_patch.pop("time.turn_index", None)

        if req.state_patch:
            try:
                validate_state_patch(req.state_patch)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid state_patch: {e}")

            try:
                state_obj = apply_patch(state_obj, cast(Dict[str, Any], req.state_patch))
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid state_patch: {e}")

        ensure_time_and_inc_turn_index(state_obj)
        enforce_hp_clamps(req.campaign_id, state_obj)

        state_obj.setdefault("meta", {})
        if not isinstance(state_obj["meta"], dict):
            state_obj["meta"] = {}
        state_obj["meta"].setdefault("system_id", pack.system_id)
        state_obj["meta"]["updated_utc"] = utcnow_z()

        new_hash = state_hash(state_obj)

        save_json_atomic(paths["state_path"], state_obj)

    # Append to log AFTER releasing state lock (lock sidecar consistently)
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
        "system_id": pack.system_id,
        "schema": state_obj.get("schema"),
    }

    log_lock = lock_for(paths["log_path"])
    with locked_open(log_lock, "w") as _lock:
        with open(paths["log_path"], "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()

    final_pack = registry_get_pack(pack.system_id)
    final_default = final_pack.default_state(req.campaign_id)
    state_out = safe_read_json(paths["state_path"], final_default)

    return {"campaign_id": req.campaign_id, "turn_id": turn_id, "state": state_out}


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

    pack, _raw, migrated = load_state_with_system(paths, campaign_id, DEFAULT_SYSTEM_ID)
    ensure_meta_defaults(migrated)
    migrated.setdefault("meta", {})
    migrated["meta"].setdefault("system_id", pack.system_id)

    pc_ids = list_ids_in_dir(paths["pc_dir"], "pc")
    npc_ids = list_ids_in_dir(paths["npc_dir"], "npc")
    hashes = recent_hashes_from_log(paths["log_path"], n_hashes)

    return {
        "campaign_id": campaign_id,
        "state": migrated,
        "pc_ids": pc_ids,
        "npc_ids": npc_ids,
        "recent_hashes": hashes,
    }