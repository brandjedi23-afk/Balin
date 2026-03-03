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

app = FastAPI(title="D&D 5e DM Compact API", version="1.0.0")


# =========================
# HELPERS
# =========================

def utcnow_z() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def require_api_key(x_api_key: Optional[str]) -> None:
    if not DM_API_TOKEN:
        # Allow running locally without token (optional)
        return
    if not x_api_key or x_api_key != DM_API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-API-KEY)")


def campaign_root(campaign_id: str) -> Path:
    return CAMPAIGNS_DIR / campaign_id


def ensure_campaign_dirs(campaign_id: str) -> Dict[str, Path]:
    root = campaign_root(campaign_id)
    pc = root / PC_DIR
    npc = root / NPC_DIR
    state = root / Path(STATE_FILE).parent
    log = root / Path(LOG_FILE).parent

    for p in (pc, npc, state, log):
        p.mkdir(parents=True, exist_ok=True)

    state_path = root / STATE_FILE
    log_path = root / LOG_FILE
    return {
        "root": root,
        "pc_dir": pc,
        "npc_dir": npc,
        "state_path": state_path,
        "log_path": log_path,
    }


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


@app.get("/pc")
def get_pc(
    campaign_id: str,
    pc_id: str,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
):
    require_api_key(x_api_key)

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
    # (We lock via separate lock file to avoid holding lock on json while atomic replace)
    lock_path = paths["state_path"].with_suffix(".lock")

    with locked_open(lock_path, "w") as _lock:
        state = load_json(paths["state_path"], default_state)

        # Apply patch if any
        if req.state_patch:
            try:
                state = apply_patch(state, req.state_patch)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid state_patch: {e}")

        # Always bump updated time; optional auto-increment turn_index if you want
        state.setdefault("meta", {})
        state["meta"]["updated_utc"] = utcnow_z()

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
    }

    with locked_open(paths["log_path"], "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()

    # Return updated state snapshot
    state = load_json(paths["state_path"], default_state)
    return {"campaign_id": req.campaign_id, "turn_id": turn_id, "state": state}