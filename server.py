from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import fcntl
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from systems.registry import get_pack as registry_get_pack

# =========================
# Config
# =========================
SERVICE_NAME = os.getenv("SERVICE_NAME", "Balin")

# IMPORTANT: mount your Railway volume on /data (recommended)
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CAMPAIGNS_DIR = DATA_DIR / "campaigns"
DEFAULT_SYSTEM = os.getenv("DEFAULT_SYSTEM", "dnd5e")  # o "greyhawk"

API_KEY_ENV = os.getenv("DM_API_TOKEN") or os.getenv("API_KEY") or ""
API_KEY_HEADER_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)

DICE_RE = re.compile(r"^\s*(\d+)\s*d\s*(\d+)\s*([+-]\s*\d+)?\s*$", re.IGNORECASE)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sanitize_id(s: str) -> str:
    # keep it filesystem safe
    safe = "".join(c for c in s if c.isalnum() or c in ("-", "_"))
    return safe or "default"


def get_campaign_system_id(campaign_id: str, state: Optional[Dict[str, Any]] = None) -> str:
    # 1) si el state ya tiene meta.system_id, úsalo
    if state:
        meta = state.get("meta") or {}
        sid = meta.get("system_id")
        if isinstance(sid, str) and sid.strip():
            return sid.strip()
    # 2) fallback a DEFAULT_SYSTEM
    return DEFAULT_SYSTEM


def try_pack(system_id: str):
    # Nunca tumbar el server por un pack incompleto.
    try:
        return registry_get_pack(system_id)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    return None, "Unknown pack error"


# =========================
# File locking + atomic IO
# =========================
@dataclass
class LockedFile:
    fp: Any


def locked_open(path: Path, mode: str, shared: bool = False) -> LockedFile:
    path.parent.mkdir(parents=True, exist_ok=True)
    fp = open(path, mode, encoding="utf-8")
    lock_type = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
    fcntl.flock(fp.fileno(), lock_type)
    return LockedFile(fp=fp)


def locked_close(lf: LockedFile) -> None:
    try:
        fcntl.flock(lf.fp.fileno(), fcntl.LOCK_UN)
    finally:
        lf.fp.close()


def json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return default
            return json.loads(raw)
    except json.JSONDecodeError:
        return default


def json_save_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def safe_read_json(path: Path, default: Any) -> Any:
    lock = path.with_suffix(path.suffix + ".lock")
    lf = locked_open(lock, "a+", shared=True)
    try:
        return json_load(path, default)
    finally:
        locked_close(lf)


def safe_write_json(path: Path, obj: Any) -> None:
    lock = path.with_suffix(path.suffix + ".lock")
    lf = locked_open(lock, "a+", shared=False)
    try:
        json_save_atomic(path, obj)
    finally:
        locked_close(lf)


# =========================
# Models (Pydantic v2 friendly)
# Avoid warning: "schema" shadows BaseModel.schema
# Use schema_ as attribute but serialize as "schema"
# =========================
class HealthResp(BaseModel):
    ok: bool
    service: str
    ts_utc: str


class RollReq(BaseModel):
    expr: str
    label: Optional[str] = None
    campaign_id: Optional[str] = None


class RollResp(BaseModel):
    roll_id: str
    total: int
    detail: str
    raw: List[int]
    text: str
    expr: str
    label: Optional[str] = None
    ts_utc: str


class SceneState(BaseModel):
    scene_id: str
    location: str
    light: str
    threat: str
    objectives: List[str]
    effects: List[str]


class TimeState(BaseModel):
    in_world: str
    round: int
    turn_index: int


class InitEntry(BaseModel):
    kind: Literal["pc", "npc"]
    id: str
    init: int


class InitiativeState(BaseModel):
    active: bool
    order: List[InitEntry]
    current: int


class FlagsState(BaseModel):
    revealed: List[str]
    consequences: List[str]


class MetaState(BaseModel):
    updated_utc: str


class CampaignState(BaseModel):
    schema_: str = Field(alias="schema")
    campaign_id: str
    scene: SceneState
    time: TimeState
    initiative: InitiativeState
    pcs: Dict[str, Any]
    npcs: Dict[str, Any]
    flags: FlagsState
    meta: MetaState

    model_config = {"populate_by_name": True}


class TurnReq(BaseModel):
    campaign_id: str
    speaker: Literal["player", "dm"]
    input_text: str
    rolls: List[Dict[str, Any]] = Field(default_factory=list)
    rulings: List[str] = Field(default_factory=list)
    state_patch: Dict[str, Any] = Field(default_factory=dict)
    output_summary: str = ""


class TurnResp(BaseModel):
    campaign_id: str
    turn_id: str
    state: CampaignState


class RulingReq(BaseModel):
    campaign_id: str
    scene_id: Optional[str] = None
    scope: Literal["scene", "global"]
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


class UpsertPCReq(BaseModel):
    campaign_id: str
    pc_id: str
    sheet: Dict[str, Any]


class UpsertNPCReq(BaseModel):
    campaign_id: str
    npc_id: str
    sheet: Dict[str, Any]


class UpsertResp(BaseModel):
    ok: bool
    campaign_id: str
    pc_id: Optional[str] = None
    npc_id: Optional[str] = None


class ListResp(BaseModel):
    campaign_id: str
    ids: List[str]


class DeleteCampaignResp(BaseModel):
    ok: bool
    campaign_id: str
    deleted: bool
    root: str

class LogTailResp(BaseModel):
    campaign_id: str
    n: int
    entries: List[Dict[str, Any]]


class LogFindResp(BaseModel):
    campaign_id: str
    q: str
    n: int
    matches: int
    entries: List[Dict[str, Any]]


class SnapshotResp(BaseModel):
    campaign_id: str
    state: CampaignState
    pc_ids: List[str]
    npc_ids: List[str]
    recent_hashes: List[Dict[str, Any]]


# =========================
# Auth
# =========================
def require_api_key(key: Optional[str] = Depends(api_key_header)) -> None:
    # If no key configured server-side, allow (useful for dev)
    if not API_KEY_ENV:
        return
    if not key or key != API_KEY_ENV:
        raise HTTPException(status_code=401, detail="Unauthorized (missing/invalid X-API-KEY)")


# =========================
# Campaign paths + defaults
# =========================
def campaign_root(campaign_id: str) -> Path:
    return CAMPAIGNS_DIR / sanitize_id(campaign_id)


def state_file(campaign_id: str) -> Path:
    # one canonical place for state (avoid duplicate paths!)
    return campaign_root(campaign_id) / "state" / "state.json"


def pcs_dir(campaign_id: str) -> Path:
    return campaign_root(campaign_id) / "pcs"


def npcs_dir(campaign_id: str) -> Path:
    return campaign_root(campaign_id) / "npcs"


def rulings_file(campaign_id: str) -> Path:
    return campaign_root(campaign_id) / "rulings.jsonl"


def log_file(campaign_id: str) -> Path:
    return campaign_root(campaign_id) / "log.jsonl"


def ensure_campaign_dirs(campaign_id: str) -> None:
    root = campaign_root(campaign_id)
    (root / "state").mkdir(parents=True, exist_ok=True)
    pcs_dir(campaign_id).mkdir(parents=True, exist_ok=True)
    npcs_dir(campaign_id).mkdir(parents=True, exist_ok=True)


def default_state(campaign_id: str, system_id: Optional[str] = None) -> Dict[str, Any]:
    now = utcnow_iso()
    system_id = (system_id or DEFAULT_SYSTEM).strip()

    return {
        "schema": "balin.state.v1",
        "campaign_id": campaign_id,
        "scene": {
            "scene_id": "start",
            "location": "Unknown",
            "light": "normal",
            "threat": "low",
            "objectives": [],
            "effects": [],
        },
        "time": {"in_world": now, "round": 0, "turn_index": 0},
        "initiative": {"active": False, "order": [], "current": 0},
        "pcs": {},
        "npcs": {},
        "flags": {"revealed": [], "consequences": []},
        "meta": {
            "updated_utc": now,
            "system_id": system_id,
        },
    }


def load_state_or_create(campaign_id: str) -> Dict[str, Any]:
    ensure_campaign_dirs(campaign_id)
    path = state_file(campaign_id)

    existing = safe_read_json(path, default=None)
    if not isinstance(existing, dict):
        existing = {}

    # 1) system_id: del state si existe; si no, DEFAULT_SYSTEM
    meta = existing.get("meta") or {}
    system_id = (meta.get("system_id") or DEFAULT_SYSTEM).strip().lower()

    # 2) intenta cargar pack sin romper el server
    pack = None
    pack_err = None
    try:
        pack = registry_get_pack(system_id)
    except Exception as e:
        pack_err = f"{type(e).__name__}: {e}"
        pack = None

    # 3) base_state: del pack si existe, o default global
    base = default_state(campaign_id, system_id)  # tu default global (válido)
    if pack is not None and hasattr(pack, "default_state") and callable(pack.default_state):
        try:
            pack_base = pack.default_state(campaign_id)
            if isinstance(pack_base, dict):
                base = {**base, **pack_base}  # pack puede sobreescribir campos
        except Exception as e:
            existing.setdefault("meta", {})
            existing["meta"]["pack_warning"] = f"default_state failed: {type(e).__name__}: {e}"

    # 4) merge + self-heal (evita 500 por schema incompleto)
    merged = {**base, **existing}
    for k in ("scene", "time", "initiative", "flags", "meta"):
        merged[k] = {**(base.get(k) or {}), **(merged.get(k) or {})}

    merged["schema"] = merged.get("schema") or base["schema"]
    merged["campaign_id"] = campaign_id
    merged["meta"]["system_id"] = system_id
    merged["meta"]["updated_utc"] = utcnow_iso()

    # 5) ensure_campaign del pack (opcional, nunca romper)
    if pack is not None and hasattr(pack, "ensure_campaign") and callable(pack.ensure_campaign):
        try:
            pack.ensure_campaign(campaign_root(campaign_id), campaign_id)
        except Exception as e:
            merged["meta"]["pack_warning"] = f"ensure_campaign failed: {type(e).__name__}: {e}"

    # 6) si el pack no cargó, deja warning
    if pack is None and pack_err:
        merged["meta"]["pack_warning"] = f"Pack not available for '{system_id}': {pack_err}"

    safe_write_json(path, merged)
    return merged


def save_state(campaign_id: str, state: Dict[str, Any]) -> None:
    state["campaign_id"] = campaign_id
    state["meta"] = state.get("meta") or {}
    state["meta"]["updated_utc"] = utcnow_iso()
    safe_write_json(state_file(campaign_id), state)


# =========================
# Patch helper (dot-path)
# =========================
def set_dot(obj: Dict[str, Any], path: str, value: Any) -> None:
    cur = obj
    parts = path.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def apply_patch(state: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """
    Supports:
      - direct assignment: {"scene.location": "Menhir circle"}
      - op dict: {"time.turn_index": {"op":"inc","by": 1}}
      - append: {"flags.revealed": {"op":"append","value":"Tripwire found"}}
    """
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and "op" in v:
            op = v.get("op")
            if op == "inc":
                by = int(v.get("by", 1))
                # read current
                cur = state
                parts = k.split(".")
                for p in parts[:-1]:
                    cur = cur.setdefault(p, {})
                leaf = parts[-1]
                cur[leaf] = int(cur.get(leaf, 0)) + by
            elif op == "append":
                val = v.get("value")
                cur = state
                parts = k.split(".")
                for p in parts[:-1]:
                    cur = cur.setdefault(p, {})
                leaf = parts[-1]
                if leaf not in cur or not isinstance(cur[leaf], list):
                    cur[leaf] = []
                cur[leaf].append(val)
            else:
                # unknown op -> treat as assignment to keep robust
                set_dot(state, k, v)
        else:
            set_dot(state, k, v)
    return state


# =========================
# Dice
# =========================
def roll_dice(expr: str) -> Tuple[int, List[int], str]:
    m = DICE_RE.match(expr or "")
    if not m:
        raise HTTPException(status_code=400, detail='Unsupported dice expression. Use NdM+K, e.g. 1d20+5')
    n = int(m.group(1))
    sides = int(m.group(2))
    mod = m.group(3)
    k = int(mod.replace(" ", "")) if mod else 0

    if n <= 0 or sides <= 0 or n > 200 or sides > 100000:
        raise HTTPException(status_code=400, detail="Dice limits exceeded")

    import random
    raw = [random.randint(1, sides) for _ in range(n)]
    total = sum(raw) + k
    detail = f"{n}d{sides}{k:+d} => {raw} {k:+d}"
    return total, raw, detail


# =========================
# JSONL log helpers
# =========================
def append_jsonl(path: Path, entry: Dict[str, Any]) -> None:
    lock = path.with_suffix(path.suffix + ".lock")
    lf = locked_open(lock, "a+", shared=False)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    finally:
        locked_close(lf)


def tail_jsonl(path: Path, n: int) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    # Simple (safe) tail: read all if file small; acceptable for early stage
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    out: List[Dict[str, Any]] = []
    for line in lines[-n:]:
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def find_jsonl(path: Path, q: str, n: int) -> Tuple[int, List[Dict[str, Any]]]:
    if not path.exists():
        return 0, []
    q_low = (q or "").lower()
    matches: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if q_low in line.lower():
                try:
                    matches.append(json.loads(line))
                except Exception:
                    continue
                if len(matches) >= n:
                    break
    return len(matches), matches


# =========================
# FastAPI app
# =========================
app = FastAPI(title=SERVICE_NAME, version="1.2.1")


# Make 500s visible while debugging (optional but useful)
@app.exception_handler(Exception)
async def unhandled_exc_handler(request: Request, exc: Exception):
    # Keep as text/plain by default? Better return JSON with detail:
    return JSONResponse(status_code=500, content={"detail": f"{type(exc).__name__}: {str(exc)}"})


# -------------------------
# Public endpoints
# -------------------------
@app.get("/health", response_model=HealthResp)
def health() -> HealthResp:
    return HealthResp(ok=True, service=SERVICE_NAME, ts_utc=utcnow_iso())


# -------------------------
# Protected endpoints
# -------------------------
@app.post("/roll", response_model=RollResp, dependencies=[Depends(require_api_key)])
def api_roll(req: RollReq) -> RollResp:
    total, raw, detail = roll_dice(req.expr)
    rid = uuid.uuid4().hex
    ts = utcnow_iso()
    # Optional: audit to campaign log if campaign_id provided
    if req.campaign_id:
        ensure_campaign_dirs(req.campaign_id)
        append_jsonl(
            log_file(req.campaign_id),
            {
                "kind": "roll",
                "roll_id": rid,
                "expr": req.expr,
                "label": req.label,
                "total": total,
                "raw": raw,
                "detail": detail,
                "ts_utc": ts,
            },
        )
    return RollResp(
        roll_id=rid,
        total=total,
        raw=raw,
        detail=detail,
        text=str(total),
        expr=req.expr,
        label=req.label,
        ts_utc=ts,
    )


@app.get("/state", response_model=CampaignState, dependencies=[Depends(require_api_key)])
def get_state(campaign_id: str = Query(...), system_id: Optional[str] = Query(None)) -> CampaignState:
    # si system_id viene, lo persistimos en meta.system_id
    if system_id:
        ensure_campaign_dirs(campaign_id)
        path = state_file(campaign_id)
        st = safe_read_json(path, default=None) or {}
        if not isinstance(st, dict):
            st = {}
        st.setdefault("meta", {})
        st["meta"]["system_id"] = system_id
        safe_write_json(path, st)

    st2 = load_state_or_create(campaign_id)
    return CampaignState.model_validate(st2)


@app.delete("/campaign", response_model=DeleteCampaignResp, dependencies=[Depends(require_api_key)])
def delete_campaign(campaign_id: str = Query(...)) -> DeleteCampaignResp:
    root = campaign_root(campaign_id)

    if not root.exists():
        return DeleteCampaignResp(
            ok=True,
            campaign_id=campaign_id,
            deleted=False,
            root=str(root),
        )

    # Borra completamente la carpeta de campaña:
    # state/, pcs/, npcs/, log.jsonl, rulings.jsonl, locks, etc.
    shutil.rmtree(root)

    return DeleteCampaignResp(
        ok=True,
        campaign_id=campaign_id,
        deleted=True,
        root=str(root),
    )


@app.post("/turn", response_model=TurnResp, dependencies=[Depends(require_api_key)])
def post_turn(req: TurnReq) -> TurnResp:
    st = load_state_or_create(req.campaign_id)
    # apply patch
    st = apply_patch(st, req.state_patch)

    # advance time bookkeeping (optional; keep deterministic)
    st["time"]["turn_index"] = int(st["time"].get("turn_index", 0)) + 1
    st["meta"]["updated_utc"] = utcnow_iso()
    save_state(req.campaign_id, st)

    turn_id = uuid.uuid4().hex
    append_jsonl(
        log_file(req.campaign_id),
        {
            "kind": "turn",
            "turn_id": turn_id,
            "speaker": req.speaker,
            "input_text": req.input_text,
            "rolls": req.rolls,
            "rulings": req.rulings,
            "state_patch": req.state_patch,
            "output_summary": req.output_summary,
            "ts_utc": utcnow_iso(),
        },
    )
    return TurnResp(campaign_id=req.campaign_id, turn_id=turn_id, state=CampaignState.model_validate(st))


@app.get("/pc", dependencies=[Depends(require_api_key)])
def get_pc(
    campaign_id: str = Query(...),
    pc_id: str = Query(...),
) -> Dict[str, Any]:
    ensure_campaign_dirs(campaign_id)
    p = pcs_dir(campaign_id) / f"{sanitize_id(pc_id)}.json"
    sheet = safe_read_json(p, default=None)
    if sheet is None:
        raise HTTPException(status_code=404, detail="PC not found")
    return sheet


@app.put("/pc", response_model=UpsertResp, dependencies=[Depends(require_api_key)])
def upsert_pc(req: UpsertPCReq) -> UpsertResp:
    ensure_campaign_dirs(req.campaign_id)
    p = pcs_dir(req.campaign_id) / f"{sanitize_id(req.pc_id)}.json"
    safe_write_json(p, req.sheet)
    return UpsertResp(ok=True, campaign_id=req.campaign_id, pc_id=req.pc_id)


@app.get("/npc", dependencies=[Depends(require_api_key)])
def get_npc(
    campaign_id: str = Query(...),
    npc_id: str = Query(...),
) -> Dict[str, Any]:
    ensure_campaign_dirs(campaign_id)
    p = npcs_dir(campaign_id) / f"{sanitize_id(npc_id)}.json"
    sheet = safe_read_json(p, default=None)
    if sheet is None:
        raise HTTPException(status_code=404, detail="NPC not found")
    return sheet


@app.put("/npc", response_model=UpsertResp, dependencies=[Depends(require_api_key)])
def upsert_npc(req: UpsertNPCReq) -> UpsertResp:
    ensure_campaign_dirs(req.campaign_id)
    p = npcs_dir(req.campaign_id) / f"{sanitize_id(req.npc_id)}.json"
    safe_write_json(p, req.sheet)
    return UpsertResp(ok=True, campaign_id=req.campaign_id, npc_id=req.npc_id)


@app.get("/pc/list", response_model=ListResp, dependencies=[Depends(require_api_key)])
def list_pcs(campaign_id: str = Query(...)) -> ListResp:
    ensure_campaign_dirs(campaign_id)
    ids = [p.stem for p in pcs_dir(campaign_id).glob("*.json")]
    return ListResp(campaign_id=campaign_id, ids=sorted(ids))


@app.get("/npc/list", response_model=ListResp, dependencies=[Depends(require_api_key)])
def list_npcs(campaign_id: str = Query(...)) -> ListResp:
    ensure_campaign_dirs(campaign_id)
    ids = [p.stem for p in npcs_dir(campaign_id).glob("*.json")]
    return ListResp(campaign_id=campaign_id, ids=sorted(ids))


@app.post("/ruling", response_model=RulingResp, dependencies=[Depends(require_api_key)])
def post_ruling(req: RulingReq) -> RulingResp:
    ensure_campaign_dirs(req.campaign_id)
    entry = {
        "campaign_id": req.campaign_id,
        "scene_id": req.scene_id,
        "scope": req.scope,
        "type": req.type,
        "key": req.key,
        "value": req.value,
        "notes": req.notes,
        "ts_utc": utcnow_iso(),
    }
    append_jsonl(rulings_file(req.campaign_id), entry)
    return RulingResp(
        campaign_id=req.campaign_id,
        scene_id=req.scene_id,
        scope=req.scope,
        key=req.key,
        stored=True,
        ts_utc=entry["ts_utc"],
    )


@app.get("/ruling/find", response_model=RulingFindResp, dependencies=[Depends(require_api_key)])
def ruling_find(
    campaign_id: str = Query(...),
    key: str = Query(...),
    scene_id: Optional[str] = Query(None),
    scope: Literal["scene", "global"] = Query("scene"),
) -> RulingFindResp:
    ensure_campaign_dirs(campaign_id)
    path = rulings_file(campaign_id)
    if not path.exists():
        return RulingFindResp(campaign_id=campaign_id, scene_id=scene_id, scope=scope, key=key, found=False, ruling=None)

    # scan from end (latest first)
    lines = tail_jsonl(path, 500)  # small bounded search
    for entry in reversed(lines):
        if entry.get("key") != key:
            continue
        if scope == "global":
            if entry.get("scope") == "global":
                return RulingFindResp(campaign_id=campaign_id, scene_id=None, scope="global", key=key, found=True, ruling=entry)
        else:
            # scene scope: try scene match then fallback to global
            if scene_id and entry.get("scope") == "scene" and entry.get("scene_id") == scene_id:
                return RulingFindResp(campaign_id=campaign_id, scene_id=scene_id, scope="scene", key=key, found=True, ruling=entry)
    # fallback to latest global
    if scope == "scene":
        for entry in reversed(lines):
            if entry.get("key") == key and entry.get("scope") == "global":
                return RulingFindResp(campaign_id=campaign_id, scene_id=scene_id, scope="scene", key=key, found=True, ruling=entry)

    return RulingFindResp(campaign_id=campaign_id, scene_id=scene_id, scope=scope, key=key, found=False, ruling=None)


@app.get("/log/tail", response_model=LogTailResp, dependencies=[Depends(require_api_key)])
def log_tail(campaign_id: str = Query(...), n: int = Query(50, ge=1, le=500)) -> LogTailResp:
    ensure_campaign_dirs(campaign_id)
    entries = tail_jsonl(log_file(campaign_id), n)
    return LogTailResp(campaign_id=campaign_id, n=n, entries=entries)


@app.get("/log/find", response_model=LogFindResp, dependencies=[Depends(require_api_key)])
def log_find(campaign_id: str = Query(...), q: str = Query(...), n: int = Query(200, ge=1, le=500)) -> LogFindResp:
    ensure_campaign_dirs(campaign_id)
    matches, entries = find_jsonl(log_file(campaign_id), q, n)
    return LogFindResp(campaign_id=campaign_id, q=q, n=n, matches=matches, entries=entries)


@app.get("/snapshot", response_model=SnapshotResp, dependencies=[Depends(require_api_key)])
def snapshot(campaign_id: str = Query(...), n_hashes: int = Query(20, ge=1, le=200)) -> SnapshotResp:
    st = load_state_or_create(campaign_id)
    pc_ids = [p.stem for p in pcs_dir(campaign_id).glob("*.json")]
    npc_ids = [p.stem for p in npcs_dir(campaign_id).glob("*.json")]

    # simple recent_hashes stub: you can replace with real hashing later
    recent_entries = tail_jsonl(log_file(campaign_id), n_hashes)
    recent_hashes = []
    for e in recent_entries:
        if e.get("kind") == "turn":
            recent_hashes.append(
                {
                    "turn_id": e.get("turn_id"),
                    "ts_utc": e.get("ts_utc"),
                    "prev_state_hash": "",
                    "new_state_hash": "",
                }
            )

    return SnapshotResp(
        campaign_id=campaign_id,
        state=CampaignState.model_validate(st),
        pc_ids=sorted(pc_ids),
        npc_ids=sorted(npc_ids),
        recent_hashes=recent_hashes,
    )