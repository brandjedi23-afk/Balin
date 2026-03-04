# Balin
D&D 5e DM Compact JSON Pack (v1)

WHAT THIS IS
- A stable, compact JSON structure for:
  - Player Character Sheets (permanent): pc_sheet.v1
  - NPC/Enemy minimum sheets (permanent): npc_sheet.v1
  - Temporary per-scene/combat state for PCs/NPCs: pc_state.v1 / npc_state.v1
  - Global campaign state (scene + initiative): campaign_state.v1
  - Append-only turn log entries: turn_log

DESIGN RULES
1) Permanent vs Temporary separation is strict:
   - Permanent changes (items, gold, level) go to *sheet*.
   - Temporary changes (damage, conditions, slots used) go to *state*.
2) No creature acts without a minimum sheet.
3) Never invent dice results; rolls are recorded in the log.

SUGGESTED STORAGE LAYOUT (Railway Volume)
/data/campaigns/<campaign_id>/
  pc/pc_<pc_id>.json
  npc/npc_<npc_id>.json
  state/state.json
  log/log.jsonl

FILES
- schemas/: JSON Schemas for validation
- examples/: Example objects you can copy/paste
