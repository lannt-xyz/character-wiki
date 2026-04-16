"""export_characters_map.py — Export characters map to Markdown for video pipeline.

Usage:
    uv run python3 export_characters_map.py
    uv run python3 export_characters_map.py --output data/my-map.md

Output: data/{story_slug}-characters.md  (default)
"""

import argparse
import json
import sys
from pathlib import Path

from loguru import logger

from config.settings import settings
from db.database import SQLiteDB


def _or(value, fallback: str = "—") -> str:
    if value is None:
        return fallback
    v = str(value).strip()
    return v if v else fallback


def _max_importance(snapshots: list[dict]) -> int:
    if not snapshots:
        return 0
    return max(s["visual_importance"] or 0 for s in snapshots)


def _render_character(rank: int, char: dict, snapshots: list[dict], relations: list[dict]) -> str:
    lines: list[str] = []

    char_id = char["character_id"]
    name = char["name"]
    aliases = json.loads(char["aliases_json"])
    traits = json.loads(char["traits_json"])
    visual_anchor = char["visual_anchor"]
    peak = _max_importance(snapshots)

    lines.append(f"## {rank}. {name} `[{char_id}]`")
    lines.append("")

    # Identity
    lines.append("### Identity")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| visual_importance | {peak}/10 |")
    lines.append(f"| visual_anchor | {_or(visual_anchor)} |")
    lines.append(f"| aliases | {', '.join(aliases) if aliases else '—'} |")
    lines.append(f"| traits | {', '.join(traits) if traits else '—'} |")
    lines.append("")

    # Snapshots
    lines.append("### Snapshots")
    lines.append("")
    lines.append("| Ch start | Ch end | Active | Level | Outfit | Weapon | VFX vibes | Physical state |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for s in snapshots:
        ch_end = str(s["chapter_end"]) if s["chapter_end"] is not None else "→"
        active = "✓" if s["is_active"] else "✗"
        lines.append(
            f"| {s['chapter_start']} | {ch_end} | {active}"
            f" | {_or(s['level'])} | {_or(s['outfit'])} | {_or(s['weapon'])}"
            f" | {_or(s['vfx_vibes'])} | {_or(s['physical_description'])} |"
        )
    lines.append("")

    # Relations
    if relations:
        lines.append("### Relations")
        lines.append("")
        lines.append("| Related | Description | From Ch |")
        lines.append("|---|---|---|")
        for r in relations:
            lines.append(f"| {_or(r['related_name'])} | {_or(r['description'])} | {r['chapter_start']} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def export_characters_map(db: SQLiteDB, output_path: str) -> int:
    all_chars = db.get_all_characters()

    entries = []
    for char in all_chars:
        char_id = char["character_id"]
        snaps_raw = db.get_all_snapshots(char_id)
        relations_raw = db.get_relations(char_id)

        snap_dicts = []
        for i, snap in enumerate(snaps_raw):
            nxt = snaps_raw[i + 1] if i + 1 < len(snaps_raw) else None
            snap_dicts.append({
                "chapter_start": snap["chapter_start"],
                "chapter_end": (nxt["chapter_start"] - 1) if nxt else None,
                "is_active": bool(snap["is_active"]),
                "level": snap["level"],
                "outfit": snap["outfit"],
                "weapon": snap["weapon"],
                "vfx_vibes": snap["vfx_vibes"],
                "physical_description": snap["physical_description"],
                "visual_importance": snap["visual_importance"],
            })

        rel_dicts = [
            {"related_name": r["related_name"], "description": r["description"], "chapter_start": r["chapter_start"]}
            for r in relations_raw
        ]

        entries.append({
            "char": dict(char),
            "snapshots": snap_dicts,
            "relations": rel_dicts,
            "peak": _max_importance(snap_dicts),
        })

    # Sort: highest visual_importance first, then name asc
    entries.sort(key=lambda x: (-x["peak"], x["char"]["name"]))

    stats = db.stats()
    merged = stats["merged_batches"]
    approx_chapters = merged * settings.wiki_batch_size

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    blocks: list[str] = []

    blocks.append(f"# Characters Map — {settings.story_slug}")
    blocks.append("")
    blocks.append(
        f"> Generated from `db/pipeline.db` | "
        f"{len(entries)} characters | "
        f"~{approx_chapters} chapters processed ({merged} batches)"
    )
    blocks.append("")
    blocks.append(
        "<!-- video-pipeline: character_id = Cref key | "
        "visual_anchor = stable LoRA tag | "
        "visual_importance = generation priority -->"
    )
    blocks.append("")
    blocks.append("---")
    blocks.append("")

    # TOC
    blocks.append("## Table of Contents")
    blocks.append("")
    for i, e in enumerate(entries, 1):
        c = e["char"]
        blocks.append(f"{i}. [{c['name']}](#{i}-{c['character_id']}) — importance {e['peak']}/10")
    blocks.append("")
    blocks.append("---")
    blocks.append("")

    for i, e in enumerate(entries, 1):
        blocks.append(_render_character(i, e["char"], e["snapshots"], e["relations"]))

    out.write_text("\n".join(blocks), encoding="utf-8")
    logger.info("Characters map written | path={} | characters={}", output_path, len(entries))
    return len(entries)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Export characters map to Markdown")
    parser.add_argument("--output", default=None, help="Output path (default: data/{story_slug}-characters.md)")
    args = parser.parse_args(argv)

    output_path = args.output or f"data/{settings.story_slug}-characters.md"
    db = SQLiteDB(settings.db_path)
    n = export_characters_map(db, output_path)
    print(f"Exported {n} characters → {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

    