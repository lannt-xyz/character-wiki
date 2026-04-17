import json
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger


class SQLiteDB:
    """SQLite wrapper for the story-wiki pipeline.

    Tables
    ------
    chapters        — pipeline state per chapter (PENDING/CRAWLED/ERROR)
    wiki_characters — static identity, never temporal data
    wiki_snapshots  — append-only temporal snapshots (truly no UPDATE)
    wiki_relations  — append-only character relations
    wiki_batches    — extraction batch state (PENDING→CRAWLED→EXTRACTED→MERGED)
    """

    def __init__(self, db_path: str) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()
        self._migrate()
        logger.info("SQLiteDB ready | path={}", db_path)

    # ------------------------------------------------------------------
    # Schema bootstrap
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        cur = self._conn
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS chapters (
                chapter_num  INTEGER PRIMARY KEY,
                title        TEXT,
                url          TEXT,
                status       TEXT NOT NULL DEFAULT 'PENDING',
                crawled_at   TEXT,
                error_msg    TEXT
            );

            CREATE TABLE IF NOT EXISTS wiki_characters (
                character_id     TEXT PRIMARY KEY,
                name             TEXT NOT NULL,
                name_normalized  TEXT NOT NULL UNIQUE,
                aliases_json     TEXT NOT NULL DEFAULT '[]',
                traits_json      TEXT NOT NULL DEFAULT '[]',
                visual_anchor    TEXT,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wiki_snapshots (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                character_id         TEXT NOT NULL REFERENCES wiki_characters(character_id),
                chapter_start        INTEGER NOT NULL,
                is_active            INTEGER NOT NULL DEFAULT 1,
                level                TEXT,
                outfit               TEXT,
                weapon               TEXT,
                vfx_vibes            TEXT,
                physical_description TEXT,
                visual_importance    INTEGER NOT NULL DEFAULT 5,
                extraction_version   INTEGER NOT NULL DEFAULT 1,
                created_at           TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snap_char_ch
                ON wiki_snapshots(character_id, chapter_start DESC);

            CREATE TABLE IF NOT EXISTS wiki_relations (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                character_id TEXT NOT NULL REFERENCES wiki_characters(character_id),
                related_name TEXT NOT NULL,
                description  TEXT,
                chapter_start INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wiki_batches (
                batch_id           INTEGER PRIMARY KEY,
                chapter_start      INTEGER NOT NULL,
                chapter_end        INTEGER NOT NULL,
                status             TEXT NOT NULL DEFAULT 'PENDING',
                extraction_version INTEGER NOT NULL DEFAULT 1,
                extracted_at       TEXT,
                merged_at          TEXT
            );

            CREATE TABLE IF NOT EXISTS wiki_remaster_batches (
                batch_id          INTEGER PRIMARY KEY,
                chapter_start     INTEGER NOT NULL,
                chapter_end       INTEGER NOT NULL,
                remaster_version  INTEGER NOT NULL DEFAULT 1,
                status            TEXT NOT NULL DEFAULT 'PENDING',
                extracted_at      TEXT,
                merged_at         TEXT
            );

            CREATE TABLE IF NOT EXISTS wiki_artifacts (
                artifact_id      TEXT PRIMARY KEY,
                name             TEXT NOT NULL,
                name_normalized  TEXT NOT NULL UNIQUE,
                rarity           TEXT,
                material         TEXT,
                visual_anchor    TEXT,
                description      TEXT,
                created_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wiki_artifact_snapshots (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact_id         TEXT NOT NULL REFERENCES wiki_artifacts(artifact_id),
                chapter_start       INTEGER NOT NULL,
                owner_id            TEXT,
                normal_state        TEXT,
                active_state        TEXT,
                condition           TEXT NOT NULL DEFAULT 'intact',
                vfx_color           TEXT,
                is_key_event        INTEGER NOT NULL DEFAULT 0,
                extraction_version  INTEGER NOT NULL DEFAULT 2,
                created_at          TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_art_snap_ch
                ON wiki_artifact_snapshots(artifact_id, chapter_start DESC);
        """)
        self._conn.commit()

    def _migrate(self) -> None:
        """Add new columns to existing tables if not present (safe to run repeatedly)."""
        existing_chars = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(wiki_characters)").fetchall()
        }
        for col_name, col_def in [
            ("faction", "TEXT"),
            ("gender", "TEXT"),
            ("personality", "TEXT"),
            ("remaster_version", "INTEGER DEFAULT 1"),
            ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
            ("merged_into_character_id", "TEXT"),
            ("deleted_at", "TEXT"),
        ]:
            if col_name not in existing_chars:
                self._conn.execute(
                    f"ALTER TABLE wiki_characters ADD COLUMN {col_name} {col_def}"
                )

        # Migrate chapters table: add content column if missing
        existing_chapters = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(chapters)").fetchall()
        }
        if "content" not in existing_chapters:
            self._conn.execute("ALTER TABLE chapters ADD COLUMN content TEXT")
        # Drop file_path column if still present (SQLite >= 3.35)
        if "file_path" in existing_chapters:
            self._conn.execute("ALTER TABLE chapters DROP COLUMN file_path")

        self._conn.commit()

    # ------------------------------------------------------------------
    # chapters table
    # ------------------------------------------------------------------

    def upsert_chapter(
        self,
        chapter_num: int,
        title: str,
        url: str,
        status: str,
        crawled_at: Optional[datetime] = None,
        content: Optional[str] = None,
    ) -> None:
        now = _now()
        self._conn.execute(
            """
            INSERT INTO chapters(chapter_num, title, url, status, crawled_at, content)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chapter_num) DO UPDATE SET
                title=excluded.title,
                url=excluded.url,
                status=excluded.status,
                crawled_at=excluded.crawled_at,
                content=excluded.content
            """,
            (chapter_num, title, url, status, _dt(crawled_at or now), content),
        )
        self._conn.commit()

    def get_chapter_content(self, chapter_num: int) -> Optional[str]:
        """Return raw chapter text stored in DB, or None if not found."""
        row = self._conn.execute(
            "SELECT content FROM chapters WHERE chapter_num=?", (chapter_num,)
        ).fetchone()
        return row["content"] if row else None

    def set_chapter_status(
        self, chapter_num: int, status: str, error_msg: Optional[str] = None
    ) -> None:
        self._conn.execute(
            "UPDATE chapters SET status=?, error_msg=? WHERE chapter_num=?",
            (status, error_msg, chapter_num),
        )
        self._conn.commit()

    def get_chapter_status(self, chapter_num: int) -> Optional[str]:
        row = self._conn.execute(
            "SELECT status FROM chapters WHERE chapter_num=?", (chapter_num,)
        ).fetchone()
        return row["status"] if row else None

    # ------------------------------------------------------------------
    # wiki_characters table
    # ------------------------------------------------------------------

    def upsert_character(
        self,
        character_id: str,
        name: str,
        name_normalized: str,
        aliases: list[str],
        traits: list[str],
        visual_anchor: Optional[str],
    ) -> None:
        """Insert or update identity fields only. Never touches temporal data."""
        now = _dt(_now())
        self._conn.execute(
            """
            INSERT INTO wiki_characters
                (character_id, name, name_normalized, aliases_json, traits_json, visual_anchor, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(character_id) DO UPDATE SET
                name=excluded.name,
                name_normalized=excluded.name_normalized,
                aliases_json=excluded.aliases_json,
                traits_json=excluded.traits_json,
                visual_anchor=excluded.visual_anchor,
                is_deleted=0,
                merged_into_character_id=NULL,
                deleted_at=NULL,
                updated_at=excluded.updated_at
            """,
            (
                character_id,
                name,
                name_normalized,
                json.dumps(aliases, ensure_ascii=False),
                json.dumps(traits, ensure_ascii=False),
                visual_anchor,
                now,
                now,
            ),
        )
        self._conn.commit()

    def merge_aliases(self, character_id: str, new_aliases: list[str]) -> None:
        """Merge new_aliases into existing aliases_json without duplicates."""
        row = self._conn.execute(
            "SELECT aliases_json FROM wiki_characters WHERE character_id=?",
            (character_id,),
        ).fetchone()
        if not row:
            return
        existing: list[str] = json.loads(row["aliases_json"])
        merged = list({*existing, *new_aliases})
        self._conn.execute(
            "UPDATE wiki_characters SET aliases_json=?, updated_at=? WHERE character_id=?",
            (json.dumps(merged, ensure_ascii=False), _dt(_now()), character_id),
        )
        self._conn.commit()

    def get_character_by_name(self, name_normalized: str, include_deleted: bool = False) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM wiki_characters WHERE name_normalized=? "
            "AND (? OR is_deleted=0)",
            (name_normalized, int(include_deleted)),
        ).fetchone()
        if row:
            return dict(row)

        # Fallback: tolerate inconsistent separators/diacritics from historical data.
        needle = _normalize_lookup_key(name_normalized)
        if not needle:
            return None
        rows = self._conn.execute(
            "SELECT * FROM wiki_characters WHERE ? OR is_deleted=0",
            (int(include_deleted),),
        ).fetchall()
        for cand in rows:
            if _normalize_lookup_key(cand["name_normalized"]) == needle:
                return dict(cand)
        return None

    def get_characters_by_names(
        self, names_normalized: list[str], include_deleted: bool = False
    ) -> list[dict]:
        """Bulk fetch by normalized names/aliases with tolerant matching."""
        if not names_normalized:
            return []
        needles = {_normalize_lookup_key(n) for n in names_normalized if _normalize_lookup_key(n)}
        if not needles:
            return []

        rows = self._conn.execute(
            "SELECT * FROM wiki_characters WHERE ? OR is_deleted=0",
            (int(include_deleted),),
        ).fetchall()
        result: list[dict] = []
        for row in rows:
            aliases: list[str] = json.loads(row["aliases_json"])
            candidates = {
                _normalize_lookup_key(row["name_normalized"]),
                _normalize_lookup_key(row["name"]),
            }
            candidates.update(_normalize_lookup_key(a) for a in aliases)
            if needles.intersection(candidates):
                result.append(dict(row))
        return result

    def get_all_characters(self, include_deleted: bool = False) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_characters WHERE ? OR is_deleted=0",
            (int(include_deleted),),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_character_by_id(self, character_id: str, include_deleted: bool = False) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM wiki_characters WHERE character_id=? AND (? OR is_deleted=0)",
            (character_id, int(include_deleted)),
        ).fetchone()
        return dict(row) if row else None

    def get_character_snapshot_count(self, character_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM wiki_snapshots WHERE character_id=?",
            (character_id,),
        ).fetchone()
        return row["c"] if row else 0

    def merge_character_records(
        self,
        canonical_id: str,
        duplicate_ids: list[str],
        reason: str = "duplicate_character_id",
    ) -> int:
        """Merge duplicate character rows into canonical row using logical delete.

        - Move snapshots/relations/artifact owners to canonical_id.
        - Keep duplicate identity rows for audit by marking is_deleted=1.
        """
        canonical = self.get_character_by_id(canonical_id, include_deleted=True)
        if not canonical:
            raise ValueError(f"canonical character not found: {canonical_id}")

        dup_ids = [cid for cid in dict.fromkeys(duplicate_ids) if cid and cid != canonical_id]
        if not dup_ids:
            return 0

        placeholders = ",".join("?" for _ in dup_ids)
        existing_dups = self._conn.execute(
            f"SELECT * FROM wiki_characters WHERE character_id IN ({placeholders})",
            dup_ids,
        ).fetchall()
        if not existing_dups:
            return 0

        now = _dt(_now())
        merged_count = 0

        self._conn.execute("BEGIN")
        try:
            canonical_aliases = set(json.loads(canonical.get("aliases_json") or "[]"))
            canonical_traits = set(json.loads(canonical.get("traits_json") or "[]"))

            for dup_row in existing_dups:
                dup = dict(dup_row)
                dup_id = dup["character_id"]
                if dup["is_deleted"]:
                    continue

                canonical_aliases.update(json.loads(dup["aliases_json"] or "[]"))
                canonical_traits.update(json.loads(dup["traits_json"] or "[]"))

                # Keep richer canonical identity when available.
                if (not canonical.get("visual_anchor")) and dup.get("visual_anchor"):
                    canonical["visual_anchor"] = dup["visual_anchor"]
                if (not canonical.get("faction")) and dup.get("faction"):
                    canonical["faction"] = dup["faction"]
                if (not canonical.get("gender")) and dup.get("gender"):
                    canonical["gender"] = dup["gender"]
                if (not canonical.get("personality")) and dup.get("personality"):
                    canonical["personality"] = dup["personality"]

                self._conn.execute(
                    "UPDATE wiki_snapshots SET character_id=? WHERE character_id=?",
                    (canonical_id, dup_id),
                )
                self._conn.execute(
                    "UPDATE wiki_relations SET character_id=? WHERE character_id=?",
                    (canonical_id, dup_id),
                )
                self._conn.execute(
                    "UPDATE wiki_artifact_snapshots SET owner_id=? WHERE owner_id=?",
                    (canonical_id, dup_id),
                )
                self._conn.execute(
                    """
                    UPDATE wiki_characters
                    SET is_deleted=1,
                        merged_into_character_id=?,
                        deleted_at=?,
                        updated_at=?
                    WHERE character_id=?
                    """,
                    (canonical_id, now, now, dup_id),
                )
                merged_count += 1

            self._conn.execute(
                """
                UPDATE wiki_characters
                SET aliases_json=?,
                    traits_json=?,
                    visual_anchor=coalesce(?, visual_anchor),
                    faction=coalesce(?, faction),
                    gender=coalesce(?, gender),
                    personality=coalesce(?, personality),
                    is_deleted=0,
                    merged_into_character_id=NULL,
                    deleted_at=NULL,
                    updated_at=?
                WHERE character_id=?
                """,
                (
                    json.dumps(sorted(canonical_aliases), ensure_ascii=False),
                    json.dumps(sorted(canonical_traits), ensure_ascii=False),
                    canonical.get("visual_anchor"),
                    canonical.get("faction"),
                    canonical.get("gender"),
                    canonical.get("personality"),
                    now,
                    canonical_id,
                ),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        logger.info(
            "merge_character_records | canonical={} merged={} reason={}",
            canonical_id,
            merged_count,
            reason,
        )
        return merged_count

    # ------------------------------------------------------------------
    # wiki_snapshots table — APPEND ONLY, never UPDATE
    # ------------------------------------------------------------------

    def add_snapshot(
        self,
        character_id: str,
        chapter_start: int,
        is_active: bool,
        level: Optional[str],
        outfit: Optional[str],
        weapon: Optional[str],
        vfx_vibes: Optional[str],
        physical_description: Optional[str],
        visual_importance: int,
        extraction_version: int,
    ) -> None:
        """INSERT a new snapshot row. Never called with UPDATE logic."""
        self._conn.execute(
            """
            INSERT INTO wiki_snapshots
                (character_id, chapter_start, is_active, level, outfit, weapon,
                 vfx_vibes, physical_description, visual_importance, extraction_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                character_id,
                chapter_start,
                int(is_active),
                level,
                outfit,
                weapon,
                vfx_vibes,
                physical_description,
                visual_importance,
                extraction_version,
                _dt(_now()),
            ),
        )
        self._conn.commit()

    def get_latest_snapshot(
        self, character_id: str, before_chapter: Optional[int] = None
    ) -> Optional[dict]:
        """Return the latest snapshot for a character.

        If before_chapter is set, only snapshots with chapter_start < before_chapter
        are considered — prevents context bleeding in remaster v2 runs.
        """
        if before_chapter is not None:
            row = self._conn.execute(
                """
                SELECT * FROM wiki_snapshots
                WHERE character_id=? AND chapter_start < ?
                ORDER BY chapter_start DESC LIMIT 1
                """,
                (character_id, before_chapter),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT * FROM wiki_snapshots
                WHERE character_id=?
                ORDER BY chapter_start DESC LIMIT 1
                """,
                (character_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_snapshot_at(self, character_id: str, chapter_num: int) -> Optional[dict]:
        """Time-travel: returns the snapshot valid at chapter_num."""
        row = self._conn.execute(
            """
            SELECT s.*, c.visual_anchor
            FROM wiki_snapshots s
            JOIN wiki_characters c ON c.character_id = s.character_id
            WHERE s.character_id=? AND s.chapter_start<=?
            ORDER BY s.chapter_start DESC LIMIT 1
            """,
            (character_id, chapter_num),
        ).fetchone()
        return dict(row) if row else None

    def get_all_snapshots(self, character_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_snapshots WHERE character_id=? ORDER BY chapter_start ASC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def snapshot_exists(self, character_id: str, chapter_start: int, extraction_version: int) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM wiki_snapshots
            WHERE character_id=? AND chapter_start=? AND extraction_version=?
            LIMIT 1
            """,
            (character_id, chapter_start, extraction_version),
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # wiki_relations table
    # ------------------------------------------------------------------

    def add_relation(
        self,
        character_id: str,
        related_name: str,
        description: Optional[str],
        chapter_start: int,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO wiki_relations(character_id, related_name, description, chapter_start)
            VALUES (?, ?, ?, ?)
            """,
            (character_id, related_name, description, chapter_start),
        )
        self._conn.commit()

    def get_relations(self, character_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_relations WHERE character_id=? ORDER BY chapter_start ASC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # wiki_batches table
    # ------------------------------------------------------------------

    def upsert_batch(
        self,
        batch_id: int,
        chapter_start: int,
        chapter_end: int,
        status: str,
        extraction_version: int = 1,
        extracted_at: Optional[datetime] = None,
        merged_at: Optional[datetime] = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO wiki_batches
                (batch_id, chapter_start, chapter_end, status, extraction_version, extracted_at, merged_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id) DO UPDATE SET
                status=excluded.status,
                extraction_version=excluded.extraction_version,
                extracted_at=coalesce(excluded.extracted_at, wiki_batches.extracted_at),
                merged_at=coalesce(excluded.merged_at, wiki_batches.merged_at)
            """,
            (
                batch_id,
                chapter_start,
                chapter_end,
                status,
                extraction_version,
                _dt(extracted_at) if extracted_at else None,
                _dt(merged_at) if merged_at else None,
            ),
        )
        self._conn.commit()

    def get_batch(self, batch_id: int) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM wiki_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_pending_batches(self) -> list[dict]:
        """Return all batches not yet MERGED, ordered by batch_id."""
        rows = self._conn.execute(
            "SELECT * FROM wiki_batches WHERE status != 'MERGED' ORDER BY batch_id ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_batches(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_batches ORDER BY batch_id ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def count_merged_batches(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) as c FROM wiki_batches WHERE status='MERGED'"
        ).fetchone()
        return row["c"] if row else 0

    def count_total_batches(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) as c FROM wiki_batches"
        ).fetchone()
        return row["c"] if row else 0

    # ------------------------------------------------------------------
    # Stats helpers
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "total_batches": self.count_total_batches(),
            "merged_batches": self.count_merged_batches(),
            "total_characters": self._scalar("SELECT COUNT(*) FROM wiki_characters WHERE is_deleted=0"),
            "total_snapshots": self._scalar("SELECT COUNT(*) FROM wiki_snapshots"),
        }

    def _scalar(self, sql: str) -> int:
        row = self._conn.execute(sql).fetchone()
        return row[0] if row else 0

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # wiki_characters — remaster identity update
    # ------------------------------------------------------------------

    def update_character_identity(
        self,
        character_id: str,
        visual_anchor: Optional[str] = None,
        faction: Optional[str] = None,
        gender: Optional[str] = None,
        aliases: Optional[list[str]] = None,
        personality: Optional[str] = None,
        remaster_version: Optional[int] = None,
    ) -> None:
        """Overwrite enriched identity fields added by remaster. Only touches non-None args."""
        sets = []
        params = []
        if visual_anchor is not None:
            sets.append("visual_anchor=?")
            params.append(visual_anchor)
        if faction is not None:
            sets.append("faction=?")
            params.append(faction)
        if gender is not None:
            sets.append("gender=?")
            params.append(gender)
        if personality is not None:
            sets.append("personality=?")
            params.append(personality)
        if aliases is not None:
            sets.append("aliases_json=?")
            params.append(json.dumps(aliases, ensure_ascii=False))
        if remaster_version is not None:
            sets.append("remaster_version=?")
            params.append(remaster_version)
        if not sets:
            return
        sets.append("updated_at=?")
        params.append(_dt(_now()))
        params.append(character_id)
        self._conn.execute(
            f"UPDATE wiki_characters SET {', '.join(sets)} WHERE character_id=?",
            params,
        )
        self._conn.commit()

    def get_top_characters_by_snapshot(self, limit: int = 20) -> list[dict]:
        """Return top N characters by v1 snapshot count (descending)."""
        rows = self._conn.execute(
            """
            SELECT c.*, COUNT(s.id) as snap_count
            FROM wiki_characters c
            JOIN wiki_snapshots s ON s.character_id = c.character_id
            WHERE s.extraction_version = 1 AND c.is_deleted = 0
            GROUP BY c.character_id
            ORDER BY snap_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # wiki_remaster_batches table
    # ------------------------------------------------------------------

    def init_remaster_batches(self) -> int:
        """Populate wiki_remaster_batches from wiki_batches (idempotent). Returns count inserted."""
        self._conn.execute(
            """
            INSERT OR IGNORE INTO wiki_remaster_batches (batch_id, chapter_start, chapter_end)
            SELECT batch_id, chapter_start, chapter_end FROM wiki_batches
            """
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT COUNT(*) FROM wiki_remaster_batches"
        ).fetchone()
        return row[0] if row else 0

    def get_remaster_pending_batches(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_remaster_batches WHERE status != 'MERGED' ORDER BY batch_id ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def set_remaster_batch_status(
        self,
        batch_id: int,
        status: str,
    ) -> None:
        now = _dt(_now())
        if status == "EXTRACTED":
            self._conn.execute(
                "UPDATE wiki_remaster_batches SET status=?, extracted_at=? WHERE batch_id=?",
                (status, now, batch_id),
            )
        elif status == "MERGED":
            self._conn.execute(
                "UPDATE wiki_remaster_batches SET status=?, merged_at=? WHERE batch_id=?",
                (status, now, batch_id),
            )
        else:
            self._conn.execute(
                "UPDATE wiki_remaster_batches SET status=? WHERE batch_id=?",
                (status, batch_id),
            )
        self._conn.commit()

    def count_remaster_merged(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM wiki_remaster_batches WHERE status='MERGED'"
        ).fetchone()
        return row[0] if row else 0

    def count_remaster_total(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM wiki_remaster_batches"
        ).fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # wiki_artifacts table
    # ------------------------------------------------------------------

    def upsert_artifact(
        self,
        artifact_id: str,
        name: str,
        name_normalized: str,
        rarity: Optional[str] = None,
        material: Optional[str] = None,
        visual_anchor: Optional[str] = None,
        description: Optional[str] = None,
    ) -> str:
        existing = self._conn.execute(
            "SELECT artifact_id FROM wiki_artifacts WHERE name_normalized=?",
            (name_normalized,),
        ).fetchone()
        if existing and existing["artifact_id"] != artifact_id:
            artifact_id = existing["artifact_id"]

        now = _dt(_now())
        self._conn.execute(
            """
            INSERT INTO wiki_artifacts
                (artifact_id, name, name_normalized, rarity, material, visual_anchor, description, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(artifact_id) DO UPDATE SET
                name=excluded.name,
                rarity=coalesce(excluded.rarity, wiki_artifacts.rarity),
                material=coalesce(excluded.material, wiki_artifacts.material),
                visual_anchor=coalesce(excluded.visual_anchor, wiki_artifacts.visual_anchor),
                description=coalesce(excluded.description, wiki_artifacts.description)
            """,
            (artifact_id, name, name_normalized, rarity, material, visual_anchor, description, now),
        )
        self._conn.commit()
        return artifact_id

    def add_artifact_snapshot(
        self,
        artifact_id: str,
        chapter_start: int,
        owner_id: Optional[str],
        normal_state: Optional[str],
        active_state: Optional[str],
        condition: str = "intact",
        vfx_color: Optional[str] = None,
        is_key_event: bool = False,
        extraction_version: int = 2,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO wiki_artifact_snapshots
                (artifact_id, chapter_start, owner_id, normal_state, active_state,
                 condition, vfx_color, is_key_event, extraction_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                chapter_start,
                owner_id,
                normal_state,
                active_state,
                condition,
                vfx_color,
                int(is_key_event),
                extraction_version,
                _dt(_now()),
            ),
        )
        self._conn.commit()

    def get_all_artifacts(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM wiki_artifacts ORDER BY artifact_id ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_artifact_snapshot_at(
        self, artifact_id: str, chapter_num: int
    ) -> Optional[dict]:
        row = self._conn.execute(
            """
            SELECT * FROM wiki_artifact_snapshots
            WHERE artifact_id=? AND chapter_start <= ?
            ORDER BY chapter_start DESC LIMIT 1
            """,
            (artifact_id, chapter_num),
        ).fetchone()
        return dict(row) if row else None

    def get_latest_artifact_snapshot(self, artifact_id: str) -> Optional[dict]:
        row = self._conn.execute(
            """
            SELECT * FROM wiki_artifact_snapshots
            WHERE artifact_id=?
            ORDER BY chapter_start DESC LIMIT 1
            """,
            (artifact_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_v1_weapon_strings(self) -> list[str]:
        """Return all non-empty weapon strings from v1 wiki_snapshots."""
        rows = self._conn.execute(
            "SELECT weapon FROM wiki_snapshots WHERE weapon IS NOT NULL AND weapon != '' AND extraction_version=1"
        ).fetchall()
        return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dt(dt: datetime) -> str:
    return dt.isoformat()


def _normalize_lookup_key(text: str) -> str:
    if not text:
        return ""
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()
