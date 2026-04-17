"""tests/test_db.py — SQLiteDB unit tests including time-travel + index."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from db.database import SQLiteDB


@pytest.fixture
def db(tmp_path):
    db_file = tmp_path / "test.db"
    database = SQLiteDB(str(db_file))
    yield database
    database.close()


def _add_character(db: SQLiteDB, char_id: str, name: str):
    db.upsert_character(
        character_id=char_id,
        name=name,
        name_normalized=name.lower(),
        aliases=[],
        traits=[],
        visual_anchor=None,
    )


def _add_snapshot(db: SQLiteDB, char_id: str, chapter_start: int, level: str = None):
    db.add_snapshot(
        character_id=char_id,
        chapter_start=chapter_start,
        is_active=True,
        level=level,
        outfit=None,
        weapon=None,
        vfx_vibes=None,
        physical_description=None,
        visual_importance=5,
        extraction_version=1,
    )


class TestUpsertCharacter:
    def test_insert(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        row = db.get_character_by_name("lâm phong")
        assert row is not None
        assert row["character_id"] == "hero_001"

    def test_update_identity(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        db.upsert_character("hero_001", "Lâm Phong Updated", "lâm phong updated", [], [], None)
        row = db.get_character_by_name("lâm phong updated")
        assert row["name"] == "Lâm Phong Updated"

    def test_merge_aliases(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        db.merge_aliases("hero_001", ["Lâm đại ca"])
        row = db.get_character_by_id("hero_001")
        import json
        aliases = json.loads(row["aliases_json"])
        assert "Lâm đại ca" in aliases


class TestSnapshots:
    def test_append_only(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        _add_snapshot(db, "hero_001", 1, level="Luyện Khí 1")
        _add_snapshot(db, "hero_001", 11, level="Luyện Khí 5")
        _add_snapshot(db, "hero_001", 51, level="Trúc Cơ")

        snaps = db.get_all_snapshots("hero_001")
        assert len(snaps) == 3
        assert [s["chapter_start"] for s in snaps] == [1, 11, 51]

    def test_get_latest_snapshot(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        _add_snapshot(db, "hero_001", 1, level="Luyện Khí 1")
        _add_snapshot(db, "hero_001", 51, level="Trúc Cơ")
        latest = db.get_latest_snapshot("hero_001")
        assert latest["chapter_start"] == 51
        assert latest["level"] == "Trúc Cơ"

    def test_time_travel(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        _add_snapshot(db, "hero_001", 1, level="Luyện Khí 1")
        _add_snapshot(db, "hero_001", 51, level="Trúc Cơ")

        # Chapter 30 → should return snapshot at chapter_start=1
        snap = db.get_snapshot_at("hero_001", 30)
        assert snap["chapter_start"] == 1
        assert snap["level"] == "Luyện Khí 1"

        # Chapter 60 → should return snapshot at chapter_start=51
        snap = db.get_snapshot_at("hero_001", 60)
        assert snap["chapter_start"] == 51
        assert snap["level"] == "Trúc Cơ"

    def test_index_used(self, db):
        """EXPLAIN QUERY PLAN should reference the idx_snap_char_ch index."""
        _add_character(db, "hero_001", "Lâm Phong")
        conn = db._conn
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT * FROM wiki_snapshots WHERE character_id=? AND chapter_start<=? "
            "ORDER BY chapter_start DESC LIMIT 1",
            ("hero_001", 120),
        ).fetchall()
        plan_text = " ".join(" ".join(str(col) for col in row) for row in plan)
        assert "idx_snap_char_ch" in plan_text

    def test_no_update_allowed(self, db):
        """DB must never UPDATE snapshots — only INSERT."""
        _add_character(db, "hero_001", "Lâm Phong")
        _add_snapshot(db, "hero_001", 1, level="Luyện Khí 1")
        _add_snapshot(db, "hero_001", 1, level="Trúc Cơ")  # same chapter_start = 2nd row
        snaps = db.get_all_snapshots("hero_001")
        assert len(snaps) == 2  # both rows exist, no upsert

    def test_snapshot_exists(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        _add_snapshot(db, "hero_001", 1, level="X")
        assert db.snapshot_exists("hero_001", 1, 1) is True
        assert db.snapshot_exists("hero_001", 1, 2) is False


class TestBatches:
    def test_upsert_and_get(self, db):
        db.upsert_batch(1, 1, 5, "PENDING")
        db.upsert_batch(6, 6, 10, "PENDING")
        pending = db.get_pending_batches()
        assert len(pending) == 2

    def test_status_transitions(self, db):
        db.upsert_batch(1, 1, 5, "PENDING")
        db.upsert_batch(1, 1, 5, "CRAWLED")
        db.upsert_batch(1, 1, 5, "EXTRACTED")
        db.upsert_batch(1, 1, 5, "MERGED")
        batch = db.get_batch(1)
        assert batch["status"] == "MERGED"

    def test_count_merged(self, db):
        db.upsert_batch(1, 1, 5, "MERGED")
        db.upsert_batch(6, 6, 10, "PENDING")
        assert db.count_merged_batches() == 1
        assert db.count_total_batches() == 2


class TestGetCharactersByNames:
    def test_by_name_normalized(self, db):
        _add_character(db, "hero_001", "Lâm Phong")
        _add_character(db, "hero_002", "Vân Lam")
        result = db.get_characters_by_names(["lâm phong"])
        assert len(result) == 1
        assert result[0]["character_id"] == "hero_001"

    def test_by_alias(self, db):
        db.upsert_character("hero_001", "Lâm Phong", "lâm phong", ["lâm sư huynh"], [], None)
        result = db.get_characters_by_names(["lâm sư huynh"])
        assert any(r["character_id"] == "hero_001" for r in result)


class TestCharacterLogicalDeleteAndMerge:
    def test_deleted_character_hidden_by_default(self, db):
        db.upsert_character("canon_001", "Diệp Thiếu Dương", "diep thieu duong", [], [], None)
        db.upsert_character("dup_001", "Diệp Thiếu Dương", "diem_thieu_duong", [], [], None)

        merged = db.merge_character_records("canon_001", ["dup_001"])
        assert merged == 1

        assert db.get_character_by_id("dup_001") is None
        deleted = db.get_character_by_id("dup_001", include_deleted=True)
        assert deleted is not None
        assert deleted["is_deleted"] == 1
        assert deleted["merged_into_character_id"] == "canon_001"

    def test_merge_moves_snapshots_relations_and_artifact_owner(self, db):
        db.upsert_character("canon_001", "Diệp Thiếu Dương", "diep thieu duong", [], [], None)
        db.upsert_character("dup_001", "Diệp Thiếu Dương", "diep_shieu_duong", ["Tiểu Dương"], [], None)

        db.add_snapshot(
            character_id="canon_001",
            chapter_start=10,
            is_active=True,
            level="A",
            outfit=None,
            weapon=None,
            vfx_vibes=None,
            physical_description=None,
            visual_importance=5,
            extraction_version=1,
        )
        db.add_snapshot(
            character_id="dup_001",
            chapter_start=20,
            is_active=True,
            level="B",
            outfit=None,
            weapon=None,
            vfx_vibes=None,
            physical_description=None,
            visual_importance=5,
            extraction_version=1,
        )
        db.add_relation("dup_001", "a_ngoc", "friend", 20)

        db.upsert_artifact("kiem_001", "Kiếm", "kiem")
        db.add_artifact_snapshot(
            artifact_id="kiem_001",
            chapter_start=20,
            owner_id="dup_001",
            normal_state=None,
            active_state=None,
        )

        db.merge_character_records("canon_001", ["dup_001"])

        snaps = db.get_all_snapshots("canon_001")
        assert len(snaps) == 2
        assert snaps[0]["chapter_start"] == 10
        assert snaps[1]["chapter_start"] == 20

        rels = db.get_relations("canon_001")
        assert len(rels) == 1
        assert rels[0]["related_name"] == "a_ngoc"

        art_snap = db.get_latest_artifact_snapshot("kiem_001")
        assert art_snap is not None
        assert art_snap["owner_id"] == "canon_001"
