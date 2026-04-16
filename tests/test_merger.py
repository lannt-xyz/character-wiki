"""tests/test_merger.py — Merger logic: inherit, append-only, dedup, min_change."""

import pytest

from db.database import SQLiteDB
from models.schemas import CharacterPatch, ExtractionResult
from wiki.merger import _apply_patch, _count_changes, merge_extraction_result


@pytest.fixture
def db(tmp_path):
    database = SQLiteDB(str(tmp_path / "test.db"))
    yield database
    database.close()


def _add_char(db, char_id="hero_001", name="Lâm Phong"):
    db.upsert_character(char_id, name, name.lower(), [], [], None)


def _add_snap(db, char_id="hero_001", chapter_start=1, level="Luyện Khí 1",
              outfit="Vải thô", weapon="Không", vfx_vibes="Trắng mờ",
              physical_description=None):
    db.add_snapshot(
        character_id=char_id,
        chapter_start=chapter_start,
        is_active=True,
        level=level,
        outfit=outfit,
        weapon=weapon,
        vfx_vibes=vfx_vibes,
        physical_description=physical_description,
        visual_importance=5,
        extraction_version=1,
    )


class TestApplyPatch:
    def test_persistent_inherit(self):
        base = {"level": "Luyện Khí 1", "outfit": "Vải thô", "weapon": "Không",
                "vfx_vibes": "Trắng", "physical_description": None, "visual_importance": 5, "is_active": 1}
        patch = CharacterPatch(character_id="hero_001")  # all None
        merged = _apply_patch(base, patch)
        assert merged["level"] == "Luyện Khí 1"
        assert merged["outfit"] == "Vải thô"
        assert merged["weapon"] == "Không"

    def test_persistent_override(self):
        base = {"level": "Luyện Khí 1", "outfit": "Vải thô", "weapon": "Không",
                "vfx_vibes": "Trắng", "physical_description": None, "visual_importance": 5, "is_active": 1}
        patch = CharacterPatch(character_id="hero_001", level="Trúc Cơ")
        merged = _apply_patch(base, patch)
        assert merged["level"] == "Trúc Cơ"
        assert merged["outfit"] == "Vải thô"  # inherited

    def test_transient_not_inherited(self):
        base = {"level": "X", "outfit": "Y", "weapon": "Z", "vfx_vibes": "W",
                "physical_description": "Đang bị thương", "visual_importance": 5, "is_active": 1}
        patch = CharacterPatch(character_id="hero_001")  # physical_description=None
        merged = _apply_patch(base, patch)
        assert merged["physical_description"] is None  # NOT inherited

    def test_is_active_inherit(self):
        base = {"level": None, "outfit": None, "weapon": None, "vfx_vibes": None,
                "physical_description": None, "visual_importance": 5, "is_active": 1}
        patch = CharacterPatch(character_id="hero_001")  # is_active=None
        merged = _apply_patch(base, patch)
        assert merged["is_active"] is True  # inherited

    def test_is_active_override_false(self):
        base = {"level": None, "outfit": None, "weapon": None, "vfx_vibes": None,
                "physical_description": None, "visual_importance": 5, "is_active": 1}
        patch = CharacterPatch(character_id="hero_001", is_active=False)
        merged = _apply_patch(base, patch)
        assert merged["is_active"] is False

    def test_no_base(self):
        patch = CharacterPatch(character_id="hero_001", level="Trúc Cơ")
        merged = _apply_patch(None, patch)
        assert merged["level"] == "Trúc Cơ"
        assert merged["outfit"] is None


class TestCountChanges:
    def test_no_changes(self):
        base = {"level": "A", "outfit": "B", "weapon": "C", "vfx_vibes": "D",
                "physical_description": None, "visual_importance": 5, "is_active": 1}
        merged = {"level": "A", "outfit": "B", "weapon": "C", "vfx_vibes": "D",
                  "physical_description": None, "visual_importance": 5, "is_active": True}
        assert _count_changes(base, merged) == 0

    def test_one_change(self):
        base = {"level": "A", "outfit": "B", "weapon": "C", "vfx_vibes": "D",
                "physical_description": None, "visual_importance": 5, "is_active": 1}
        merged = {"level": "New Level", "outfit": "B", "weapon": "C", "vfx_vibes": "D",
                  "physical_description": None, "visual_importance": 5, "is_active": True}
        assert _count_changes(base, merged) == 1

    def test_no_base_returns_max(self):
        merged = {"level": "A"}
        count = _count_changes(None, merged)
        assert count > 0


class TestMergeExtractionResult:
    def test_new_character_creates_snapshot(self, db):
        result = ExtractionResult(
            batch_chapter_start=1,
            batch_chapter_end=5,
            new_characters=[{
                "character": {
                    "character_id": "hero_001",
                    "name": "Lâm Phong",
                    "name_normalized": "lâm phong",
                    "aliases": [],
                    "traits": ["Kiên trì"],
                    "relations": [],
                    "visual_anchor": "Sẹo cánh tay phải",
                },
                "snapshot": {
                    "chapter_start": 1,
                    "is_active": True,
                    "level": "Luyện Khí 1",
                    "outfit": "Vải thô",
                    "weapon": "Không",
                    "vfx_vibes": "Trắng mờ",
                    "physical_description": None,
                    "visual_importance": 8,
                },
            }],
        )
        n_new, n_updated, n_skipped = merge_extraction_result(result, db)
        assert n_new == 1
        snaps = db.get_all_snapshots("hero_001")
        assert len(snaps) == 1
        assert snaps[0]["level"] == "Luyện Khí 1"

    def test_updated_character_appends_snapshot(self, db):
        _add_char(db)
        _add_snap(db, level="Luyện Khí 1", outfit="Vải thô")

        result = ExtractionResult(
            batch_chapter_start=51,
            batch_chapter_end=55,
            updated_characters=[
                CharacterPatch(character_id="hero_001", level="Trúc Cơ", outfit="Trường bào")
            ],
        )
        n_new, n_updated, n_skipped = merge_extraction_result(result, db)
        assert n_updated == 1
        snaps = db.get_all_snapshots("hero_001")
        assert len(snaps) == 2
        assert snaps[1]["level"] == "Trúc Cơ"

    def test_no_change_skipped(self, db):
        _add_char(db)
        _add_snap(db, level="Luyện Khí 1", outfit="Vải thô", weapon="Không", vfx_vibes="Trắng mờ")

        result = ExtractionResult(
            batch_chapter_start=11,
            batch_chapter_end=15,
            updated_characters=[
                # Patch returns all None → merged == base → 0 changes
                CharacterPatch(character_id="hero_001")
            ],
        )
        n_new, n_updated, n_skipped = merge_extraction_result(result, db)
        assert n_skipped == 1
        snaps = db.get_all_snapshots("hero_001")
        assert len(snaps) == 1  # no new snapshot added

    def test_alias_merge(self, db):
        _add_char(db)
        import json

        result = ExtractionResult(
            batch_chapter_start=11,
            batch_chapter_end=15,
            updated_characters=[
                CharacterPatch(character_id="hero_001", aliases=["Lâm đại ca"], level="X")
            ],
        )
        merge_extraction_result(result, db)
        char = db.get_character_by_id("hero_001")
        aliases = json.loads(char["aliases_json"])
        assert "Lâm đại ca" in aliases

    def test_unknown_patch_skipped(self, db):
        result = ExtractionResult(
            batch_chapter_start=1,
            batch_chapter_end=5,
            updated_characters=[
                CharacterPatch(character_id="unknown_char", level="X")
            ],
        )
        n_new, n_updated, n_skipped = merge_extraction_result(result, db)
        assert n_skipped == 1
