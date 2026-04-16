"""tests/test_extractor.py — Mock Ollama responses for extractor unit tests."""

import json
from unittest.mock import MagicMock, patch

import pytest

from models.schemas import ExtractionResult
from wiki.extractor import (
    ExtractionFatalError,
    _normalize,
    _pass1_name_scan,
    _pass2_delta_extract,
    extract_batch,
)


MOCK_PASS1_RESPONSE = json.dumps([
    {"name": "Lâm Phong", "aliases": ["Lâm sư huynh"]},
    {"name": "Vân Lam", "aliases": []},
])

MOCK_PASS2_RESPONSE = json.dumps({
    "new_characters": [],
    "updated_characters": [
        {
            "character_id": "hero_001",
            "level": "Trúc Cơ",
            "outfit": None,
            "weapon": None,
            "vfx_vibes": None,
            "physical_description": None,
            "visual_importance": None,
            "is_active": None,
            "aliases": None,
        }
    ],
})


class TestPass1NameScan:
    def test_parses_names(self):
        with patch("wiki.extractor._ollama_generate", return_value=MOCK_PASS1_RESPONSE):
            entries = _pass1_name_scan("some text", 1, 5)
        assert len(entries) == 2
        assert entries[0].name == "Lâm Phong"
        assert "Lâm sư huynh" in entries[0].aliases

    def test_parse_fail_returns_empty(self):
        with patch("wiki.extractor._ollama_generate", return_value="NOT_JSON"):
            entries = _pass1_name_scan("text", 1, 5)
        assert entries == []

    def test_wrapped_dict_response(self):
        wrapped = json.dumps({"characters": [{"name": "Hero", "aliases": []}]})
        with patch("wiki.extractor._ollama_generate", return_value=wrapped):
            entries = _pass1_name_scan("text", 1, 5)
        assert len(entries) == 1


class TestPass2DeltaExtract:
    def test_returns_extraction_result(self):
        with patch("wiki.extractor._ollama_generate", return_value=MOCK_PASS2_RESPONSE):
            result = _pass2_delta_extract("text", 11, 15, [])
        assert isinstance(result, ExtractionResult)
        assert len(result.updated_characters) == 1
        assert result.updated_characters[0].character_id == "hero_001"
        assert result.updated_characters[0].level == "Trúc Cơ"

    def test_parse_fail_raises(self):
        with patch("wiki.extractor._ollama_generate", return_value="BAD"):
            with pytest.raises(Exception):
                _pass2_delta_extract("text", 1, 5, [])


class TestExtractBatch:
    def _make_db_fns(self):
        get_by_names = MagicMock(return_value=[])
        get_all = MagicMock(return_value=[])
        get_snap = MagicMock(return_value=None)
        return get_by_names, get_all, get_snap

    def test_success_resets_counter(self):
        get_by_names, get_all, get_snap = self._make_db_fns()
        counter = [0]
        with patch("wiki.extractor._pass1_name_scan", return_value=[]), \
             patch("wiki.extractor._pass2_delta_extract",
                   return_value=ExtractionResult(batch_chapter_start=1, batch_chapter_end=5)):
            result = extract_batch("text", 1, 5, get_by_names, get_all, get_snap, counter)
        assert isinstance(result, ExtractionResult)
        assert counter[0] == 0

    def test_fail_increments_counter(self):
        get_by_names, get_all, get_snap = self._make_db_fns()
        counter = [0]
        with patch("wiki.extractor._pass2_delta_extract", side_effect=Exception("LLM error")):
            result = extract_batch("text", 1, 5, get_by_names, get_all, get_snap, counter)
        assert counter[0] == 1
        assert isinstance(result, ExtractionResult)  # returns empty result on single fail

    def test_consecutive_fail_raises_fatal(self):
        get_by_names, get_all, get_snap = self._make_db_fns()
        # Set counter already at max
        max_fails = 5  # matches settings default
        counter = [max_fails]
        with patch("wiki.extractor._pass2_delta_extract", side_effect=Exception("LLM down")):
            with pytest.raises(ExtractionFatalError):
                extract_batch("text", 1, 5, get_by_names, get_all, get_snap, counter)

    def test_small_wiki_skips_pass1(self):
        """When wiki_size < wiki_context_threshold, Pass 1 should be skipped."""
        get_by_names, get_all, get_snap = self._make_db_fns()
        get_all.return_value = []  # empty wiki → size=0 < threshold=50
        counter = [0]

        with patch("wiki.extractor._pass1_name_scan") as mock_p1, \
             patch("wiki.extractor._pass2_delta_extract",
                   return_value=ExtractionResult(batch_chapter_start=1, batch_chapter_end=5)):
            extract_batch("text", 1, 5, get_by_names, get_all, get_snap, counter)
        mock_p1.assert_not_called()


class TestNormalize:
    def test_lowercase_strip(self):
        assert _normalize("  Lâm Phong  ") == "lâm phong"

    def test_nfc(self):
        # Already NFC, no change expected
        result = _normalize("Lâm")
        assert result == "lâm"
