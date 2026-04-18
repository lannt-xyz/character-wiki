"""tests/test_wiki_remaster_main.py — Safety guard unit tests for wiki_remaster.main().

All tests mock SQLiteDB so zero real DB I/O.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest


def _make_db(merged_count: int = 0) -> MagicMock:
    """Return a MagicMock SQLiteDB with count_char_batches_merged pre-set."""
    db = MagicMock()
    db.count_char_batches_merged.return_value = merged_count
    db.count_char_batches_total.return_value = merged_count
    db.get_all_active_characters.return_value = []
    db._scalar.return_value = 0
    return db


def _run_main(argv: list[str], db_mock: MagicMock) -> int:
    """Run wiki_remaster.main() with given argv and a pre-built db mock.

    Returns the exit code passed to sys.exit (or 0 if main returned normally).
    """
    exit_code: list[int] = [0]

    def _fake_exit(code: int = 0) -> None:
        exit_code[0] = code
        raise SystemExit(code)

    with patch("wiki_remaster.SQLiteDB", return_value=db_mock), \
         patch("wiki_remaster.sys.exit", side_effect=_fake_exit), \
         patch("wiki_remaster._setup_logging"), \
         patch("wiki_remaster.phase0_backup"), \
         patch("wiki_remaster.phase1_init_batches"), \
         patch("wiki_remaster.phase2_build_input", return_value=[]), \
         patch("wiki_remaster.phase3_char_extraction_loop"), \
         patch("wiki_remaster.phase4_final_synthesis"):
        saved_argv = sys.argv
        try:
            sys.argv = argv
            import wiki_remaster
            try:
                wiki_remaster.main()
            except SystemExit as exc:
                exit_code[0] = int(exc.code or 0)
        finally:
            sys.argv = saved_argv

    return exit_code[0]


class TestSafetyGuard:
    def test_blocks_when_merged_and_no_flag(self):
        """Default run (from_phase=0) + merged>0 → exit(1)."""
        db = _make_db(merged_count=500)
        code = _run_main(["wiki_remaster.py"], db)
        assert code == 1

    def test_passes_when_no_merged_batches(self):
        """Default run + no merged batches → Phase 1 runs, exit(0)."""
        db = _make_db(merged_count=0)
        with patch("wiki_remaster.SQLiteDB", return_value=db), \
             patch("wiki_remaster._setup_logging"), \
             patch("wiki_remaster.phase0_backup"), \
             patch("wiki_remaster.phase1_init_batches") as mock_p1, \
             patch("wiki_remaster.phase2_build_input", return_value=[]), \
             patch("wiki_remaster.phase3_char_extraction_loop"), \
             patch("wiki_remaster.phase4_final_synthesis"):
            saved_argv = sys.argv
            try:
                sys.argv = ["wiki_remaster.py"]
                import wiki_remaster
                wiki_remaster.main()
                mock_p1.assert_called_once()
            finally:
                sys.argv = saved_argv

    def test_from_phase_1_allows_reinit_with_merged(self):
        """--from-phase 1 + merged>0 → should NOT block, phase1 runs."""
        db = _make_db(merged_count=500)
        with patch("wiki_remaster.SQLiteDB", return_value=db), \
             patch("wiki_remaster._setup_logging"), \
             patch("wiki_remaster.phase0_backup"), \
             patch("wiki_remaster.phase1_init_batches") as mock_p1, \
             patch("wiki_remaster.phase2_build_input", return_value=[]), \
             patch("wiki_remaster.phase3_char_extraction_loop"), \
             patch("wiki_remaster.phase4_final_synthesis"), \
             patch("wiki_remaster.sys.exit") as mock_exit:
            saved_argv = sys.argv
            try:
                sys.argv = ["wiki_remaster.py", "--from-phase", "1"]
                import wiki_remaster
                wiki_remaster.main()
                mock_p1.assert_called_once()
                mock_exit.assert_not_called()
            finally:
                sys.argv = saved_argv

    def test_from_phase_3_skips_phase1_entirely(self):
        """--from-phase 3 → phase1 never called, safety guard not triggered."""
        db = _make_db(merged_count=500)
        with patch("wiki_remaster.SQLiteDB", return_value=db), \
             patch("wiki_remaster._setup_logging"), \
             patch("wiki_remaster.phase0_backup"), \
             patch("wiki_remaster.phase1_init_batches") as mock_p1, \
             patch("wiki_remaster.phase2_build_input", return_value=[]), \
             patch("wiki_remaster.phase3_char_extraction_loop") as mock_p3, \
             patch("wiki_remaster.phase4_final_synthesis"), \
             patch("wiki_remaster.sys.exit") as mock_exit:
            saved_argv = sys.argv
            try:
                sys.argv = ["wiki_remaster.py", "--from-phase", "3"]
                import wiki_remaster
                wiki_remaster.main()
                mock_p1.assert_not_called()
                mock_p3.assert_called_once()
                mock_exit.assert_not_called()
            finally:
                sys.argv = saved_argv

    def test_from_phase_4_skips_phase1_and_phase3(self):
        """--from-phase 4 → only phase4 runs."""
        db = _make_db(merged_count=500)
        with patch("wiki_remaster.SQLiteDB", return_value=db), \
             patch("wiki_remaster._setup_logging"), \
             patch("wiki_remaster.phase0_backup"), \
             patch("wiki_remaster.phase1_init_batches") as mock_p1, \
             patch("wiki_remaster.phase2_build_input", return_value=[]), \
             patch("wiki_remaster.phase3_char_extraction_loop") as mock_p3, \
             patch("wiki_remaster.phase4_final_synthesis") as mock_p4, \
             patch("wiki_remaster.sys.exit") as mock_exit:
            saved_argv = sys.argv
            try:
                sys.argv = ["wiki_remaster.py", "--from-phase", "4"]
                import wiki_remaster
                wiki_remaster.main()
                mock_p1.assert_not_called()
                mock_p3.assert_not_called()
                mock_p4.assert_called_once()
                mock_exit.assert_not_called()
            finally:
                sys.argv = saved_argv
