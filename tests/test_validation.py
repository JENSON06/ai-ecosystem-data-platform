"""
Tests for ingestion/validate.py

What we test:
  - validate_raw_file() raises FileNotFoundError when file is missing
  - validate_raw_file() raises ValueError for invalid JSON
  - validate_raw_file() raises ValueError when record count is below minimum
  - validate_raw_file() raises ValueError when required fields are missing
  - validate_raw_file() returns True for a valid file
  - validate_all() returns False if any source fails
  - validate_all() returns True when all sources pass
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import validate module directly to bypass ingestion/__init__.py
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "ingestion.validate",
    Path(__file__).parent.parent / "ingestion" / "validate.py",
)
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
validate_raw_file = _mod.validate_raw_file
validate_all = _mod.validate_all

BATCH_DATE = "2024-01-15"


def _write_raw(base: Path, source: str, records: list[dict]) -> Path:
    """Helper: writes a properly structured raw JSON file."""
    source_dir = base / source
    source_dir.mkdir(parents=True, exist_ok=True)
    file_path = source_dir / f"{source}_{BATCH_DATE}.json"
    file_path.write_text(json.dumps({
        "source": source,
        "batch_date": BATCH_DATE,
        "record_count": len(records),
        "records": records,
    }))
    return file_path


def _make_github_records(n: int) -> list[dict]:
    return [{"repo_id": i, "name": f"repo_{i}", "stars": i * 10} for i in range(n)]

def _make_huggingface_records(n: int) -> list[dict]:
    return [{"model_id": f"org/model_{i}", "author": f"org_{i}", "downloads": i * 100} for i in range(n)]

def _make_arxiv_records(n: int) -> list[dict]:
    return [{"paper_id": f"2024.{i:04d}", "title": f"Paper {i}", "published": "2024-01-15"} for i in range(n)]


# ----------------------------------------------------------------
# File existence check
# ----------------------------------------------------------------
class TestFileExistence:
    def test_raises_when_file_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            validate_raw_file("github", str(tmp_path), BATCH_DATE)


# ----------------------------------------------------------------
# JSON validity check
# ----------------------------------------------------------------
class TestJsonValidity:
    def test_raises_on_invalid_json(self, tmp_path):
        source_dir = tmp_path / "github"
        source_dir.mkdir()
        (source_dir / f"github_{BATCH_DATE}.json").write_text("{ not valid json }")

        with pytest.raises(ValueError, match="Invalid JSON"):
            validate_raw_file("github", str(tmp_path), BATCH_DATE)


# ----------------------------------------------------------------
# Minimum record count check
# ----------------------------------------------------------------
class TestRecordCount:
    def test_raises_when_below_minimum(self, tmp_path):
        _write_raw(tmp_path, "github", _make_github_records(10))  # min is 50
        with pytest.raises(ValueError, match="10 records"):
            validate_raw_file("github", str(tmp_path), BATCH_DATE)

    def test_passes_at_minimum_threshold(self, tmp_path):
        _write_raw(tmp_path, "github", _make_github_records(50))
        result = validate_raw_file("github", str(tmp_path), BATCH_DATE)
        assert result is True

    def test_passes_above_minimum(self, tmp_path):
        _write_raw(tmp_path, "github", _make_github_records(100))
        assert validate_raw_file("github", str(tmp_path), BATCH_DATE) is True


# ----------------------------------------------------------------
# Required fields check
# ----------------------------------------------------------------
class TestRequiredFields:
    def test_raises_when_required_field_missing(self, tmp_path):
        # Missing 'stars' field
        records = [{"repo_id": i, "name": f"repo_{i}"} for i in range(50)]
        _write_raw(tmp_path, "github", records)

        with pytest.raises(ValueError, match="missing required fields"):
            validate_raw_file("github", str(tmp_path), BATCH_DATE)

    def test_passes_when_all_required_fields_present(self, tmp_path):
        _write_raw(tmp_path, "github", _make_github_records(50))
        assert validate_raw_file("github", str(tmp_path), BATCH_DATE) is True


# ----------------------------------------------------------------
# All three sources
# ----------------------------------------------------------------
class TestAllSources:
    def test_github_validation_passes(self, tmp_path):
        _write_raw(tmp_path, "github", _make_github_records(60))
        assert validate_raw_file("github", str(tmp_path), BATCH_DATE) is True

    def test_huggingface_validation_passes(self, tmp_path):
        _write_raw(tmp_path, "huggingface", _make_huggingface_records(60))
        assert validate_raw_file("huggingface", str(tmp_path), BATCH_DATE) is True

    def test_arxiv_validation_passes(self, tmp_path):
        _write_raw(tmp_path, "arxiv", _make_arxiv_records(60))
        assert validate_raw_file("arxiv", str(tmp_path), BATCH_DATE) is True


# ----------------------------------------------------------------
# validate_all()
# ----------------------------------------------------------------
class TestValidateAll:
    def test_returns_true_when_all_pass(self, tmp_path):
        _write_raw(tmp_path, "github",      _make_github_records(60))
        _write_raw(tmp_path, "huggingface", _make_huggingface_records(60))
        _write_raw(tmp_path, "arxiv",       _make_arxiv_records(60))

        assert validate_all(str(tmp_path), BATCH_DATE) is True

    def test_returns_false_when_one_source_missing(self, tmp_path):
        _write_raw(tmp_path, "github",      _make_github_records(60))
        _write_raw(tmp_path, "huggingface", _make_huggingface_records(60))
        # arxiv file intentionally missing

        assert validate_all(str(tmp_path), BATCH_DATE) is False

    def test_returns_false_when_one_source_has_low_count(self, tmp_path):
        _write_raw(tmp_path, "github",      _make_github_records(60))
        _write_raw(tmp_path, "huggingface", _make_huggingface_records(5))  # too few
        _write_raw(tmp_path, "arxiv",       _make_arxiv_records(60))

        assert validate_all(str(tmp_path), BATCH_DATE) is False
