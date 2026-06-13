"""
Tests for ingestion/base.py

What we test:
  - save() writes valid JSON with correct structure
  - run() returns empty string when fetch() returns nothing
  - run() calls save() and returns a file path when fetch() returns records
  - _get() raises on HTTP errors (retry behaviour covered by tenacity itself)
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import the module directly to avoid ingestion/__init__.py pulling in
# optional heavy dependencies (arxiv, sqlalchemy) not installed here
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "ingestion.base",
    Path(__file__).parent.parent / "ingestion" / "base.py",
)
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
BaseIngester = _mod.BaseIngester


# Minimal concrete subclass — BaseIngester is abstract
class DummyIngester(BaseIngester):
    def __init__(self, tmp_path, records=None):
        super().__init__("test_source", str(tmp_path))
        self._records = records or []

    def fetch(self) -> list[dict]:
        return self._records


# ----------------------------------------------------------------
# save()
# ----------------------------------------------------------------
class TestSave:
    def test_creates_json_file(self, tmp_path):
        ingester = DummyIngester(tmp_path)
        records = [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
        path = ingester.save(records)

        assert Path(path).exists()

    def test_json_structure(self, tmp_path):
        ingester = DummyIngester(tmp_path)
        records = [{"id": 1}]
        path = ingester.save(records)

        with open(path) as f:
            data = json.load(f)

        assert data["source"] == "test_source"
        assert data["record_count"] == 1
        assert data["records"] == records
        assert "batch_date" in data
        assert "ingested_at" in data

    def test_returns_file_path_string(self, tmp_path):
        ingester = DummyIngester(tmp_path)
        result = ingester.save([{"x": 1}])
        assert isinstance(result, str)
        assert result.endswith(".json")

    def test_overwrites_existing_file(self, tmp_path):
        ingester = DummyIngester(tmp_path)
        ingester.save([{"x": 1}])
        path = ingester.save([{"x": 2}, {"x": 3}])

        with open(path) as f:
            data = json.load(f)
        assert data["record_count"] == 2


# ----------------------------------------------------------------
# run()
# ----------------------------------------------------------------
class TestRun:
    def test_returns_empty_string_when_no_records(self, tmp_path):
        ingester = DummyIngester(tmp_path, records=[])
        assert ingester.run() == ""

    def test_returns_file_path_when_records_present(self, tmp_path):
        ingester = DummyIngester(tmp_path, records=[{"id": 1}])
        result = ingester.run()
        assert result != ""
        assert Path(result).exists()

    def test_saved_file_contains_correct_records(self, tmp_path):
        records = [{"id": 1, "val": "hello"}]
        ingester = DummyIngester(tmp_path, records=records)
        path = ingester.run()

        with open(path) as f:
            data = json.load(f)
        assert data["records"] == records


# ----------------------------------------------------------------
# _get()
# ----------------------------------------------------------------
class TestGet:
    def test_returns_parsed_json_on_success(self, tmp_path):
        ingester = DummyIngester(tmp_path)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"items": [1, 2, 3]}
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            result = ingester._get("https://example.com/api")

        assert result == {"items": [1, 2, 3]}

    def test_raises_on_http_error(self, tmp_path):
        import requests
        ingester = DummyIngester(tmp_path)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

        with patch("requests.get", return_value=mock_resp):
            with pytest.raises(requests.exceptions.HTTPError):
                ingester._get("https://example.com/api")
