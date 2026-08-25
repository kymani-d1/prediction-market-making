from __future__ import annotations

import asyncio
import json
import selectors
import shutil
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest


TESTS_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = TESTS_ROOT.parent / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


def pytest_asyncio_loop_factories(
    config: pytest.Config, item: pytest.Item
) -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    del config, item
    if sys.platform == "win32":
        return {
            "selector": lambda: asyncio.SelectorEventLoop(
                selectors.SelectSelector()
            )
        }
    return {"default": asyncio.new_event_loop}


@pytest.fixture
def load_fixture() -> Callable[[str], dict[str, Any]]:
    def load(name: str) -> dict[str, Any]:
        path = TESTS_ROOT / "fixtures" / name
        value = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(value, dict), f"fixture {name} must contain a JSON object"
        return value

    return load


@pytest.fixture
def workspace_tmp_path() -> Path:
    """A per-test temporary directory inside the writable repository sandbox."""
    root = TESTS_ROOT / ".pytest-tmp"
    root.mkdir(exist_ok=True)
    path = root / uuid.uuid4().hex
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
        try:
            root.rmdir()
        except OSError:
            # Another concurrently running test may still own a sibling directory.
            pass
