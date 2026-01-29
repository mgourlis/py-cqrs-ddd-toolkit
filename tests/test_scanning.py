import sys
import importlib
import os
from types import ModuleType
import pkgutil

import pytest

from cqrs_ddd.scanning import scan_packages


def test_scan_packages_nonexistent_package_logs_error(caplog):
    caplog.clear()
    caplog.set_level("ERROR")
    scan_packages(["this_package_does_not_exist_12345"])
    assert any("Failed to import package" in rec.message for rec in caplog.records)


def test_scan_packages_with_module_without_path_skips(caplog):
    import math
    caplog.clear()
    caplog.set_level("DEBUG")
    # math has no __path__, scan_packages should simply skip it without error
    scan_packages([math])
    # No debug "Scanned module" entries should be produced
    assert not any("Scanned module" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_scan_packages_scans_submodules(tmp_path, caplog):
    caplog.clear()
    caplog.set_level("DEBUG")

    # Create a temporary package with two submodules: one valid, one that raises ImportError
    pkg_dir = tmp_path / "tmp_pkg_scan"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("# package init")

    # good module
    (pkg_dir / "good.py").write_text("VALUE = 123")

    # bad module that raises on import
    (pkg_dir / "bad.py").write_text("raise ImportError('boom during import')")

    # Add tmp_path's parent to sys.path and import
    sys.path.insert(0, str(tmp_path))
    try:
        # import package to ensure it's a module object
        import tmp_pkg_scan as mod  # noqa: F401
        importlib.invalidate_caches()

        # Run scan
        scan_packages(["tmp_pkg_scan"])

        # We expect good module scanned and bad module logged as error
        assert any("tmp_pkg_scan.good" in rec.message for rec in caplog.records if rec.levelname == "DEBUG")
        assert any("Failed to import module 'tmp_pkg_scan.bad'" in rec.message for rec in caplog.records if rec.levelname == "ERROR")

    finally:
        # Cleanup sys.path and modules
        sys.path.remove(str(tmp_path))
        for m in list(sys.modules.keys()):
            if m.startswith("tmp_pkg_scan"):
                del sys.modules[m]
        importlib.invalidate_caches()
