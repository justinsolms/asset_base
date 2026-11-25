import os

import asset_base


def test_source_tree_root_is_still_discovered(tmp_path, monkeypatch):
    package_dir = tmp_path / "checkout" / "src" / "asset_base"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").touch()
    (package_dir.parent.parent / "pyproject.toml").touch()
    monkeypatch.setattr(asset_base, "__file__", str(package_dir / "__init__.py"))

    assert asset_base.get_project_root_folder() == str(package_dir.parent.parent)


def test_runtime_directories_are_created_for_wheel_install(tmp_path, monkeypatch):
    package_dir = tmp_path / "venv" / "site-packages" / "asset_base"
    package_dir.mkdir(parents=True)
    package_file = package_dir / "__init__.py"
    package_file.touch()
    data_root = tmp_path / "runtime-data"
    monkeypatch.setattr(asset_base, "__file__", str(package_file))
    monkeypatch.setenv("ASSET_BASE_HOME", str(data_root))

    paths = (
        asset_base.get_tmp_path(),
        asset_base.get_cache_path(),
        asset_base.get_log_path(),
    )

    assert paths == tuple(
        str(data_root / "var" / name) for name in ("tmp", "cache", "log")
    )
    assert all(os.path.isdir(path) for path in paths)


def test_wheel_install_uses_user_data_directory_by_default(tmp_path, monkeypatch):
    package_dir = tmp_path / "venv" / "site-packages" / "asset_base"
    package_dir.mkdir(parents=True)
    package_file = package_dir / "__init__.py"
    package_file.touch()
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(asset_base, "__file__", str(package_file))
    monkeypatch.delenv("ASSET_BASE_HOME", raising=False)
    monkeypatch.setenv("HOME", str(home))

    assert asset_base.get_project_root_folder() == str(
        home / ".local" / "share" / "asset_base"
    )
