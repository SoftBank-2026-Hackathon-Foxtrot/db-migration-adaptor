from __future__ import annotations

import os
import pwd
import stat
import subprocess
import sys

import pytest

from db_operator import config_file


def test_replace_preserves_mode_owner_group_and_bytes(tmp_path):
    path = tmp_path / "pgbouncer.ini"
    path.write_bytes(b"[databases]\napp=host=before\n")
    group = next((gid for gid in os.getgroups() if gid != os.getgid()), os.getgid())
    os.chown(path, os.getuid(), group)
    path.chmod(0o640)
    before, info = config_file.snapshot(path)
    config_file.preflight(path)
    after = b"[databases]\napp=host=after\n"
    config_file.replace(path, before, info, after)
    actual = path.stat()
    assert path.read_bytes() == after
    assert (actual.st_uid, actual.st_gid, stat.S_IMODE(actual.st_mode)) == (
        info.st_uid,
        group,
        0o640,
    )
    assert actual.st_ino != info.st_ino


def test_external_metadata_change_prevents_replace(tmp_path):
    path = tmp_path / "pgbouncer.ini"
    path.write_bytes(b"before")
    before, info = config_file.snapshot(path)
    path.chmod(0o640)
    with pytest.raises(FileExistsError):
        config_file.replace(path, before, info, b"after")
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".foxtrot-*"))


def test_preflight_rejects_unpreservable_owner_before_replace(tmp_path, monkeypatch):
    path = tmp_path / "pgbouncer.ini"
    path.write_bytes(b"before")
    group = next((gid for gid in os.getgroups() if gid != os.getgid()), None)
    if group is None:
        pytest.skip("A second group is required")
    os.chown(path, os.getuid(), group)

    def denied(*args):
        raise PermissionError("injected ownership denial")

    monkeypatch.setattr(os, "fchown", denied)
    with pytest.raises(PermissionError):
        config_file.preflight(path)
    assert path.read_bytes() == b"before"
    assert not list(tmp_path.glob(".foxtrot-*"))


def test_symlink_config_rejected(tmp_path):
    actual = tmp_path / "actual"
    actual.write_bytes(b"before")
    path = tmp_path / "symlink"
    path.symlink_to(actual)
    with pytest.raises(OSError):
        config_file.preflight(path)
    assert actual.read_bytes() == b"before"


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin ACL check")
def test_extended_acl_is_rejected(tmp_path):
    path = tmp_path / "acl.ini"
    path.write_bytes(b"before")
    subprocess.run(
        ["chmod", "+a", f"user:{pwd.getpwuid(os.getuid()).pw_name} allow read", str(path)],
        check=True,
    )
    try:
        with pytest.raises(PermissionError, match="ACL"):
            config_file.preflight(path)
    finally:
        subprocess.run(["chmod", "-N", str(path)], check=True)
    assert path.read_bytes() == b"before"


def test_replace_preserves_extended_attributes(tmp_path):
    path = tmp_path / "attributes.ini"
    path.write_bytes(b"before")
    if sys.platform == "darwin":
        subprocess.run(["xattr", "-w", "com.foxtrot.test", "preserved", str(path)], check=True)
    else:
        os.setxattr(path, "user.foxtrot.test", b"preserved")
    expected = config_file.attributes(path)
    before, info = config_file.snapshot(path)
    config_file.preflight(path)
    config_file.replace(path, before, info, b"after")
    assert path.read_bytes() == b"after"
    assert config_file.attributes(path) == expected
