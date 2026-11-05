"""설정 파일의 소유자·권한을 확인하고 안전하게 교체합니다."""

from __future__ import annotations

import ctypes
import errno
import os
import stat
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

# 파일 속성 확인


def attributes(path: Path) -> dict:
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        libc.listxattr.argtypes = [ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        libc.listxattr.restype = ctypes.c_ssize_t
        libc.getxattr.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_int,
        ]
        libc.getxattr.restype = ctypes.c_ssize_t
        size = libc.listxattr(os.fsencode(path), None, 0, 0)
        if size < 0:
            raise PermissionError("Config attributes could not be checked")
        names = ctypes.create_string_buffer(size)
        if libc.listxattr(os.fsencode(path), names, size, 0) != size:
            raise PermissionError("Config attributes changed")
        result = {}
        for name in filter(None, names.raw.split(b"\0")):
            length = libc.getxattr(os.fsencode(path), name, None, 0, 0, 0)
            if length < 0:
                raise PermissionError("Config attribute could not be read")
            data = ctypes.create_string_buffer(length)
            if libc.getxattr(os.fsencode(path), name, data, length, 0, 0) != length:
                raise PermissionError("Config attribute changed")
            result[name] = data.raw
        return result
    return {name: os.getxattr(path, name) for name in os.listxattr(path)}


def snapshot(path: Path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_mode & 0o7000:
            raise PermissionError(
                "A regular config file without special permission bits is required"
            )
        if any("posix_acl" in str(key) for key in attributes(path)):
            raise PermissionError("Extended config ACLs are not supported")
        if sys.platform == "darwin":
            libc = ctypes.CDLL(None, use_errno=True)
            libc.acl_get_file.argtypes = [ctypes.c_char_p, ctypes.c_int]
            libc.acl_get_file.restype = ctypes.c_void_p
            libc.acl_get_entry.argtypes = [
                ctypes.c_void_p,
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            libc.acl_free.argtypes = [ctypes.c_void_p]
            acl = libc.acl_get_file(os.fsencode(path), 0x100)
            if not acl:
                # Darwin reports ENOENT for a present file with no extended ACL.
                if ctypes.get_errno() != errno.ENOENT or identity(path.stat()) != identity(info):
                    raise PermissionError("Config ACL could not be checked")
            else:
                try:
                    entry = ctypes.c_void_p()
                    status = libc.acl_get_entry(acl, 0, ctypes.byref(entry))
                    if status == 0 or ctypes.get_errno() != errno.EINVAL:
                        raise PermissionError("Extended config ACLs are not supported")
                finally:
                    libc.acl_free(acl)
        return stream.read(), info


def identity(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_uid,
        info.st_gid,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def copy_attributes(source: Path, destination: Path) -> None:
    expected_attrs = attributes(source)
    if attributes(destination) != expected_attrs:
        if sys.platform == "darwin":
            libc = ctypes.CDLL(None, use_errno=True)
            libc.copyfile.argtypes = [
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.c_void_p,
                ctypes.c_uint32,
            ]
            if libc.copyfile(os.fsencode(source), os.fsencode(destination), None, 4) != 0:
                raise PermissionError("Config attributes could not be preserved")
        else:
            for key, value in expected_attrs.items():
                os.setxattr(destination, key, value)
        if attributes(destination) != expected_attrs:
            raise PermissionError("Config attributes could not be preserved")


# 설정 파일 교체


@contextmanager
def staged(path: Path, data: bytes, original):
    """Prove UID/GID/mode preservation before replacing the original."""
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".foxtrot-")
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            current = os.fstat(stream.fileno())
            if (current.st_uid, current.st_gid) != (original.st_uid, original.st_gid):
                os.fchown(stream.fileno(), original.st_uid, original.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(original.st_mode))
            current = os.fstat(stream.fileno())
            if (current.st_uid, current.st_gid, current.st_mode) != (
                original.st_uid,
                original.st_gid,
                original.st_mode,
            ):
                raise PermissionError("Config ownership or permissions could not be preserved")
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        copy_attributes(path, temporary)
        # A parent default ACL must not change the replacement file's access rules.
        snapshot(temporary)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        yield temporary
    finally:
        temporary.unlink(missing_ok=True)


def preflight(path: Path):
    data, info = snapshot(path)
    with staged(path, data, info):
        pass


def replace(path: Path, original: bytes, info, output: bytes):
    with staged(path, output, info) as temporary:
        current, latest = snapshot(path)
        if current != original or identity(latest) != identity(info):
            raise FileExistsError("Config changed outside the operator")
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
