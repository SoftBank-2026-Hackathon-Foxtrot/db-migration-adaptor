"""Run inside an isolated Linux container. No database or credentials are needed."""

import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, "/fixture")
from db_operator import config_file


def receive(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        assert chunk, "PgBouncer disconnected"
        data += chunk
    return data


def response(sock):
    result = []
    while True:
        kind = receive(sock, 1)
        payload = receive(sock, struct.unpack("!I", receive(sock, 4))[0] - 4)
        assert kind != b"E", payload.decode(errors="replace")
        result.append(payload)
        if kind == b"Z":
            return b"\n".join(result)


def admin(command):
    with socket.create_connection(("127.0.0.1", 6432), timeout=3) as sock:
        params = b"user\0operator\0database\0pgbouncer\0\0"
        sock.sendall(struct.pack("!II", len(params) + 8, 196608) + params)
        response(sock)
        query = command.encode() + b"\0"
        sock.sendall(b"Q" + struct.pack("!I", len(query) + 4) + query)
        return response(sock)


with tempfile.TemporaryDirectory(prefix="config-reader-") as directory:
    root = Path(directory)
    root.chmod(0o755)
    path = root / "pgbouncer.ini"
    users = root / "users.txt"
    users.write_text('"operator" ""\n')
    path.write_text(
        "[databases]\napp=host=127.0.0.1 port=5432 dbname=app\n"
        "[pgbouncer]\nlisten_addr=127.0.0.1\nlisten_port=6432\n"
        f"auth_type=trust\nauth_file={users}\nadmin_users=operator\n"
        "pool_mode=transaction\nunix_socket_dir=/tmp\n"
        f"pidfile={root}/pgbouncer.pid\n"
    )
    # Only root owns the directory; PgBouncer gets group read, not world read.
    for file in [path, users]:
        os.chown(file, 0, 65534)
        file.chmod(0o640)
    # PID needs a writable directory owned by the reader.
    pid = root / "reader"
    pid.mkdir()
    os.chown(pid, 65534, 65534)
    path.write_text(path.read_text().replace(f"pidfile={root}/", f"pidfile={pid}/"))

    def reader_identity():
        os.setgroups([])
        os.setgid(65534)
        os.setuid(65534)

    log = root / "process.log"
    with log.open("wb") as stream:
        process = subprocess.Popen(
            ["pgbouncer", str(path)], preexec_fn=reader_identity, stdout=stream, stderr=stream
        )
        try:
            deadline = time.monotonic() + 5
            while True:
                try:
                    assert b"127.0.0.1" in admin("SHOW DATABASES")
                    break
                except (OSError, AssertionError):
                    assert process.poll() is None, log.read_text()
                    assert time.monotonic() < deadline, log.read_text()
                    time.sleep(0.05)
            before, metadata = config_file.snapshot(path)
            config_file.preflight(path)
            config_file.replace(
                path, before, metadata, before.replace(b"127.0.0.1 port", b"127.0.0.2 port")
            )
            actual = path.stat()
            assert (actual.st_uid, actual.st_gid, actual.st_mode & 0o777) == (0, 65534, 0o640)
            admin("RELOAD")
            assert b"127.0.0.2" in admin("SHOW DATABASES")
            print(
                "PASS: root writer -> uid/gid 65534 PgBouncer reader; 0640; RELOAD and live route"
            )
        finally:
            process.terminate()
            process.wait(timeout=5)
