"""Minimal ShardKV TCP client matching src/net/Protocol.h."""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass
from typing import Optional, Tuple

KV_GET_REQ, KV_GET_RESP = 1, 2
KV_PUT_REQ, KV_PUT_RESP = 3, 4
KV_DELETE_REQ, KV_DELETE_RESP = 5, 6
PING_REQ, PING_RESP = 11, 12

OK, NOT_FOUND, NOT_LEADER, TIMEOUT, OVERLOADED, ERROR = 0, 1, 2, 3, 4, 5

STATUS_NAME = {
    OK: "OK",
    NOT_FOUND: "NOT_FOUND",
    NOT_LEADER: "NOT_LEADER",
    TIMEOUT: "TIMEOUT",
    OVERLOADED: "OVERLOADED",
    ERROR: "ERROR",
}


class KvError(Exception):
    def __init__(self, status: int, message: str, leader_id: int = 0):
        super().__init__(message)
        self.status = status
        self.leader_id = leader_id


def _u32(n: int) -> bytes:
    return struct.pack(">I", n & 0xFFFFFFFF)


def _u64(n: int) -> bytes:
    return struct.pack(">Q", n & 0xFFFFFFFFFFFFFFFF)


def _str(s: str) -> bytes:
    data = s.encode("utf-8")
    return _u32(len(data)) + data


class _Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def u32(self) -> int:
        if self.pos + 4 > len(self.data):
            raise KvError(ERROR, "truncated response")
        v = struct.unpack(">I", self.data[self.pos : self.pos + 4])[0]
        self.pos += 4
        return v

    def u64(self) -> int:
        if self.pos + 8 > len(self.data):
            raise KvError(ERROR, "truncated response")
        v = struct.unpack(">Q", self.data[self.pos : self.pos + 8])[0]
        self.pos += 8
        return v

    def boolean(self) -> bool:
        if self.pos + 1 > len(self.data):
            raise KvError(ERROR, "truncated response")
        v = self.data[self.pos] != 0
        self.pos += 1
        return v

    def string(self) -> str:
        n = self.u32()
        if self.pos + n > len(self.data):
            raise KvError(ERROR, "truncated string")
        s = self.data[self.pos : self.pos + n].decode("utf-8", errors="replace")
        self.pos += n
        return s


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise KvError(ERROR, "connection closed")
        buf.extend(chunk)
    return bytes(buf)


@dataclass
class PingResult:
    node_id: int
    leader_id: int
    term: int


@dataclass
class GetResult:
    status: int
    leader_id: int
    found: bool
    value: str


@dataclass
class WriteResult:
    status: int
    leader_id: int


class TcpKvClient:
    def __init__(self, address: str, timeout: float = 5.0):
        host, port_s = address.rsplit(":", 1)
        self.host = host
        self.port = int(port_s)
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self) -> None:
        self.close()
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(self.timeout)
        self.sock = s

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def __enter__(self) -> "TcpKvClient":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _rpc(self, req_type: int, payload: bytes, resp_type: int) -> _Reader:
        if self.sock is None:
            self.connect()
        assert self.sock is not None
        frame = _u32(len(payload)) + _u32(req_type) + payload
        self.sock.sendall(frame)
        header = _recv_exact(self.sock, 8)
        length, mtype = struct.unpack(">II", header)
        if length > 16 * 1024 * 1024:
            raise KvError(ERROR, "response too large")
        body = _recv_exact(self.sock, length) if length else b""
        if mtype != resp_type:
            raise KvError(ERROR, f"unexpected message type {mtype}")
        return _Reader(body)

    def ping(self) -> PingResult:
        r = self._rpc(PING_REQ, b"", PING_RESP)
        return PingResult(r.u32(), r.u32(), r.u64())

    def get(self, key: str) -> GetResult:
        r = self._rpc(KV_GET_REQ, _str(key), KV_GET_RESP)
        return GetResult(r.u32(), r.u32(), r.boolean(), r.string())

    def put(self, key: str, value: str) -> WriteResult:
        r = self._rpc(KV_PUT_REQ, _str(key) + _str(value), KV_PUT_RESP)
        return WriteResult(r.u32(), r.u32())

    def delete(self, key: str) -> WriteResult:
        r = self._rpc(KV_DELETE_REQ, _str(key), KV_DELETE_RESP)
        return WriteResult(r.u32(), r.u32())


def ping(address: str, timeout: float = 1.0) -> PingResult:
    with TcpKvClient(address, timeout=timeout) as c:
        return c.ping()


def get(address: str, key: str, timeout: float = 5.0) -> GetResult:
    with TcpKvClient(address, timeout=timeout) as c:
        return c.get(key)


def put(address: str, key: str, value: str, timeout: float = 5.0) -> WriteResult:
    with TcpKvClient(address, timeout=timeout) as c:
        return c.put(key, value)


def delete(address: str, key: str, timeout: float = 5.0) -> WriteResult:
    with TcpKvClient(address, timeout=timeout) as c:
        return c.delete(key)


def try_rpc(fn, *args, **kwargs) -> Tuple[Optional[object], Optional[str]]:
    try:
        return fn(*args, **kwargs), None
    except (KvError, OSError, TimeoutError, socket.timeout) as e:
        return None, str(e)
