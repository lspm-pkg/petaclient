#!/usr/bin/env python
import io
import httpx
import sys
import os
import errno
import toml
import struct
import socket
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor
import time

with open("config.toml", "r") as f:
    config = toml.load(f)
SERVER = str(config["auth"]["server"])
DISK_NAME = config.get("disk_name", "/petafuse_volume.img")
if not DISK_NAME.startswith("/"): DISK_NAME = "/" + DISK_NAME
DISK_SIZE_BYTES = int(float(config.get("disk", {}).get("disk_size_gb", 256.0)) * 1024**3)

_executor = ThreadPoolExecutor(max_workers=128)
SOCKET_PATH = "/tmp/petafuse.sock"
_socket_send_lock = threading.Lock()
_stop_event = threading.Event()

class ApiClient:
    def __init__(self, server_url: str):
        self.base_url = server_url.strip('/')
        self.client = httpx.Client(
            timeout=120.0, 
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
            http2=True
        )

    def login(self, email: str, password: str):
        resp = self.client.post(f"{self.base_url}/api/login", json={"email": email, "password": password})
        resp.raise_for_status()
        self.client.headers.update({"Authorization": f"Bearer {resp.json()['token']}"})
        return resp.json()["token"]

    def request(self, method: str, endpoint: str, **kwargs):
        try:
            resp = self.client.request(method, f"{self.base_url}{endpoint}", **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            raise ConnectionError(f"API Error: {e}")

api_client = None

class DiskIO:
    @staticmethod
    def read(path: str, size: int, offset: int) -> bytes:
        return api_client.request("GET", "/api/fs/read", params={"path": path, "size": size, "offset": offset}).content
    
    @staticmethod
    def write(path: str, buf: bytes, offset: int):
        return api_client.request("POST", "/api/fs/write", data={"path": path, "start": offset}, files={"data": buf}).json()["written"]
    
    @staticmethod
    def flush(path: str):
        api_client.request("POST", "/api/fs/flush", data={"path": path})
        return 0
    
    @staticmethod
    def discard(path: str, size: int, offset: int):
        api_client.request("POST", "/api/fs/discard", data={"path": path, "size": size, "offset": offset})
        return 0
    
    @staticmethod
    def create(path: str):
        api_client.request("POST", "/api/fs/create", data={"path": path})
    
    @staticmethod
    def truncate(path: str, length: int):
        api_client.request("POST", "/api/fs/truncate", data={"path": path, "length": length})

def nbd_negotiate(conn):
    conn.sendall(struct.pack('>Q', 0x4e42444d41474943))
    conn.sendall(struct.pack('>Q', 0x00420281861253))
    conn.sendall(struct.pack('>Q', DISK_SIZE_BYTES))
    conn.sendall(struct.pack('>I', 1))
    conn.sendall(b'\x00' * 124)
    return True

def nbd_loop(conn):
    while not _stop_event.is_set():
        try:
            header = conn.recv(28)
            if not header or len(header) < 28: break
            magic, type_, handle, offset, length = struct.unpack(">IIQQL", header)
            if magic != 0x25609513: break
            
            data = None
            if type_ == 1:
                data = b''
                while len(data) < length:
                    chunk = conn.recv(length - len(data))
                    if not chunk: raise IOError("Write failed")
                    data += chunk
            
            _executor.submit(_process_and_reply, conn, handle, type_, offset, length, data)
        except: break
    conn.close()

def _process_and_reply(conn, handle, type_, offset, length, req_data):
    err, resp = 0, b""
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if type_ == 0:
                resp = DiskIO.read(DISK_NAME, length, offset)
            elif type_ == 1:
                DiskIO.write(DISK_NAME, req_data, offset)
            elif type_ == 3:
                err = DiskIO.flush(DISK_NAME)
            elif type_ == 4:
                err = DiskIO.discard(DISK_NAME, length, offset)
            elif type_ == 2:
                return
            else:
                err = errno.EINVAL
            break
        except:
            if attempt < max_retries - 1:
                time.sleep(0.1)
            else:
                err = errno.EROFS

    with _socket_send_lock:
        try:
            conn.sendall(struct.pack('>IIQ', 0x67446698, err, handle))
            if type_ == 0 and err == 0: conn.sendall(resp)
        except:
            pass

def main():
    if os.path.exists(SOCKET_PATH): os.unlink(SOCKET_PATH)
    global api_client
    api_client = ApiClient(SERVER)
    api_client.login(config["auth"]["email"], config["auth"]["password"])
    
    try:
        DiskIO.create(DISK_NAME)
        DiskIO.truncate(DISK_NAME, DISK_SIZE_BYTES)
    except: pass

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(SOCKET_PATH)
    sock.listen(1)

    subprocess.call(["qemu-nbd", "--disconnect", "/dev/nbd0"], stderr=subprocess.DEVNULL)
    connector = subprocess.Popen([
        "qemu-nbd",
        "--connect=/dev/nbd0",
        "--format=raw",
        "--cache=writeback",
        "--aio=io_uring",
        "--discard=ignore",
        "--detect-zeroes=on",
        f"nbd:unix:{SOCKET_PATH}"
    ])
    
    try:
        conn, _ = sock.accept()
        if nbd_negotiate(conn): nbd_loop(conn)
    except KeyboardInterrupt: pass
    finally:
        _stop_event.set()
        _executor.shutdown(wait=True)
        connector.terminate()
        subprocess.call(["qemu-nbd", "--disconnect", "/dev/nbd0"], stderr=subprocess.DEVNULL)
        if os.path.exists(SOCKET_PATH): os.remove(SOCKET_PATH)

if __name__ == '__main__': main()
