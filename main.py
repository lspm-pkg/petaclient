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
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor

with open("config.toml", "r") as f:
    config = toml.load(f)
SERVER = str(config["auth"]["server"])
raw_disk_name = config.get("disk_name", "/petafuse_volume.img")
if not raw_disk_name.startswith("/"):
    raw_disk_name = "/" + raw_disk_name
DISK_NAME = raw_disk_name
disk_gb = config.get("disk", {}).get("disk_size_gb", 256.0)
DISK_SIZE_BYTES = int(float(disk_gb) * 1024**3)
_executor = ThreadPoolExecutor(max_workers=64)
SOCKET_PATH = "/tmp/petafuse.sock"
_socket_send_lock = threading.Lock()
_stop_event = threading.Event()

class ApiClient:
    def __init__(self, server_url: str):
        self.base_url = server_url.strip('/')
        self.client = httpx.Client(timeout=120.0, follow_redirects=True)

    def login(self, email: str, password: str):
        resp = self.client.post(f"{self.base_url}/api/login", json={"email": email, "password": password})
        resp.raise_for_status()
        return resp.json()["token"]

    def request(self, method: str, endpoint: str, **kwargs):
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.client.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except httpx.RequestError as e:
            print(f"HTTP request failed: {e}", file=sys.stderr)
            raise ConnectionError(f"Failed to connect to server: {e}") from e

api_client = None

class DiskIO:
    @staticmethod
    def read(path: str, size: int, offset: int) -> bytes:
        params = {"path": path, "size": size, "offset": offset}
        resp = api_client.request("GET", "/api/fs/read", params=params)
        return resp.content
    @staticmethod
    def write(path: str, buf: bytes, offset: int):
        data = {"path": path, "start": offset}
        files = {"data": io.BytesIO(buf)}
        resp = api_client.request("POST", "/api/fs/write", data=data, files=files)
        return resp.json()["written"]
    @staticmethod
    def flush(path: str):
        api_client.request("POST", "/api/fs/flush", data={"path": path})
        return 0
    @staticmethod
    def discard(path: str, size: int, offset: int):
        data = {"path": path, "size": size, "offset": offset}
        api_client.request("POST", "/api/fs/discard", data=data)
        return 0
    @staticmethod
    def create(path: str):
        api_client.request("POST", "/api/fs/create", data={"path": path})
    @staticmethod
    def truncate(path: str, length: int):
        api_client.request("POST", "/api/fs/truncate", data={"path": path, "length": length})

def nbd_negotiate(conn):
    NBD_MAGIC = 0x4e42444d41474943
    NBD_CLISERV_MAGIC = 0x00420281861253
    NBD_FLAG_HAS_FLAGS = (1 << 0)
    conn.sendall(struct.pack('>Q', NBD_MAGIC))
    conn.sendall(struct.pack('>Q', NBD_CLISERV_MAGIC))
    conn.sendall(struct.pack('>Q', DISK_SIZE_BYTES))
    conn.sendall(struct.pack('>I', NBD_FLAG_HAS_FLAGS))
    conn.sendall(b'\x00' * 124)
    return True

def nbd_loop(conn):
    while not _stop_event.is_set():
        try:
            header = conn.recv(28)
            if not header or len(header) < 28: break
            magic, type_, handle, offset, length = struct.unpack(">IIQQL", header)
            if magic != 0x25609513: break
            req_data = None
            if type_ == 1:
                req_data = b''
                while len(req_data) < length:
                    chunk_data = conn.recv(length - len(req_data))
                    if not chunk_data: raise IOError("Socket closed during write payload")
                    req_data += chunk_data
            _executor.submit(_process_and_reply, conn, handle, type_, offset, length, req_data)
        except Exception as e:
            if not _stop_event.is_set():
                print(f"Error in NBD loop: {e}", file=sys.stderr)
            break
    conn.close()

def _process_and_reply(conn, handle, type_, offset, length, req_data):
    err = 0; resp_data = b""
    try:
        if type_ == 0: resp_data = DiskIO.read(DISK_NAME, length, offset)
        elif type_ == 1: DiskIO.write(DISK_NAME, req_data, offset)
        elif type_ == 3: err = DiskIO.flush(DISK_NAME)
        elif type_ == 4: DiskIO.discard(DISK_NAME, length, offset)
        elif type_ == 2: return
        else: err = errno.EINVAL
    except Exception as e:
        print(f"Error processing NBD request:", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        err = errno.EIO
    with _socket_send_lock:
        try:
            reply = struct.pack('>IIQ', 0x67446698, err, handle)
            conn.sendall(reply)
            if type_ == 0 and err == 0 and resp_data: conn.sendall(resp_data)
        except Exception: pass

def main():
    if os.path.exists(SOCKET_PATH):
        try: os.unlink(SOCKET_PATH)
        except OSError as e: print(f"Error removing old socket file: {e}", file=sys.stderr)
    
    global api_client
    api_client = ApiClient(SERVER)
    try:
        api_client.login(config["auth"]["email"], config["auth"]["password"])
    except Exception as e:
        print(f"Login failed: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        DiskIO.create(DISK_NAME)
        DiskIO.truncate(DISK_NAME, DISK_SIZE_BYTES)
    except Exception as e:
        if "File or directory already exists" not in str(e):
            print(f"Failed to ensure disk file exists on server: {e}", file=sys.stderr)
            sys.exit(1)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(SOCKET_PATH); sock.listen(1)

    try: subprocess.call(["qemu-nbd", "--disconnect", "/dev/nbd0"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    except Exception: pass
    
    connector = subprocess.Popen(["qemu-nbd", "--connect=/dev/nbd0", "--format=raw", "--cache=writeback", f"nbd:unix:{SOCKET_PATH}"])
    
    try:
        print("Waiting for NBD connection...")
        conn, _ = sock.accept()
        print("NBD client connected.")
        if nbd_negotiate(conn):
            print("NBD negotiation successful. Starting main loop.")
            nbd_loop(conn)
    except KeyboardInterrupt:
        print("Keyboard interrupt received.")
    except Exception as e:
        print(f"Main loop error: {e}", file=sys.stderr)
    finally:
        print("Shutting down...")
        _stop_event.set()
        _executor.shutdown(wait=True)
        if connector.poll() is None:
            connector.terminate()
            try:
                connector.wait(timeout=5)
            except subprocess.TimeoutExpired:
                connector.kill()
        subprocess.call(["qemu-nbd", "--disconnect", "/dev/nbd0"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        if os.path.exists(SOCKET_PATH): os.remove(SOCKET_PATH)

if __name__ == '__main__':
    main()
