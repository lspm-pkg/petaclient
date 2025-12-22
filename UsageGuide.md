# Peta Project - Client & Server Setup

Unlimited Discord-backed storage, up to 990 PB, using PetaServer and PetaClient.

This document assumes:
- Linux (Debian/Ubuntu-style)
- Root access
- You know how to not brick your box

---

## Requirements

For server and client:
- UV (download from Astral)

For client:
- nbdkit
- libfuse-dev


Download:
- `petaserver.zip`
- `petaclient.zip`

From:
```

https://mirror.5136.cloud/projects/petaproject

```

---

## Peta Server Setup

### 1. Extract

Unzip the server into:

```

/petaserver

```

---

### 2. Configuration Files

```

cd /petaserver
cp config.toml.example config.toml
nano config.toml

```
```

cp example-env .env
nano .env

```

You **must** have both `config.toml` and `.env` configured before starting the service.

---

### 3. Server config.toml (example)

```

[network]
host = "0.0.0.0"
port = 7004

[cache]
in_memory_chunk_limit = 128

[uploads]
chunk_size = 8388608

[uploads.discord]
channel_id = 1440814868922896434

[auth]
registration_enabled = true

```

Cache math reminder:
- Each chunk = 8 MB
- 1 GB RAM ≈ 128 chunks
- RAM used ≈ `chunk_limit * 8 MB`

---

### 4. Environment Variables (.env)

```

DISCORD_BOT_TOKEN=""
SESSION_SECRET=
ENCRYPTION_KEY=

```

Notes:
- `DISCORD_BOT_TOKEN` is mandatory
- `ENCRYPTION_KEY` must be base16 (hex)
- Do not leak this file

---

### 5. Systemd Service

Create the service:

```

nano /etc/systemd/system/petaserver.service

```
```

[Unit]
Description=Peta Server

[Service]
ExecStart=/root/.local/bin/uv run -m petabytestorage
WorkingDirectory=/petaserver
User=root

[Install]
WantedBy=multi-user.target

```

Enable and start:

```

systemctl enable --now petaserver.service

```

---

### 6. Create an Account

Once the server is running:

```
curl -X 'POST' \
  'http://127.0.0.1:7004/api/register' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "email": "example@example.net",
  "password": "example",
  "terms_accepted": true
}'
```

You may also want to disable registeration after registering, just edit the config.toml; and then systemctl restart it.

---

## Peta Client Setup

### 1. Extract

Unzip the client into:

```

/petaclient

```

---

### 2. Client Configuration

```

cd /petaclient
cp config.toml.example config.toml
nano config.toml

```

Example:

```

[auth]
server = "http://127.0.0.1:7004"
email = ""
password = ""

[disk]
disk_size_gb = 256.0

```

Notes:
- `disk_size_gb` is a float
- This is the exposed virtual disk size, not RAM usage

---

### 3. Installing depends

Just run this:
```
apt-get install -y nbdkit qemu-utils libfuse-dev pkg-config
```

If you're using proxmox as the host:
```
apt-get install -y nbdkit libfuse-dev pkg-config
```

This also works on arch(btw) but I'm too lazy to give the comamnds.

---

### 3. Systemd Service

Create the service:

```

nano /etc/systemd/system/petaclient.service

```
```

[Unit]
Description=Peta Client
After=petabyte.service

[Service]
ExecStart=/root/.local/bin/uv run main.py
WorkingDirectory=/petaclient
User=root

[Install]
WantedBy=multi-user.target

```

Enable and start:

```

systemctl enable --now petaclient.service

```

---

## Notes / Warnings

- Chunk size should not be increased unless you have level 1+ In boosts and are sure to get even more ping.
- This is storage-through-Discord; treat it accordingly
- Backups are still your problem

