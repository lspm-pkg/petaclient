#!/bin/bash
echo "Starting Peta Client setup..."

if [ "$EUID" -ne 0 ]; then
  echo "Run this as root"
  exit 1
fi

if [ ! -f /etc/os-release ]; then
  echo "/etc/os-release missing, cannot determine OS"
  exit 1
fi

. /etc/os-release

case "$ID" in
  debian|ubuntu)
    echo "Debian/Ubuntu detected: $ID"
    ;;
  *)
    echo "Unknown base OS: $ID"
    ;;
esac

if dpkg -s proxmox-ve >/dev/null 2>&1; then
  IS_PVE=1
  echo "Proxmox VE detected via proxmox-ve package"
else
  IS_PVE=0
  echo "Not Proxmox VE"
fi

if ! command -v /root/.local/bin/uv &>/dev/null; then
    echo "uv not found, installing via Astral..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

source /root/.local/bin/env

apt-get update

if [ "$IS_PVE" -eq 1 ]; then
  apt-get install -y nbdkit libfuse-dev pkg-config git build-essential python3-dev
else
  apt-get install -y nbdkit libfuse-dev pkg-config qemu-utils git build-essential python3-dev
fi

cd /
if [ ! -d petaclient ]; then
  git clone https://github.com/lspm-pkg/petaclient.git
else
  echo "petaclient already exists, skipping clone"
fi

read -p "Do you want to create an account on the server now? [y/N]: " create_account

if [[ "$create_account" =~ ^[Yy]$ ]]; then
  read -p "Enter server URL (e.g., http://127.0.0.1:7004): " server_url
  read -p "Enter email: " user_email
  read -s -p "[Will not show] Enter password: " user_password
  echo
  read -s -p "[Will not show] Confirm password: " user_password2
  echo
  if [[ "$user_password" != "$user_password2" ]]; then
    echo "Passwords do not match. Exiting."
    exit 1
  fi
  response=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$server_url/api/register" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "{\"email\":\"$user_email\",\"password\":\"$user_password\",\"terms_accepted\":true}")
  if [[ "$response" == "200" || "$response" == "201" ]]; then
    echo Success.
  else
    echo "Failed to create account. HTTP status code: $response"
  fi
fi

cd /petaclient

cp -n config.example.toml config.toml

echo "Please edit /petaclient/config.toml with your settings."
echo "Press Enter to continue..."
read
nano /petaclient/config.toml

if systemctl list-unit-files | grep -q '^petaserver\.service'; then
    after_line="After=petaserver.service"
else
    after_line=""
fi

cat >/etc/systemd/system/petaclient.service <<EOF
[Unit]
Description=Peta Client
$after_line

[Service]
ExecStartPre=/usr/sbin/modprobe nbd
ExecStart=/root/.local/bin/uv run main.py
WorkingDirectory=/petaclient
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now petaclient.service

echo "Peta Client installed"
echo ""
echo "systemctl status petaclient"
echo ""
echo "If it's running/active, then you can use the block device at /dev/nbd0"
echo ""
echo "Recommended format command:"
echo "mkfs.btrfs --nodiscard /dev/nbd0"
echo ""
echo "Avoid discard on large devices unless you enjoy waiting."
