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
  debian|ubuntu|pve|proxmox)
    echo "Compatible OS detected: $ID"
    ;;
  *)
    echo "Unsupported OS detected: $ID"
    echo "This installer only supports Debian, Ubuntu, or Proxmox"
    exit 1
    ;;
esac

apt-get update

if [[ "$ID" == "pve" || "$ID" == "proxmox" ]]; then
  echo "Proxmox detected, skipping qemu-utils"
  apt-get install -y nbdkit libfuse-dev pkg-config git
else
  echo "Standard Debian/Ubuntu detected"
  apt-get install -y nbdkit libfuse-dev pkg-config qemu-utils git
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
    echo "Account successfully created!"
    # Pre-fill config.toml with email, password, and server URL
    sed -i "s|server = .*|server = \"$server_url\"|" config.toml
    sed -i "s|email = .*|email = \"$user_email\"|" config.toml
    sed -i "s|password = .*|password = \"$user_password\"|" config.toml
  else
    echo "Failed to create account. HTTP status code: $response"
  fi
fi

cd /petaclient

cp -n config.toml.example config.toml

echo "Please edit /petaclient/config.toml with your settings."
echo "Press Enter to continue after editing..."
read

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
ExecStart=modprobe nbd && /root/.local/bin/uv run main.py
WorkingDirectory=/petaclient
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
echo "It is best to not use discard when formatting to avoid wasting time + on large sizes it could take a very long time."
