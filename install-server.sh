#!/bin/bash
set -e
echo "Starting Peta Server setup..."

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
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
apt-get install -y git curl nano

cd /
if [ ! -d "petaserver" ]; then
    git clone https://github.com/lspm-pkg/petaserver.git
else
    echo "petaserver already exists, skipping clone"
fi

cd /petaserver
cp -n config.toml.example config.toml
cp -n example-env .env
echo "Please edit /petaserver/config.toml and /petaserver/.env with your settings."
echo "Press Enter to continue after editing..."
read

cat <<EOF >/etc/systemd/system/petaserver.service
[Unit]
Description=Peta Server

[Service]
ExecStart=/root/.local/bin/uv run -m petabytestorage
WorkingDirectory=/petaserver
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now petaserver.service

echo "Peta Server setup complete. Check service status with:"
echo "systemctl status petaserver.service"
echo
echo "You can now start configuring the client by using the install-client.sh included in the repo."
