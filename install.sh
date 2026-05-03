#!/usr/bin/env bash

# sudo systemctl stop notifier 2>/dev/null || true && curl -fsSL https://raw.githubusercontent.com/berserkkv/notifier/main/install.sh | sudo bash

set -e

APP_NAME="notifier"
INSTALL_DIR="/usr/local/bin"

BIN_PATH="$INSTALL_DIR/$APP_NAME"
CONFIG_PATH="$INSTALL_DIR/config.json"
SERVICE_FILE="/etc/systemd/system/$APP_NAME.service"

BIN_URL="https://raw.githubusercontent.com/berserkkv/notifier/main/notifier"
CONFIG_URL="https://raw.githubusercontent.com/berserkkv/notifier/main/config.json"

echo "Downloading binary..."
curl -L "$BIN_URL" -o "$BIN_PATH"

echo "Downloading config..."
curl -L "$CONFIG_URL" -o "$CONFIG_PATH"

echo "Setting permissions..."
chmod +x "$BIN_PATH"
chmod 644 "$CONFIG_PATH"

echo "Creating systemd service..."
sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=Notifier Service
After=network.target

[Service]
ExecStart=$BIN_PATH -config $CONFIG_PATH
WorkingDirectory=$INSTALL_DIR
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
EOF

echo "Reloading systemd..."
systemctl daemon-reload

echo "Enabling service..."
systemctl enable $APP_NAME

echo "Starting service..."
systemctl restart $APP_NAME

echo "Done. Service status:"
systemctl status $APP_NAME --no-pager