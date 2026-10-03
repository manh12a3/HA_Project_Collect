#!/usr/bin/env bash
# =============================================================================
# install-site.sh - cài Project Collector cho 1 SITE mới (Ubuntu 22.04 / 24.04, Docker)
#
#   Thiết bị --SNMP/Modbus/BACnet--> Project Collector (Docker) --MQTT--> Mosquitto site
#                                         --bridge--> Mosquitto trung tâm --> Home Assistant
#
# Cài mới (hỏi từng câu):     sudo bash install-site.sh
# Cài không hỏi:              sudo BRIDGE_PASS='...' WEBUI_PASS='...' bash install-site.sh \
#                               --site SITE_02 --center 192.168.21.48 --bridge-user bridge_site_02 --yes
# Cập nhật bản mới:           sudo bash install-site.sh --update
# Gỡ (GIỮ dữ liệu thiết bị):  sudo bash install-site.sh --uninstall
#
# Thư mục cài: /opt/svtech  (data/ = devices.json + mẫu + sao lưu - chạy lại script KHÔNG mất)
# Mật khẩu chỉ nhập qua câu hỏi (ẩn) hoặc biến môi trường BRIDGE_PASS / WEBUI_PASS, không qua tham số.
# =============================================================================
set -euo pipefail

REPO_TARBALL="https://codeload.github.com/manh12a3/HA_Project_Collect/tar.gz/refs/heads/main"
DEST="/opt/svtech"
SITE="" ; ROOM="" ; CENTER="" ; CENTER_PORT="" ; BRIDGE_USER="" ; SCAN_RANGES=""
BRIDGE_PASS="${BRIDGE_PASS:-}" ; WEBUI_PASS="${WEBUI_PASS:-}"
SOURCE="" ; MQTT_MODE="auto" ; ACTION="install" ; ASSUME_YES=0 ; WEBUI_PORT="8099"

c_ok()   { printf '\033[32m%s\033[0m\n' "$*"; }
c_warn() { printf '\033[33m%s\033[0m\n' "$*"; }
die()    { printf '\033[31mLỖI: %s\033[0m\n' "$*" >&2; exit 1; }
step()   { printf '\n\033[36m== %s\033[0m\n' "$*"; }
slug()   { printf '%s' "$1" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'; }

usage() { awk 'NR>1 && /^#/ {sub(/^# ?/, ""); print; next} NR>1 {exit}' "$0"; cat <<'EOF'
Tham số:
  --site CODE          mã site (vd SITE_02) -> topic svtech/<site-slug>/<room>/...
  --room CODE          mã phòng (mặc định server-room)
  --center IP          IP Mosquitto trung tâm (Home Assistant chính)
  --center-port N      cổng MQTT trung tâm (mặc định 1883)
  --bridge-user USER   tài khoản Mosquitto trung tâm dành cho site này
  --scan-ranges LIST   dải IP mặc định cho chức năng quét (vd 192.168.30.0/24)
  --mosquitto MODE     auto | container (Mosquitto trong Docker) | existing (dùng Mosquitto có sẵn)
  --source DIR         dùng mã add-on trong DIR (thư mục project_collector) thay vì tải GitHub
  --update             tải mã mới + build lại, giữ nguyên cấu hình và dữ liệu
  --uninstall          dừng + xoá container (giữ /opt/svtech/data)
  --yes                không hỏi xác nhận
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --site) SITE="$2"; shift 2 ;;
    --room) ROOM="$2"; shift 2 ;;
    --center) CENTER="$2"; shift 2 ;;
    --center-port) CENTER_PORT="$2"; shift 2 ;;
    --bridge-user) BRIDGE_USER="$2"; shift 2 ;;
    --scan-ranges) SCAN_RANGES="$2"; shift 2 ;;
    --mosquitto) MQTT_MODE="$2"; shift 2 ;;
    --source) SOURCE="$2"; shift 2 ;;
    --update) ACTION="update"; shift ;;
    --uninstall) ACTION="uninstall"; shift ;;
    --yes|-y) ASSUME_YES=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "tham số không hiểu: $1 (xem --help)" ;;
  esac
done

[ "$(id -u)" -eq 0 ] || die "chạy bằng sudo / root"
cd /

compose() { docker compose --project-directory "$DEST" -f "$DEST/docker-compose.yml" "$@"; }

# ---------------------------------------------------------------- gỡ cài đặt
if [ "$ACTION" = "uninstall" ]; then
  [ -f "$DEST/docker-compose.yml" ] || die "chưa cài ($DEST/docker-compose.yml không có)"
  if [ "$ASSUME_YES" -ne 1 ]; then
    read -rp "Dừng và xoá container Project Collector (dữ liệu $DEST/data GIỮ LẠI)? [y/N] " a
    [ "${a,,}" = "y" ] || exit 0
  fi
  COMPOSE_PROFILES=mosquitto compose down --rmi local || true
  c_ok "Đã gỡ container. Dữ liệu còn ở $DEST/data (xoá tay nếu không cần)."
  exit 0
fi

# ---------------------------------------------------------------- đọc cấu hình cũ (cập nhật / chạy lại)
if [ -f "$DEST/.env" ]; then
  # shellcheck disable=SC1091
  OLD_SITE="$(. "$DEST/.env"; echo "${SITE:-}")"
  [ -n "$SITE" ] || SITE="$OLD_SITE"
  [ -n "$ROOM" ] || ROOM="$(. "$DEST/.env"; echo "${ROOM:-}")"
  [ -n "$CENTER" ] || CENTER="$(. "$DEST/.env"; echo "${CENTER:-}")"
  [ -n "$CENTER_PORT" ] || CENTER_PORT="$(. "$DEST/.env"; echo "${CENTER_PORT:-}")"
  [ -n "$BRIDGE_USER" ] || BRIDGE_USER="$(. "$DEST/.env"; echo "${BRIDGE_USER:-}")"
  [ -n "$SCAN_RANGES" ] || SCAN_RANGES="$(. "$DEST/.env"; echo "${SCAN_RANGES:-}")"
  [ -n "$WEBUI_PASS" ] || WEBUI_PASS="$(. "$DEST/.env"; echo "${WEBUI_PASSWORD:-}")"
  if [ "$MQTT_MODE" = "auto" ]; then MQTT_MODE="$(. "$DEST/.env"; echo "${MQTT_MODE:-auto}")"; fi
elif [ "$ACTION" = "update" ]; then
  die "chưa cài - chạy không có --update trước"
fi

ask() {  # ask VAR "câu hỏi" "mặc định"
  local __v="$1" __q="$2" __d="${3:-}" __a
  [ -n "${!__v}" ] && return 0
  if [ "$ASSUME_YES" -eq 1 ]; then
    [ -n "$__d" ] || die "thiếu --${__v,,} (chạy --yes phải đủ tham số)"
    printf -v "$__v" '%s' "$__d"; return 0
  fi
  read -rp "$__q${__d:+ [$__d]}: " __a
  printf -v "$__v" '%s' "${__a:-$__d}"
  [ -n "${!__v}" ] || die "$__q: không được để trống"
}
ask_secret() {  # ask_secret VAR "câu hỏi"
  local __v="$1" __q="$2" __a __b
  [ -n "${!__v}" ] && return 0
  [ "$ASSUME_YES" -eq 1 ] && die "thiếu $__v (đặt biến môi trường $__v)"
  while :; do
    read -rsp "$__q: " __a; echo
    read -rsp "Nhập lại: " __b; echo
    [ -n "$__a" ] && [ "$__a" = "$__b" ] && break
    c_warn "Trống hoặc không khớp, nhập lại."
  done
  printf -v "$__v" '%s' "$__a"
}
check_value() {  # giá trị ghi vào .env trong nháy đơn -> không chứa ' và xuống dòng
  case "$2" in *"'"*|*$'\n'*) die "$1 không được chứa dấu nháy đơn hoặc xuống dòng" ;; esac
}

step "Cấu hình site"
ask SITE "Mã site (vd SITE_02)"
ask ROOM "Mã phòng" "server-room"
ask CENTER "IP Mosquitto trung tâm"
ask CENTER_PORT "Cổng MQTT trung tâm" "1883"
SITE_SLUG="$(slug "$SITE")" ; ROOM_SLUG="$(slug "$ROOM")"
[ -n "$SITE_SLUG" ] || die "mã site không hợp lệ"
if [ -n "${OLD_SITE:-}" ] && [ "$(slug "$OLD_SITE")" != "$SITE_SLUG" ]; then
  die "máy này đã cài site '$OLD_SITE' - đổi mã site sẽ tạo lại toàn bộ entity. Gỡ (--uninstall) trước nếu thật sự muốn."
fi
ask SCAN_RANGES "Dải IP mặc định để quét thiết bị (sửa lại được trong HA)" "192.168.1.0/24"

# ---------------------------------------------------------------- Mosquitto: container hay có sẵn
port_1883_busy() { ss -ltnH 2>/dev/null | awk '$4 ~ /(^|:)1883$/ {f=1} END {exit !f}'; }
has_container() { local names; names="$(docker ps -a --format '{{.Names}}' 2>/dev/null || true)"; grep -qx "$1" <<<"$names"; }
gen_pass() { local p; p="$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')"; printf '%s' "${p:0:20}"; }
if [ "$MQTT_MODE" = "auto" ]; then
  if has_container svtech-mosquitto; then MQTT_MODE="container"
  elif port_1883_busy || systemctl is-active --quiet mosquitto 2>/dev/null; then MQTT_MODE="existing"
  else MQTT_MODE="container"; fi
fi
case "$MQTT_MODE" in container|existing) ;; *) die "--mosquitto phải là auto|container|existing" ;; esac
echo "Mosquitto: $MQTT_MODE"

NEED_BRIDGE_CONF=0
if [ "$MQTT_MODE" = "container" ]; then
  # chưa có cấu hình bridge, hoặc truyền mật khẩu mới (BRIDGE_PASS) -> ghi lại; còn lại giữ nguyên
  if [ ! -f "$DEST/mosquitto/config/mosquitto.conf" ] || [ -n "$BRIDGE_PASS" ]; then
    NEED_BRIDGE_CONF=1
  fi
  if [ "$NEED_BRIDGE_CONF" -eq 1 ]; then
    ask BRIDGE_USER "Tài khoản Mosquitto trung tâm cho site này" "bridge_$(echo "$SITE_SLUG" | tr '-' '_')"
    ask_secret BRIDGE_PASS "Mật khẩu tài khoản $BRIDGE_USER"
  fi
fi

LOCAL_MQTT_USER="${LOCAL_MQTT_USER:-}" ; LOCAL_MQTT_PASS="${LOCAL_MQTT_PASS:-}"
if [ "$MQTT_MODE" = "existing" ]; then
  if [ -f "$DEST/.env" ]; then
    [ -n "$LOCAL_MQTT_USER" ] || LOCAL_MQTT_USER="$(. "$DEST/.env"; echo "${MQTT_USERNAME:-}")"
    [ -n "$LOCAL_MQTT_PASS" ] || LOCAL_MQTT_PASS="$(. "$DEST/.env"; echo "${MQTT_PASSWORD:-}")"
  fi
  if [ -z "$LOCAL_MQTT_USER" ] && [ "$ASSUME_YES" -ne 1 ]; then
    read -rp "Mosquitto có sẵn trên máy này cần tài khoản? Nhập user (bỏ trống = ẩn danh): " LOCAL_MQTT_USER
    [ -z "$LOCAL_MQTT_USER" ] || ask_secret LOCAL_MQTT_PASS "Mật khẩu $LOCAL_MQTT_USER"
  fi
fi

if [ -z "$WEBUI_PASS" ]; then
  if [ "$ASSUME_YES" -eq 1 ]; then
    WEBUI_PASS="$(gen_pass)"
  else
    read -rsp "Mật khẩu giao diện quản lý (user admin), bỏ trống = tự sinh: " WEBUI_PASS; echo
    [ -n "$WEBUI_PASS" ] || WEBUI_PASS="$(gen_pass)"
  fi
fi
for v in SITE ROOM CENTER CENTER_PORT BRIDGE_USER SCAN_RANGES WEBUI_PASS BRIDGE_PASS LOCAL_MQTT_USER LOCAL_MQTT_PASS; do check_value "$v" "${!v}"; done

if [ "$ASSUME_YES" -ne 1 ]; then
  echo
  echo "  Site / phòng : $SITE / $ROOM   (topic svtech/$SITE_SLUG/$ROOM_SLUG/...)"
  echo "  Trung tâm    : $CENTER:$CENTER_PORT   (Mosquitto: $MQTT_MODE${BRIDGE_USER:+, user $BRIDGE_USER})"
  echo "  Thư mục cài  : $DEST"
  read -rp "Tiếp tục? [Y/n] " a
  [ "${a,,}" != "n" ] || exit 0
fi

# ---------------------------------------------------------------- Docker
step "Docker"
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  . /etc/os-release
  [ "${ID:-}" = "ubuntu" ] || c_warn "Hệ điều hành ${PRETTY_NAME:-?} chưa thử - vẫn cài bằng apt"
  apt-get update -q
  apt-get install -y -q docker.io docker-compose-v2 || apt-get install -y -q docker.io docker-compose-plugin
  systemctl enable --now docker
fi
docker compose version >/dev/null 2>&1 || die "không có 'docker compose'"
c_ok "$(docker --version)"

# ---------------------------------------------------------------- mã nguồn
step "Mã Project Collector"
mkdir -p "$DEST/data" "$DEST/mosquitto/config" "$DEST/mosquitto/data"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
if [ -n "$SOURCE" ]; then
  [ -f "$SOURCE/Dockerfile" ] && [ -d "$SOURCE/app" ] || die "--source phải là thư mục project_collector (có Dockerfile, app/)"
  cp -a "$SOURCE" "$TMP/src"
else
  command -v curl >/dev/null 2>&1 || apt-get install -y -q curl
  curl -fsSL "$REPO_TARBALL" -o "$TMP/repo.tgz" || die "không tải được $REPO_TARBALL"
  tar -xzf "$TMP/repo.tgz" -C "$TMP"
  SRC_DIR="$(find "$TMP" -mindepth 2 -maxdepth 2 -type d -name project_collector -print -quit)"
  [ -n "$SRC_DIR" ] || die "gói tải về không có thư mục project_collector"
  cp -a "$SRC_DIR" "$TMP/src"
fi
rm -rf "$DEST/src"; mv "$TMP/src" "$DEST/src"
VERSION="$(awk -F'"' '/^version:/ {print $2; exit}' "$DEST/src/config.yaml")"
c_ok "Project Collector $VERSION"

# ---------------------------------------------------------------- cấu hình
step "Ghi cấu hình"
PROFILES=""; [ "$MQTT_MODE" = "container" ] && PROFILES="mosquitto"
umask 077
cat > "$DEST/.env" <<EOF
# Sinh bởi install-site.sh $(date '+%Y-%m-%d %H:%M') - chạy lại script để đổi, đừng sửa tay khi không cần
SITE='$SITE'
ROOM='$ROOM'
LOG_LEVEL='info'
SCAN_RANGES='$SCAN_RANGES'
MQTT_HOST='127.0.0.1'
MQTT_PORT='1883'
MQTT_USERNAME='$LOCAL_MQTT_USER'
MQTT_PASSWORD='$LOCAL_MQTT_PASS'
WEBUI_PORT='$WEBUI_PORT'
WEBUI_PASSWORD='$WEBUI_PASS'
COMPOSE_PROFILES='$PROFILES'
# chỉ để script đọc lại:
MQTT_MODE='$MQTT_MODE'
CENTER='$CENTER'
CENTER_PORT='$CENTER_PORT'
BRIDGE_USER='$BRIDGE_USER'
EOF
chmod 600 "$DEST/.env"
umask 022

cat > "$DEST/docker-compose.yml" <<'EOF'
# Sinh bởi install-site.sh - Project Collector chạy độc lập cho 1 site.
#   cd /opt/svtech && docker compose logs -f collector
services:
  mosquitto:
    profiles: ["mosquitto"]
    image: eclipse-mosquitto:2
    container_name: svtech-mosquitto
    restart: unless-stopped
    network_mode: host
    volumes:
      - ./mosquitto/config:/mosquitto/config
      - ./mosquitto/data:/mosquitto/data
  collector:
    build: ./src
    image: svtech/project-collector:local
    container_name: svtech-project-collector
    restart: unless-stopped
    network_mode: host
    env_file: .env
    volumes:
      - ./data:/share/project_collector
    logging:
      driver: json-file
      options: {max-size: "10m", max-file: "3"}
EOF

BRIDGE_TOPICS="topic homeassistant/# out 1
topic svtech/$SITE_SLUG/# both 1"

if [ "$MQTT_MODE" = "container" ] && [ "$NEED_BRIDGE_CONF" -eq 1 ]; then
  cat > "$DEST/mosquitto/config/mosquitto.conf" <<EOF
# Sinh bởi install-site.sh - Mosquitto của site $SITE (chỉ nghe máy này) + bridge về trung tâm
listener 1883 127.0.0.1
allow_anonymous true
persistence true
persistence_location /mosquitto/data/
log_dest stdout
log_type error
log_type warning
log_type notice

connection svtech-center
address $CENTER:$CENTER_PORT
remote_clientid svtech-bridge-$SITE_SLUG
remote_username $BRIDGE_USER
remote_password $BRIDGE_PASS
try_private true
cleansession true
restart_timeout 5 60
# discovery đi LÊN (HA trung tâm tự tạo entity); số liệu lên + lệnh Polling/Scan/Settings xuống
$BRIDGE_TOPICS
EOF
  chown -R 1883:1883 "$DEST/mosquitto"
  chmod 640 "$DEST/mosquitto/config/mosquitto.conf"
fi

if [ "$MQTT_MODE" = "existing" ]; then
  if grep -rqsE "^[[:space:]]*topic[[:space:]]+svtech/$SITE_SLUG/#" /etc/mosquitto/ \
     && grep -rqsE "^[[:space:]]*topic[[:space:]]+homeassistant/#" /etc/mosquitto/; then
    c_ok "Bridge Mosquitto có sẵn đã có topic svtech/$SITE_SLUG/# và homeassistant/#"
  else
    c_warn "Mosquitto có sẵn: thêm 2 dòng sau vào khối 'connection' bridge (vd /etc/mosquitto/conf.d/*.conf)"
    c_warn "rồi 'systemctl restart mosquitto':"
    printf '%s\n' "$BRIDGE_TOPICS"
  fi
fi

# ---------------------------------------------------------------- chạy
step "Khởi động"
if [ -n "$CENTER" ] && ! timeout 3 bash -c "</dev/tcp/$CENTER/$CENTER_PORT" 2>/dev/null; then
  c_warn "Chưa kết nối được $CENTER:$CENTER_PORT từ máy này (tường lửa / sai IP?) - vẫn tiếp tục."
fi
compose up -d --build --remove-orphans
[ "$MQTT_MODE" = "container" ] && compose restart mosquitto >/dev/null 2>&1 || true

step "Kiểm tra"
ok=0
for _ in $(seq 1 20); do
  LOGS="$(docker logs svtech-project-collector 2>&1 || true)"
  if grep -q "Đã kết nối MQTT" <<<"$LOGS"; then ok=1; break; fi
  sleep 2
done
if [ "$ok" -eq 1 ]; then c_ok "Collector đã kết nối MQTT cục bộ"; else
  c_warn "Collector chưa báo kết nối MQTT - xem: cd $DEST && docker compose logs collector"; fi
if [ "$MQTT_MODE" = "container" ]; then
  sleep 3
  LOGS="$(docker logs svtech-mosquitto 2>&1 || true)"
  if grep -qiE "not authori[sz]ed|connection refused|error" <<<"$LOGS"; then
    c_warn "Bridge có lỗi (tài khoản / mật khẩu / mạng):"; tail -n 5 <<<"$LOGS"
  else
    c_ok "Bridge đang kết nối $CENTER:$CENTER_PORT (xác nhận: log Mosquitto trung tâm có 'svtech-bridge-$SITE_SLUG')"
  fi
fi

IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
echo
c_ok "XONG. Giao diện quản lý: http://${IP:-<IP máy>}:$WEBUI_PORT   (user: admin)"
echo "  Mật khẩu: sudo grep WEBUI_PASSWORD $DEST/.env"
echo "  Trên HA trung tâm: thiết bị 'Project Collector $(echo "$SITE_SLUG" | tr 'a-z' 'A-Z')' (MQTT) + entity của từng thiết bị."
echo "  Đặt tên thiết bị có tiền tố site (vd '$SITE UPS A') để entity_id không trùng site khác."
if command -v ufw >/dev/null 2>&1 && grep -q "Status: active" <<<"$(ufw status 2>/dev/null || true)"; then
  c_warn "ufw đang bật: mở giao diện bằng 'sudo ufw allow $WEBUI_PORT/tcp' (nên giới hạn IP quản trị)."
fi
