# Project Collector – Home Assistant add-on

Đọc dữ liệu thiết bị phòng máy (**SNMP / Modbus TCP / BACnet IP**) rồi gửi vào Home Assistant
qua **MQTT discovery**. Có giao diện quản lý thiết bị ngay trong HA (thêm / sửa / xoá thiết bị,
OID / thanh ghi, đọc thử, mẫu thiết bị, CSV, quét mạng).

*Reads data-center devices (SNMP / Modbus TCP / BACnet IP) and publishes them to Home Assistant via
MQTT discovery, with a built-in device manager (Ingress panel).*

```
Thiết bị  ──SNMP / Modbus / BACnet──►  Project Collector  ──MQTT──►  Mosquitto  ──►  Home Assistant
```

## Yêu cầu
- **Home Assistant OS** (có mục Settings → Add-ons), máy **amd64** hoặc **aarch64**.
- Add-on **Mosquitto broker** đang chạy + integration **MQTT** đã thêm.
- Internet lúc cài (build add-on, tải thư viện Python).
- HA đi tới được mạng thiết bị (SNMP 161/udp, Modbus 502/tcp, BACnet 47808/udp).

## Cài đặt
1. Settings → Add-ons → **Add-on store** → ⋮ → **Repositories** → dán
   `https://github.com/manh12a3/HA_Project_Collect` → **Add**.
2. Mục *SVTECH Server Room add-ons* → **Project Collector** → **Install** (build vài phút).
3. Tab **Configuration**: `site` (vd `hq`, `hn-dc2`), `room` (vd `server-room`) → Save.
   Topic MQTT: `svtech/<site>/<room>/<thiết-bị>/<điểm>`.
4. Tab **Info**: **Start**, bật **Start on boot**, **Show in sidebar**.

## Sử dụng – thanh bên HA → **Project Collector**
| Tab | Chức năng |
|---|---|
| **Devices** | Danh sách thiết bị, trạng thái đọc (số điểm OK, lần đọc cuối, lỗi), công tắc **Polling**, **Edit**, **Delete**, **+ New device** |
| Edit device | Tên, model, IP, cổng, Unit ID (Modbus) / community + version (SNMP) / device instance (BACnet), chu kỳ đọc. **Bảng điểm đo**: key, tên, OID / địa chỉ thanh ghi + kiểu thanh ghi + kiểu dữ liệu + thứ tự word, hệ số, làm tròn, đơn vị, device class, state class, *Advanced JSON* (`map` bảng mã, `bit`, `format`, `valid_min`). **▶ Test** từng điểm / tất cả, **Import / Export CSV**, **Save as template**. **Save = áp dụng ngay**, không cần restart |
| **Scan** | Quét dải IP (SNMP / Modbus TCP / BACnet) → thiết bị mới → **Create device** (chọn mẫu) |
| **Templates** | Mẫu dựng sẵn + mẫu người dùng lưu |

**Mẫu dựng sẵn** (lấy từ thiết bị chạy thật tại site HQ): APC Smart-UPS SRT (SNMP, 64 điểm),
ZTE ZXDU68 rectifier (SNMP), Power meter 3-phase (Modbus TCP, 16 điểm), Delta InsightPower EMS2000
(SNMP), cảm biến nhiệt độ / độ ẩm / ánh sáng (Modbus TCP), DPM380 (chưa kiểm chứng), Generic SNMP.

Tuỳ chọn quét và nút quét nhanh cũng có trên trang thiết bị **Project Collector <SITE>** trong
Settings → Devices & services → MQTT.

## Site không có Home Assistant – chạy độc lập bằng Docker (từ 0.7.0)
Dùng cho site / phòng máy mới: 1 máy **Ubuntu 22.04 / 24.04** tại site chạy Project Collector +
Mosquitto, **bridge** về Mosquitto của HA trung tâm. Entity của site tự xuất hiện trên HA trung tâm.

```
Thiết bị site ──► Project Collector (Docker) ──► Mosquitto site ══bridge══► Mosquitto trung tâm ──► HA
```

1. **Trên HA trung tâm:** Settings → Add-ons → **Mosquitto broker** → Configuration → `logins`, thêm
   tài khoản cho site (vd `bridge_site_02` + mật khẩu) → Save → Restart add-on.
2. **Trên máy Ubuntu tại site:**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/manh12a3/HA_Project_Collect/main/standalone/install-site.sh -o install-site.sh
   sudo bash install-site.sh
   ```
   Script hỏi: mã site (vd `SITE_02`), phòng, IP Mosquitto trung tâm, tài khoản + mật khẩu bridge,
   mật khẩu giao diện. Tự cài Docker, tạo `/opt/svtech/`, chạy Mosquitto (chỉ nghe máy này) + bridge
   (`homeassistant/# out`, `svtech/<site>/# both`) + Project Collector. Máy đã có Mosquitto riêng →
   script dùng luôn và in 2 dòng topic cần thêm vào bridge.
3. Mở `http://<IP máy site>:8099` (user `admin`) → thêm thiết bị như trong HA.
   **Đặt tên thiết bị có tiền tố site** (vd `SITE_02 UPS A`) để entity_id không trùng site khác.

| Lệnh | Việc |
|---|---|
| `sudo bash install-site.sh --update` | Lấy bản mới từ GitHub + build lại, giữ cấu hình và thiết bị |
| `sudo bash install-site.sh --uninstall` | Gỡ container, giữ `/opt/svtech/data` |
| `cd /opt/svtech && sudo docker compose logs -f collector` | Xem log |
| `sudo grep WEBUI_PASSWORD /opt/svtech/.env` | Xem mật khẩu giao diện |

Dữ liệu: `/opt/svtech/data` (= `/share/project_collector` của add-on: `devices.json`, `backups/`,
`templates/`). Giao diện qua HTTP thường – chỉ mở cổng 8099 cho mạng quản trị.

## Lưu ý
- **Một thiết bị chỉ một bộ đọc; một gateway / đường RS485 chỉ một master.** Hai hệ thống cùng hỏi
  Modbus qua một gateway → lỗi, giá trị rác. Giao diện hỏi xác nhận trước khi Test / bật Polling Modbus.
- Đổi **key** của điểm = tạo entity mới (lịch sử của entity cũ vẫn ở entity cũ). Đổi tên / IP thiết bị
  giữ nguyên entity.
- Xoá điểm / thiết bị = xoá entity khỏi HA (lịch sử recorder vẫn còn).
- Dữ liệu lưu ở `/share/project_collector/`: `devices.json` (có community - **không đưa lên Git**),
  `backups/` (10 bản gần nhất, tự tạo mỗi lần sửa), `templates/` (mẫu người dùng), `scan_settings.json`.
  Full backup của HA có thư mục `share` → đã gồm các file này.
- Chuyển từ integration "Project" cũ (giữ entity_id + lịch sử): `project_collector/tools/export_from_ha.py`
  (chạy trong add-on SSH, đọc `.storage` của HA, sinh `devices.json`), rồi xoá entry Project cũ.

## Cấu trúc
```
repository.yaml                 kho add-on
project_collector/
  config.yaml, Dockerfile       add-on (python:3.12-alpine, pymodbus, pysnmp, bacpypes3, paho-mqtt)
  app/collector.py              đọc thiết bị, MQTT discovery, Polling, quét, quản lý thiết bị
  app/client.py                 đọc Modbus / SNMP / BACnet (dùng chung với integration Project)
  app/scanner.py                quét mạng
  app/webui.py, app/web/        giao diện quản lý (Ingress)
  app/templates/*.json          mẫu thiết bị dựng sẵn
  tools/export_from_ha.py       chuyển cấu hình từ integration Project cũ
standalone/install-site.sh      cài chạy độc lập (Docker) cho site không có Home Assistant
```
