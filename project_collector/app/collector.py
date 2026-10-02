"""Project Collector - đọc thiết bị (Modbus TCP / SNMP / BACnet) rồi gửi qua MQTT.

Mô hình 2 (2026-10-02): HA không tự đọc thiết bị nữa mà nhận dữ liệu qua MQTT discovery.
Phần đọc thiết bị = client.py của integration Project (bản sao, giữ nguyên cách đọc đã
chạy ổn: khoá theo gateway, hỏi lại lỗi mã 11, SNMP gộp 10 OID/gói...). Logic lọc rác +
giữ giá trị khi lỗi chép từ custom_components/project/coordinator.py.

Cấu hình: /share/project_collector/devices.json (sinh bằng tools/export_from_ha.py từ
integration Project, rồi sửa tay khi thêm thiết bị):
  {"devices": [{"id", "name", "model", "protocol", "host", "port", "unit_id",
                "community", "snmp_version", "device_instance", "scan_interval",
                "enabled", "points": [{...điểm như integration Project...,
                                       "entity_id", "enabled"}]}]}

Topic: svtech/<site>/<room>/<device>/<key>          giá trị (retain)
       svtech/<site>/<room>/<device>/<key>/avail    online|offline (retain)
       svtech/<site>/<room>/<device>/<key>/attr     {"options": [...]} (retain)
       svtech/<site>/<room>/collector/status        online|offline (LWT, retain)
       homeassistant/sensor/pc_<id>_<key>/config    discovery (retain)
Entity giữ NGUYÊN entity_id cũ qua `default_entity_id` -> lịch sử / thống kê nối tiếp.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import os
import re
import signal
import threading
import time
import urllib.request

import paho.mqtt.client as mqtt

from client import bacnet_read_point, format_snmp_value, modbus_read_points, snmp_get_points
from scanner import run_scan

CONFIG_FILE = "/share/project_collector/devices.json"
SCAN_FILE = "/share/project_collector/scan_result.json"
VERSION = "0.5.0"
OPTIONS_FILE = "/data/options.json"
DISCOVERY_PREFIX = "homeassistant"

# --- giống coordinator.py của integration Project ---
MAX_ABS_VALUE = 1e12
TOTAL_MAX_JUMP = 0.05
TOTAL_MIN_JUMP = 100
CONFIRM_READS = 3
OFFLINE_AFTER = 3  # Modbus: lỗi liên tiếp đủ số lần này mới báo mất kết nối

log = logging.getLogger("project_collector")
_stop = threading.Event()


def slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")
    return s or "device"


def load_options() -> dict:
    try:
        with open(OPTIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def mqtt_service() -> dict:
    """Lấy thông tin broker từ Supervisor (services: mqtt:need)."""
    token = os.environ.get("SUPERVISOR_TOKEN", "")
    req = urllib.request.Request("http://supervisor/services/mqtt",
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)["data"]


class Device:
    """1 thiết bị = 1 luồng đọc theo chu kỳ, phát giá trị từng điểm lên MQTT."""

    def __init__(self, cfg: dict, base: str, mq: mqtt.Client):
        self.cfg = cfg
        self.id = cfg["id"]
        self.name = cfg["name"]
        self.protocol = cfg["protocol"]
        self.enabled = bool(cfg.get("enabled", True))
        # Đọc MỌI điểm như integration Project (kể cả điểm người dùng tắt entity - "enabled": false
        # chỉ làm entity mặc định bị tắt trong HA).
        self.points = list(cfg.get("points", []))
        self.topic = f"{base}/{slug(self.name)}"
        self.mq = mq
        self.interval = max(5, int(cfg.get("scan_interval") or 30))
        self._last_good: dict = {}
        self._suspect: dict = {}
        self._held: dict = {}
        self._fails: dict = {}
        self._thread = None
        self._halt = threading.Event()  # dừng riêng thiết bị này (công tắc Polling)
        self.polling_cmd = f"{self.topic}/_polling/set"
        self.polling_state = f"{self.topic}/_polling/state"
        self.on_change = None  # callback lưu devices.json khi bật/tắt (gán từ main)

    # ---------- discovery ----------
    def publish_discovery(self, collector_status: str) -> None:
        dev = {
            "identifiers": [f"project_collector_{self.id}"],
            "name": self.name,
            "manufacturer": "Project",
            "model": self.cfg.get("model") or "Project Device",
        }
        # Công tắc bật/tắt đọc thiết bị (2026-10-02) - mục Configuration của từng thiết bị trong HA
        self.mq.publish(f"{DISCOVERY_PREFIX}/switch/pc_{self.id}_polling/config", json.dumps({
            "name": "Polling", "unique_id": f"pc_{self.id}_polling", "command_topic": self.polling_cmd,
            "state_topic": self.polling_state, "payload_on": "ON", "payload_off": "OFF",
            "availability_topic": collector_status, "entity_category": "config", "icon": "mdi:sync",
            "device": dev, "origin": {"name": "Project Collector", "sw_version": VERSION},
        }, ensure_ascii=False), qos=1, retain=True)
        self.mq.publish(self.polling_state, "ON" if self.enabled else "OFF", qos=1, retain=True)
        for p in self.points:
            key = p["key"]
            cfg = {
                "name": p.get("label") or key,
                "unique_id": f"pc_{self.id}_{key}",
                "state_topic": f"{self.topic}/{key}",
                "availability": [{"topic": collector_status}, {"topic": f"{self.topic}/{key}/avail"}],
                "availability_mode": "all",
                "device": dev,
                "origin": {"name": "Project Collector", "sw_version": VERSION},
            }
            if p.get("entity_id"):
                cfg["default_entity_id"] = p["entity_id"]
            for src, dst in (("unit", "unit_of_measurement"), ("device_class", "device_class"),
                             ("state_class", "state_class")):
                if p.get(src):
                    cfg[dst] = p[src]
            if p.get("enabled") is False:
                cfg["enabled_by_default"] = False
            options = self._options(p)
            if options:
                cfg["json_attributes_topic"] = f"{self.topic}/{key}/attr"
                self.mq.publish(cfg["json_attributes_topic"], json.dumps({"options": options}), qos=1, retain=True)
            self.mq.publish(f"{DISCOVERY_PREFIX}/sensor/pc_{self.id}_{key}/config",
                            json.dumps(cfg, ensure_ascii=False), qos=1, retain=True)
            if not self.enabled:
                self.mq.publish(f"{self.topic}/{key}/avail", "offline", qos=1, retain=True)

    @staticmethod
    def _options(point: dict):
        """Giống sensor.py extra_state_attributes: giá trị chữ có thể có (map / bit)."""
        if isinstance(point.get("map"), dict):
            items = sorted(point["map"].items(),
                           key=lambda kv: (not kv[0].isdigit(), int(kv[0]) if kv[0].isdigit() else 0, kv[0]))
            return list(dict.fromkeys(v for _, v in items))
        if "bit" in point:
            return [point.get("on", "Yes"), point.get("off", "No")]
        return None

    # ---------- đọc ----------
    def read_once(self) -> dict:
        c = self.cfg
        result: dict = {}
        if self.protocol == "modbus":
            raw = modbus_read_points(c["host"], int(c["port"]), int(c.get("unit_id") or 1), self.points)
            for p in self.points:
                key, value = p["key"], raw.get(p["key"])
                if isinstance(value, Exception):
                    fails = self._fails.get(key, 0) + 1
                    self._fails[key] = fails
                    if fails < OFFLINE_AFTER and self._held.get(key) is not None:
                        log.warning("%s: lỗi đọc %s lần %s/%s - giữ giá trị cũ: %s",
                                    self.name, key, fails, OFFLINE_AFTER, value)
                        result[key] = self._held[key]
                    else:
                        log.warning("%s: lỗi đọc %s lần %s: %s", self.name, key, fails, value)
                        result[key] = None
                else:
                    self._fails.pop(key, None)
                    result[key] = self._sanitize(p, value)
                    self._held[key] = result[key]
            return result
        if self.protocol == "snmp":
            oids = list(dict.fromkeys(p["oid"] for p in self.points))
            raw = snmp_get_points(c["host"], int(c.get("port") or 161), c.get("community") or "public",
                                  c.get("snmp_version") or "2c", oids)
            for p in self.points:
                value = raw.get(p["oid"])
                try:
                    if isinstance(value, Exception):
                        raise value
                    result[p["key"]] = self._sanitize(p, format_snmp_value(p, value))
                except Exception as err:  # noqa: BLE001
                    log.warning("%s: lỗi đọc %s: %s", self.name, p["key"], err)
                    result[p["key"]] = None
            return result
        if self.protocol == "bacnet":
            for p in self.points:
                try:
                    value = asyncio.run(bacnet_read_point(
                        c["host"], int(c.get("port") or 47808), int(c.get("device_instance") or 999999),
                        p["object_type"], p["object_instance"], p["property_name"]))
                    result[p["key"]] = self._sanitize(p, value)
                except Exception as err:  # noqa: BLE001
                    log.warning("%s: lỗi đọc %s: %s", self.name, p["key"], err)
                    result[p["key"]] = None
        return result

    def _sanitize(self, point: dict, value):
        """Bỏ giá trị rác - y hệt coordinator.py của integration Project."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return value
        key = point["key"]
        if not math.isfinite(value) or abs(value) > MAX_ABS_VALUE:
            log.warning("%s: bỏ giá trị rác %s = %s", self.name, key, value)
            return None
        if point.get("state_class") != "total_increasing":
            return value
        last = self._last_good.get(key)
        if last is None:
            self._last_good[key] = value
            return value
        jump = value - last
        if 0 <= jump <= max(abs(last) * TOTAL_MAX_JUMP, TOTAL_MIN_JUMP):
            self._suspect.pop(key, None)
            self._last_good[key] = value
            return value
        prev, count = self._suspect.get(key, (None, 0))
        count = count + 1 if prev is not None and abs(value - prev) <= max(abs(prev) * 0.001, 1) else 1
        if count >= CONFIRM_READS:
            log.warning("%s: chấp nhận giá trị mới %s = %s (lặp lại %s lần, cũ %s)", self.name, key, value, count, last)
            self._suspect.pop(key, None)
            self._last_good[key] = value
            return value
        self._suspect[key] = (value, count)
        log.warning("%s: nghi rác %s = %s (cũ %s, lần %s/%s) - giữ giá trị cũ", self.name, key, value, last, count, CONFIRM_READS)
        return last

    def publish_values(self, values: dict) -> None:
        for p in self.points:
            key = p["key"]
            value = values.get(key)
            if value is None:
                self.mq.publish(f"{self.topic}/{key}/avail", "offline", qos=1, retain=True)
                continue
            self.mq.publish(f"{self.topic}/{key}", str(value), qos=1, retain=True)
            self.mq.publish(f"{self.topic}/{key}/avail", "online", qos=1, retain=True)

    # ---------- vòng lặp ----------
    def start(self) -> None:
        if not self.enabled:
            log.info("%s: TẮT (enabled=false) - chỉ khai báo entity, không đọc thiết bị", self.name)
            return
        if self._thread and self._thread.is_alive():
            return
        self._halt.clear()
        self._thread = threading.Thread(target=self._run, name=self.name, daemon=True)
        self._thread.start()

    def set_enabled(self, on: bool) -> None:
        """Công tắc Polling trong HA: bật -> đọc ngay; tắt -> dừng đọc, entity unavailable."""
        if on == self.enabled:
            self.mq.publish(self.polling_state, "ON" if on else "OFF", qos=1, retain=True)
            return
        self.enabled = on
        self.cfg["enabled"] = on
        if self.on_change:
            self.on_change()
        if on:
            log.info("%s: BẬT đọc thiết bị (công tắc Polling)", self.name)
            self.start()
        else:
            log.info("%s: TẮT đọc thiết bị (công tắc Polling)", self.name)
            self._halt.set()
            if self._thread:
                self._thread.join(timeout=max(10, self.interval))
            for p in self.points:
                self.mq.publish(f"{self.topic}/{p['key']}/avail", "offline", qos=1, retain=True)
        self.mq.publish(self.polling_state, "ON" if on else "OFF", qos=1, retain=True)

    def _stopped(self) -> bool:
        return _stop.is_set() or self._halt.is_set()

    def _run(self) -> None:
        log.info("%s: bắt đầu đọc %s %s:%s, %s điểm, chu kỳ %ss", self.name, self.protocol,
                 self.cfg.get("host"), self.cfg.get("port"), len(self.points), self.interval)
        while not self._stopped():
            started = time.monotonic()
            try:
                values = self.read_once()
                if self._stopped():  # vừa tắt giữa vòng đọc -> không phát giá trị nữa
                    break
                self.publish_values(values)
            except Exception as err:  # noqa: BLE001 - 1 vòng lỗi không làm chết luồng
                log.error("%s: lỗi vòng đọc: %s", self.name, err)
                self.publish_values({})
            wait = max(1.0, self.interval - (time.monotonic() - started))
            deadline = time.monotonic() + wait
            while not self._stopped() and time.monotonic() < deadline:
                _stop.wait(min(1.0, deadline - time.monotonic()))


def main() -> None:
    opts = load_options()
    logging.basicConfig(level=getattr(logging, str(opts.get("log_level", "info")).upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    base = f"svtech/{slug(opts.get('site', 'hq'))}/{slug(opts.get('room', 'server-room'))}"
    status_topic = f"{base}/collector/status"

    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            config = json.load(f)
    except (OSError, ValueError) as err:
        log.error("Không đọc được %s: %s - dừng", CONFIG_FILE, err)
        time.sleep(60)
        return

    broker = mqtt_service()
    mq = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"project-collector-{slug(opts.get('site', 'hq'))}")
    if broker.get("username"):
        mq.username_pw_set(broker["username"], broker.get("password"))
    mq.will_set(status_topic, "offline", qos=1, retain=True)

    devices = [Device(d, base, mq) for d in config.get("devices", [])]

    # ---- Quét thiết bị: nút + cảm biến kết quả trong HA (2026-10-02) ----
    scan_cmd = f"{base}/collector/scan/set"
    scan_state = f"{base}/collector/scan/state"
    scan_attr = f"{base}/collector/scan/attr"
    site_id = slug(opts.get("site", "hq"))
    collector_dev = {"identifiers": [f"project_collector_{site_id}"], "name": f"Project Collector {site_id.upper()}",
                     "manufacturer": "Project", "model": "Project Collector", "sw_version": VERSION}
    scan_lock = threading.Lock()

    # ---- Tuỳ chọn quét chỉnh ngay trong HA, mỗi giao thức riêng (2026-10-02) ----
    # Lưu ở SETTINGS_FILE; lần đầu lấy mặc định từ tuỳ chọn add-on. (key, loại, tên, thêm)
    SETTINGS_FILE = "/share/project_collector/scan_settings.json"
    SETTING_DEFS = [
        ("scan_ranges", "text", "Scan: IP ranges", {"icon": "mdi:ip-network", "max": 255}),
        ("scan_timeout", "number", "Scan: timeout", {"icon": "mdi:timer-outline", "min": 0.5, "max": 5,
                                                      "step": 0.5, "mode": "box", "unit_of_measurement": "s"}),
        ("scan_snmp", "switch", "Scan: SNMP", {"icon": "mdi:lan"}),
        ("scan_snmp_communities", "text", "Scan: SNMP communities", {"icon": "mdi:key-variant",
                                                                      "mode": "password", "max": 255}),
        ("scan_snmp_version", "select", "Scan: SNMP version", {"icon": "mdi:numeric",
                                                                "options": ["auto", "v2c", "v1"]}),
        ("scan_snmp_known_communities", "switch", "Scan: SNMP also try known communities", {"icon": "mdi:key-chain"}),
        ("scan_modbus", "switch", "Scan: Modbus TCP", {"icon": "mdi:ethernet"}),
        ("scan_modbus_ports", "text", "Scan: Modbus TCP ports", {"icon": "mdi:numeric", "max": 100}),
        ("scan_include_known", "switch", "Scan: Modbus include configured devices", {"icon": "mdi:alert-outline"}),
        ("scan_bacnet", "switch", "Scan: BACnet/IP", {"icon": "mdi:hvac"}),
    ]
    defaults = {"scan_ranges": opts.get("scan_ranges", "192.168.25.0/24"), "scan_timeout": 2.0,
                "scan_snmp": True, "scan_snmp_communities": opts.get("scan_snmp_communities", "public"),
                "scan_snmp_version": "auto", "scan_snmp_known_communities": True, "scan_modbus": True,
                "scan_modbus_ports": opts.get("scan_modbus_ports", "502,10502"),
                "scan_include_known": bool(opts.get("scan_include_known", False)),
                "scan_bacnet": bool(opts.get("scan_bacnet", False))}
    try:
        with open(SETTINGS_FILE, encoding="utf-8") as f:
            settings = {**defaults, **json.load(f)}
    except (OSError, ValueError):
        settings = dict(defaults)

    def _setting_topic(key, kind):
        return f"{base}/collector/{key}/{kind}"

    def _publish_setting(key):
        value = settings.get(key)
        kind = next(k for s, k, _n, _x in SETTING_DEFS if s == key)
        payload = ("ON" if value else "OFF") if kind == "switch" else str(value)
        if key == "scan_snmp_communities":
            payload = "********"  # không đưa community thật vào trạng thái entity của HA
        mq.publish(_setting_topic(key, "state"), payload, qos=1, retain=True)

    def _apply_setting(key, raw: str):
        kind = next(k for s, k, _n, _x in SETTING_DEFS if s == key)
        if kind == "switch":
            value = raw.strip().upper() == "ON"
        elif kind == "number":
            value = float(raw)
        elif kind == "select":
            value = raw if raw in ("auto", "v2c", "v1") else "auto"
        else:
            value = raw.strip()
            if key == "scan_snmp_communities" and (not value or set(value) == {"*"}):
                _publish_setting(key)  # giữ giá trị cũ khi ô vẫn là dấu sao / để trống
                return
            if key == "scan_ranges":
                from scanner import _hosts
                _hosts(value)  # kiểm tra hợp lệ (sai -> lỗi, không lưu)
        settings[key] = value
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(settings, f, ensure_ascii=False, indent=1)
        except OSError as err:
            log.error("Không ghi được %s: %s", SETTINGS_FILE, err)
        _publish_setting(key)
        log.info("Tuỳ chọn quét %s = %s", key, "***" if "communit" in key else value)

    def publish_collector_entities():
        mq.publish(f"{DISCOVERY_PREFIX}/button/pc_{site_id}_scan/config", json.dumps({
            "name": "Scan devices", "unique_id": f"pc_{site_id}_scan", "command_topic": scan_cmd,
            "payload_press": "{}", "icon": "mdi:radar", "availability_topic": status_topic,
            "device": collector_dev}, ensure_ascii=False), qos=1, retain=True)
        mq.publish(f"{DISCOVERY_PREFIX}/sensor/pc_{site_id}_scan_result/config", json.dumps({
            "name": "Scan result", "unique_id": f"pc_{site_id}_scan_result", "state_topic": scan_state,
            "json_attributes_topic": scan_attr, "icon": "mdi:lan-check", "availability_topic": status_topic,
            "device": collector_dev}, ensure_ascii=False), qos=1, retain=True)
        for key, kind, name, extra in SETTING_DEFS:
            cfg = {"name": name, "unique_id": f"pc_{site_id}_{key}", "command_topic": _setting_topic(key, "set"),
                   "state_topic": _setting_topic(key, "state"), "availability_topic": status_topic,
                   "entity_category": "config", "device": collector_dev, **extra}
            if kind == "switch":
                cfg.update(payload_on="ON", payload_off="OFF")
            mq.publish(f"{DISCOVERY_PREFIX}/{kind}/pc_{site_id}_{key}/config",
                       json.dumps(cfg, ensure_ascii=False), qos=1, retain=True)
            _publish_setting(key)

    def do_scan(overrides: dict):
        if not scan_lock.acquire(blocking=False):
            log.warning("Đang quét - bỏ qua yêu cầu mới")
            return
        try:
            mq.publish(scan_state, "Scanning...", qos=1, retain=True)
            scan_opts = {**settings, **{k: v for k, v in overrides.items() if k.startswith("scan_")}}
            result = run_scan(scan_opts, config.get("devices", []))
            secrets = result.pop("_secrets", {})
            try:
                with open(SCAN_FILE, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=1)
            except OSError as err:
                log.error("Không ghi được %s: %s", SCAN_FILE, err)
            mq.publish(scan_attr, json.dumps(result, ensure_ascii=False), qos=1, retain=True)
            mq.publish(scan_state, f"{result['started'][5:16]} · {result['summary']}"[:250], qos=1, retain=True)
            _set_candidates(result, secrets)
        except Exception as err:  # noqa: BLE001
            log.error("Lỗi quét: %s", err)
            mq.publish(scan_state, f"Error: {err}"[:250], qos=1, retain=True)
        finally:
            scan_lock.release()

    # ---- Thêm thiết bị từ kết quả quét (2026-10-02) ----
    # Chọn thiết bị tìm thấy -> tên + "chép điểm đo từ" (điền sẵn theo gợi ý) -> Add device:
    # thêm vào devices.json, khai báo entity, bắt đầu đọc NGAY (không cần restart).
    GENERIC_SNMP = "Generic SNMP (system info)"
    EMPTY = "(empty - add points later)"
    NONE_FOUND = "(no new device - run a scan)"
    GENERIC_SNMP_POINTS = [
        {"key": "sys_name", "label": "System Name", "oid": "1.3.6.1.2.1.1.5.0"},
        {"key": "uptime", "label": "Uptime", "oid": "1.3.6.1.2.1.1.3.0", "format": "ticks_min",
         "unit": "min", "device_class": "duration", "state_class": "measurement"},
    ]
    add = {"candidates": [], "candidate": NONE_FOUND, "name": "", "template": GENERIC_SNMP, "unit_id": 1,
           "status": "Ready"}
    config_lock = threading.RLock()

    def save_config():
        """Ghi devices.json (atomic) - gọi khi thêm thiết bị / bật-tắt Polling."""
        with config_lock:
            tmp = CONFIG_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=1)
            os.replace(tmp, CONFIG_FILE)

    for d in devices:
        d.on_change = save_config

    def _add_topic(key, kind):
        return f"{base}/collector/add/{key}/{kind}"

    def _template_options():
        return [d["name"] for d in config.get("devices", [])] + [GENERIC_SNMP, EMPTY]

    def _candidate_options():
        return [c["label"] for c in add["candidates"]] or [NONE_FOUND]

    def publish_add_entities():
        common = {"availability_topic": status_topic, "device": collector_dev}
        defs = [
            ("select", "candidate", "Add: device found", {"icon": "mdi:lan-pending", "options": _candidate_options()}),
            ("text", "name", "Add: device name", {"icon": "mdi:rename", "max": 60}),
            ("select", "template", "Add: copy points from", {"icon": "mdi:content-copy",
                                                              "options": _template_options()}),
            ("number", "unit_id", "Add: Modbus unit ID", {"icon": "mdi:numeric", "min": 1, "max": 247,
                                                          "step": 1, "mode": "box"}),
        ]
        for kind, key, name, extra in defs:
            mq.publish(f"{DISCOVERY_PREFIX}/{kind}/pc_{site_id}_add_{key}/config", json.dumps({
                "name": name, "unique_id": f"pc_{site_id}_add_{key}", "command_topic": _add_topic(key, "set"),
                "state_topic": _add_topic(key, "state"), **common, **extra}, ensure_ascii=False), qos=1, retain=True)
        mq.publish(f"{DISCOVERY_PREFIX}/button/pc_{site_id}_add_device/config", json.dumps({
            "name": "Add device", "unique_id": f"pc_{site_id}_add_device", "command_topic": _add_topic("press", "set"),
            "payload_press": "add", "icon": "mdi:plus-network", **common}, ensure_ascii=False), qos=1, retain=True)
        mq.publish(f"{DISCOVERY_PREFIX}/sensor/pc_{site_id}_add_status/config", json.dumps({
            "name": "Add status", "unique_id": f"pc_{site_id}_add_status", "state_topic": _add_topic("status", "state"),
            "icon": "mdi:information-outline", **common}, ensure_ascii=False), qos=1, retain=True)
        _publish_add_states()

    def _publish_add_states():
        for key in ("candidate", "name", "template", "unit_id", "status"):
            mq.publish(_add_topic(key, "state"), str(add[key])[:250], qos=1, retain=True)

    def _status(text):
        add["status"] = text
        log.info("Thêm thiết bị: %s", text)
        mq.publish(_add_topic("status", "state"), text[:250], qos=1, retain=True)

    def _select_candidate(label):
        cand = next((c for c in add["candidates"] if c["label"] == label), None)
        add["candidate"] = label if cand else NONE_FOUND
        if cand:  # điền sẵn tên + mẫu theo gợi ý
            add["name"] = cand.get("sys_name") if cand.get("sys_name") not in (None, "", "(none)") \
                else f"Device {cand['ip']}"
            if cand.get("similar_to"):
                add["template"] = cand["similar_to"]
            elif cand["protocol"] == "snmp":
                add["template"] = GENERIC_SNMP
            else:
                add["template"] = EMPTY
        _publish_add_states()

    def _set_candidates(result, secrets):
        cands = []
        for d in result.get("snmp", []):
            if d.get("known"):
                continue
            label = f"{d['ip']} · SNMP v{d['snmp_version']} · {d.get('vendor') or d.get('sys_name') or '?'}"
            if d.get("similar_to"):
                label += f" (like {d['similar_to']})"
            cands.append({"label": label[:250], "protocol": "snmp", "ip": d["ip"], "port": 161,
                          "version": d["snmp_version"], "community": secrets.get(d["ip"]),
                          "sys_name": d.get("sys_name"), "similar_to": d.get("similar_to")})
        for d in result.get("modbus", []):
            if d.get("known"):
                continue
            for port in d.get("ports_open", []):
                cands.append({"label": f"{d['ip']}:{port} · Modbus TCP", "protocol": "modbus",
                              "ip": d["ip"], "port": port})
        add["candidates"] = cands
        publish_add_entities()  # cập nhật danh sách lựa chọn
        _select_candidate(cands[0]["label"] if cands else NONE_FOUND)
        _status(f"{len(cands)} new device(s) found - choose one, check name / template, press Add device"
                if cands else "No new device found")

    def _add_device():
        cand = next((c for c in add["candidates"] if c["label"] == add["candidate"]), None)
        if not cand:
            return _status("Nothing to add - run a scan and choose a device first")
        name = str(add["name"]).strip()
        if not name:
            return _status("Enter a device name first")
        with config_lock:
            if any(d["name"].lower() == name.lower() for d in config.get("devices", [])):
                return _status(f"Name '{name}' already exists - choose another name")
            tpl = add["template"]
            if tpl == GENERIC_SNMP:
                if cand["protocol"] != "snmp":
                    return _status("Generic SNMP template only fits SNMP devices")
                points, model = copy.deepcopy(GENERIC_SNMP_POINTS), "Generic SNMP"
            elif tpl == EMPTY:
                points, model = [], "Project Device"
            else:
                src = next((d for d in config.get("devices", []) if d["name"] == tpl), None)
                if not src:
                    return _status(f"Template device '{tpl}' not found")
                if src.get("protocol") != cand["protocol"]:
                    return _status(f"'{tpl}' is {src.get('protocol')}, the found device is {cand['protocol']}")
                points = copy.deepcopy(src.get("points", []))
                for p in points:  # thiết bị mới -> entity_id mới, mọi điểm bật
                    p.pop("entity_id", None)
                    p.pop("enabled", None)
                model = src.get("model") or "Project Device"
            dev_cfg = {
                "id": f"{slug(name).replace('-', '_')}_{int(time.time()) % 1000000}",
                "name": name, "model": model, "protocol": cand["protocol"], "host": cand["ip"],
                "port": cand["port"], "unit_id": int(float(add["unit_id"])) if cand["protocol"] == "modbus" else None,
                "community": cand.get("community") if cand["protocol"] == "snmp" else None,
                "snmp_version": {"2c": "2c", "1": "1"}.get(cand.get("version")) if cand["protocol"] == "snmp" else None,
                "device_instance": None, "scan_interval": 30, "enabled": True, "points": points,
                "added_from_scan": time.strftime("%Y-%m-%d %H:%M"),
            }
            config.setdefault("devices", []).append(dev_cfg)
            try:
                save_config()
            except OSError as err:
                config["devices"].remove(dev_cfg)
                return _status(f"Cannot save devices.json: {err}")
            dev = Device(dev_cfg, base, mq)
            dev.on_change = save_config
            devices.append(dev)
            dev.publish_discovery(status_topic)
            mq.subscribe(dev.polling_cmd, qos=1)
            dev.start()
            add["candidates"] = [c for c in add["candidates"] if c is not cand]
        publish_add_entities()
        _select_candidate(add["candidates"][0]["label"] if add["candidates"] else NONE_FOUND)
        _status(f"Added '{name}' ({cand['ip']}, {len(points)} points) at {time.strftime('%H:%M')}"
                + (" - add points to devices.json" if not points else ""))

    def _apply_add(key, raw: str):
        if key == "candidate":
            _select_candidate(raw)
        elif key == "name":
            add["name"] = raw.strip()[:60]
        elif key == "template":
            add["template"] = raw if raw in _template_options() else add["template"]
        elif key == "unit_id":
            add["unit_id"] = int(min(247, max(1, float(raw))))
        elif key == "press":
            threading.Thread(target=_add_device, name="add-device", daemon=True).start()
            return
        _publish_add_states()

    add_cmds = {_add_topic(k, "set"): k for k in ("candidate", "name", "template", "unit_id", "press")}

    connected_at = {"t": 0.0}

    setting_cmds = {_setting_topic(k, "set"): k for k, *_ in SETTING_DEFS}

    def on_message(_client, _userdata, msg):
        dev = next((d for d in devices if d.polling_cmd == msg.topic), None)
        if dev:  # công tắc Polling - chạy luồng riêng (tắt phải chờ vòng đọc dừng, không chặn MQTT)
            on = msg.payload.decode().strip().upper() == "ON"
            threading.Thread(target=dev.set_enabled, args=(on,), name=f"polling-{dev.name}", daemon=True).start()
            return
        if msg.topic in add_cmds:
            if add_cmds[msg.topic] == "press" and time.monotonic() - connected_at["t"] < 15:
                log.info("Bỏ qua lệnh Add device nhận ngay sau khi kết nối")
                return
            try:
                _apply_add(add_cmds[msg.topic], msg.payload.decode())
            except Exception as err:  # noqa: BLE001
                _status(f"Error: {err}")
            return
        if msg.topic in setting_cmds:
            try:
                _apply_setting(setting_cmds[msg.topic], msg.payload.decode())
            except Exception as err:  # noqa: BLE001 - giá trị sai -> giữ giá trị cũ
                log.warning("Tuỳ chọn %s không hợp lệ: %s", setting_cmds[msg.topic], err)
                _publish_setting(setting_cmds[msg.topic])
            return
        if msg.topic != scan_cmd:
            return
        # Bỏ lệnh quét tới trong 15 s đầu sau khi kết nối (lệnh cũ bị gửi lại lúc add-on khởi
        # động lại -> quét ngoài ý muốn, đã gặp 02/10).
        if time.monotonic() - connected_at["t"] < 15:
            log.info("Bỏ qua lệnh quét nhận ngay sau khi kết nối (có thể là lệnh cũ gửi lại)")
            return
        try:
            overrides = json.loads(msg.payload.decode() or "{}")
        except ValueError:
            overrides = {}
        threading.Thread(target=do_scan, args=(overrides if isinstance(overrides, dict) else {},),
                         name="scan", daemon=True).start()

    def on_connect(client, _userdata, _flags, reason_code, _props):
        log.info("Đã kết nối MQTT %s:%s (%s)", broker.get("host"), broker.get("port"), reason_code)
        for d in devices:
            d.publish_discovery(status_topic)
        publish_collector_entities()
        publish_add_entities()
        connected_at["t"] = time.monotonic()
        for topic in add_cmds:
            client.subscribe(topic, qos=1)
        for d in devices:
            client.subscribe(d.polling_cmd, qos=1)
        client.subscribe(scan_cmd, qos=1)
        for topic in setting_cmds:
            client.subscribe(topic, qos=1)
        client.publish(status_topic, "online", qos=1, retain=True)

    mq.on_connect = on_connect
    mq.on_message = on_message
    mq.reconnect_delay_set(1, 30)
    mq.connect(broker["host"], int(broker.get("port", 1883)), keepalive=60)
    mq.loop_start()

    for d in devices:
        d.start()
    log.info("Đang chạy: %s thiết bị (%s đang đọc), topic gốc %s", len(devices),
             sum(1 for d in devices if d.enabled), base)

    def _shutdown(*_):
        _stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    while not _stop.is_set():
        _stop.wait(5)
    mq.publish(status_topic, "offline", qos=1, retain=True)
    time.sleep(1)
    mq.loop_stop()
    mq.disconnect()


if __name__ == "__main__":
    main()
