"""Sinh /share/project_collector/devices.json từ integration Project trên HA (chạy trong
add-on SSH: python3 export_from_ha.py [--only "EMS2000,UPS A"] [--disable-modbus]).

Đọc /config/.storage/core.config_entries (cấu hình thiết bị) + core.entity_registry
(entity_id hiện có của từng điểm, theo unique_id "<entry_id>_<key>") -> mỗi điểm mang
"entity_id" để collector tạo entity MQTT GIỮ NGUYÊN entity_id (lịch sử nối tiếp).
Chỉ đọc .storage, không sửa gì.
"""
import argparse
import json
import os

STORAGE = "/config/.storage"
OUT = "/share/project_collector/devices.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="chỉ lấy các thiết bị (tên, cách nhau dấu phẩy)")
    ap.add_argument("--disable-modbus", action="store_true", help="thiết bị Modbus enabled=false")
    ap.add_argument("--merge", action="store_true", help="giữ thiết bị đã có trong file, chỉ thêm/ghi đè")
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args()

    entries = json.load(open(f"{STORAGE}/core.config_entries", encoding="utf-8"))["data"]["entries"]
    registry = json.load(open(f"{STORAGE}/core.entity_registry", encoding="utf-8"))["data"]["entities"]
    by_uid = {e["unique_id"]: e for e in registry if e.get("platform") == "project"}
    only = [s.strip() for s in a.only.split(",") if s.strip()]

    devices = []
    for e in entries:
        if e["domain"] != "project" or (only and e["title"] not in only):
            continue
        d = e["data"]
        points = []
        for p in d.get("points", []):
            p = dict(p)
            reg = by_uid.get(f"{e['entry_id']}_{p['key']}")
            if reg:
                p["entity_id"] = reg["entity_id"]
                if reg.get("disabled_by") == "user":
                    p["enabled"] = False
            points.append(p)
        devices.append({
            "id": e["entry_id"],
            "name": e["title"],
            "model": d.get("template_name") or "Project Device",
            "protocol": d["protocol"],
            "host": d["host"],
            "port": d["port"],
            "unit_id": d.get("unit_id"),
            "community": d.get("community"),
            "snmp_version": d.get("snmp_version"),
            "device_instance": d.get("device_instance"),
            "scan_interval": (e.get("options") or {}).get("scan_interval", d.get("scan_interval", 30)),
            "enabled": not (a.disable_modbus and d["protocol"] == "modbus"),
            "points": points,
        })

    if a.merge and os.path.exists(a.out):
        old = json.load(open(a.out, encoding="utf-8")).get("devices", [])
        ids = {d["id"] for d in devices}
        devices = [d for d in old if d["id"] not in ids] + devices

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    tmp = a.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"devices": devices}, f, ensure_ascii=False, indent=1)
    os.replace(tmp, a.out)
    for d in devices:
        missing = sum(1 for p in d["points"] if not p.get("entity_id"))
        print(f"{d['name']:30} {d['protocol']:6} {d['host']}:{d['port']} enabled={d['enabled']} "
              f"points={len(d['points'])} thieu_entity_id={missing}")
    print("->", a.out)


if __name__ == "__main__":
    main()
