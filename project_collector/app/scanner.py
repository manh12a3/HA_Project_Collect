"""Quét thiết bị cho Project Collector (2026-10-02).

Chạy khi bấm nút "Scan devices" trong HA (MQTT `<base>/collector/scan/set`). KHÔNG tự thêm thiết
bị - chỉ trả danh sách để người duyệt rồi thêm vào devices.json.

- SNMP: quét dải IP, GET sysDescr / sysObjectID / sysName (v2c rồi v1, các community cấu hình).
  Thiết bị mới có cùng sysObjectID với thiết bị ĐÃ CÓ -> gợi ý "giống <tên>" (dùng lại mẫu).
- Modbus TCP: CHỈ thử mở kết nối TCP tới cổng (502, 10502...) rồi đóng - không gửi lệnh Modbus.
  Mặc định BỎ QUA IP đã có trong cấu hình (gateway RS485 đang đọc thật) -> không chen ngang.
- BACnet/IP: Who-Is unicast tới từng IP (chưa kiểm chứng - phòng máy chưa có thiết bị BACnet).
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import time

log = logging.getLogger("project_collector.scan")

OID_DESCR = "1.3.6.1.2.1.1.1.0"
OID_OBJECT_ID = "1.3.6.1.2.1.1.2.0"
OID_NAME = "1.3.6.1.2.1.1.5.0"
MAX_HOSTS = 1024  # chặn quét nhầm dải quá lớn

# Mã doanh nghiệp (IANA enterprise) phổ biến trong phòng máy - chỉ để hiển thị cho dễ đọc
ENTERPRISES = {
    "318": "APC / Schneider Electric", "534": "Eaton", "476": "Vertiv (Liebert / Emerson)",
    "3902": "ZTE", "2254": "Delta Electronics (InsightPower)", "2011": "Huawei", "9": "Cisco", "11": "HP", "232": "HP / Compaq", "674": "Dell",
    "8072": "Net-SNMP (Linux)", "311": "Microsoft", "25506": "H3C", "14988": "MikroTik",
    "41112": "Ubiquiti", "6574": "Synology", "24681": "QNAP", "2636": "Juniper", "12356": "Fortinet",
}


def _hosts(ranges: str) -> list[str]:
    out: list[str] = []
    for part in [p.strip() for p in str(ranges or "").replace(";", ",").split(",") if p.strip()]:
        if "-" in part and "/" not in part:  # 192.168.25.1-192.168.25.50
            a, b = [ipaddress.ip_address(x.strip()) for x in part.split("-", 1)]
            out += [str(ipaddress.ip_address(i)) for i in range(int(a), int(b) + 1)]
        else:
            net = ipaddress.ip_network(part, strict=False)
            out += [str(h) for h in (net.hosts() if net.num_addresses > 1 else [net.network_address])]
        if len(out) > MAX_HOSTS:
            raise ValueError(f"Dải quét quá lớn (> {MAX_HOSTS} địa chỉ)")
    return list(dict.fromkeys(out))


def _enterprise(object_id: str) -> str:
    parts = object_id.split(".")
    if object_id.startswith("1.3.6.1.4.1.") and len(parts) > 6:
        num = parts[6]
        return ENTERPRISES.get(num, f"enterprise {num}")
    return ""


SNMP_VERSIONS = {"auto": (("2c", 1), ("1", 0)), "v2c": (("2c", 1),), "v1": (("1", 0),)}


async def _snmp_scan(hosts, communities, versions=SNMP_VERSIONS["auto"], concurrency=32, timeout=2.0):
    from pysnmp.hlapi.v3arch.asyncio import (
        CommunityData, ContextData, ObjectIdentity, ObjectType, SnmpEngine, UdpTransportTarget, get_cmd,
    )

    engine = SnmpEngine()
    sem = asyncio.Semaphore(concurrency)
    found = []

    async def probe(ip):
        async with sem:
            for idx, community in enumerate(communities, start=1):
                for version, mp in versions:
                    try:
                        target = await UdpTransportTarget.create((ip, 161), timeout=timeout, retries=0)
                        err_ind, err_status, _i, binds = await get_cmd(
                            engine, CommunityData(community, mpModel=mp), target, ContextData(),
                            ObjectType(ObjectIdentity(OID_DESCR)), ObjectType(ObjectIdentity(OID_OBJECT_ID)),
                            ObjectType(ObjectIdentity(OID_NAME)))
                    except Exception:  # noqa: BLE001
                        continue
                    if err_ind or err_status:
                        continue
                    vals = [b[1].prettyPrint() for b in binds]
                    # sysObjectID có thể ra dạng "SNMPv2-SMI::enterprises.318.1.3.27" -> đổi về số
                    vals[1] = vals[1].replace("SNMPv2-SMI::enterprises.", "1.3.6.1.4.1.")
                    found.append({"ip": ip, "snmp_version": version, "community_no": idx,
                                  "sys_descr": vals[0][:160], "sys_object_id": vals[1], "sys_name": vals[2][:60],
                                  "vendor": _enterprise(vals[1]), "_community": community})
                    return

    await asyncio.gather(*(probe(ip) for ip in hosts))
    closer = getattr(engine, "close_dispatcher", None)
    if closer:
        try:
            closer()
        except Exception:  # noqa: BLE001
            pass
    return sorted(found, key=lambda d: ipaddress.ip_address(d["ip"]))


async def _tcp_scan(hosts, ports, concurrency=64, timeout=1.0):
    sem = asyncio.Semaphore(concurrency)
    found: dict = {}

    async def probe(ip, port):
        async with sem:
            try:
                _r, w = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout)
                w.close()
                try:
                    await w.wait_closed()
                except Exception:  # noqa: BLE001
                    pass
                found.setdefault(ip, []).append(port)
            except Exception:  # noqa: BLE001
                return

    await asyncio.gather(*(probe(ip, p) for ip in hosts for p in ports))
    return [{"ip": ip, "ports_open": sorted(p)} for ip, p in sorted(found.items(), key=lambda kv: ipaddress.ip_address(kv[0]))]


async def _bacnet_scan(hosts, timeout=2.0):
    from bacpypes3.app import Application
    from bacpypes3.local.device import DeviceObject
    from bacpypes3.pdu import Address

    app = Application.from_object_list([DeviceObject(
        objectIdentifier=("device", 4194301), objectName="project-collector-scan", vendorIdentifier=999)])
    found = []
    try:
        sem = asyncio.Semaphore(16)

        async def probe(ip):
            async with sem:
                try:
                    i_ams = await asyncio.wait_for(app.who_is(address=Address(ip)), timeout)
                except Exception:  # noqa: BLE001
                    return
                for i_am in i_ams or []:
                    found.append({"ip": ip, "device_instance": i_am.iAmDeviceIdentifier[1],
                                  "vendor_id": getattr(i_am, "vendorID", None)})

        await asyncio.gather(*(probe(ip) for ip in hosts))
    finally:
        app.close()
    return found


def run_scan(options: dict, devices: list[dict]) -> dict:
    """Quét theo options - mỗi giao thức bật/tắt + tham số riêng (2026-10-02, chỉnh trong HA):
      chung:  scan_ranges, scan_timeout (giây)
      SNMP:   scan_snmp (bool), scan_snmp_communities, scan_snmp_version (auto|v2c|v1),
              scan_snmp_known_communities (bool - thử thêm community của thiết bị đã có)
      Modbus: scan_modbus (bool), scan_modbus_ports, scan_include_known (bool)
      BACnet: scan_bacnet (bool)
    `devices` = cấu hình hiện có, để đánh dấu "đã có" / gợi ý mẫu."""
    started = time.time()
    hosts = _hosts(options.get("scan_ranges", ""))
    timeout = min(5.0, max(0.5, float(options.get("scan_timeout") or 2)))
    do_snmp = bool(options.get("scan_snmp", True))
    do_modbus = bool(options.get("scan_modbus", True))
    do_bacnet = bool(options.get("scan_bacnet", False))
    communities = [c.strip() for c in str(options.get("scan_snmp_communities") or "public").split(",") if c.strip()]
    # Thử thêm community của thiết bị SNMP đã cấu hình (thiết bị cùng loại thường dùng chung);
    # kết quả chỉ ghi SỐ THỨ TỰ community, không lộ giá trị.
    if options.get("scan_snmp_known_communities", True):
        for d in devices:
            c = d.get("community")
            if d.get("protocol") == "snmp" and c and c not in communities:
                communities.append(c)
    versions = SNMP_VERSIONS.get(str(options.get("scan_snmp_version") or "auto"), SNMP_VERSIONS["auto"])
    ports = [int(p) for p in str(options.get("scan_modbus_ports") or "502").replace(";", ",").split(",") if p.strip()]
    include_known = bool(options.get("scan_include_known", False))
    known_by_ip: dict = {}
    for d in devices:
        known_by_ip.setdefault(str(d.get("host")), []).append(d)
    result = {"started": time.strftime("%Y-%m-%d %H:%M:%S"), "hosts": len(hosts),
              "ranges": options.get("scan_ranges", ""), "protocols": [p for p, on in
              (("snmp", do_snmp), ("modbus", do_modbus), ("bacnet", do_bacnet)) if on],
              "snmp": [], "modbus": [], "bacnet": [], "errors": []}
    if not result["protocols"]:
        result.update(duration_s=0, summary="No protocol selected")
        return result
    log.info("Quét %s địa chỉ (%s) - %s; SNMP %s community %s; Modbus cổng %s; timeout %ss",
             len(hosts), result["ranges"], "+".join(result["protocols"]), len(communities),
             options.get("scan_snmp_version") or "auto", ports, timeout)

    if do_snmp:
        try:
            result["snmp"] = asyncio.run(_snmp_scan(hosts, communities, versions, timeout=timeout))
        except Exception as err:  # noqa: BLE001
            result["errors"].append(f"SNMP: {err}")
    # sysObjectID của thiết bị đã có -> gợi ý cho thiết bị mới cùng model
    oid_to_known: dict = {}
    for item in result["snmp"]:
        names = [d["name"] for d in known_by_ip.get(item["ip"], []) if d.get("protocol") == "snmp"]
        item["known"] = ", ".join(names) or None
        if names:
            oid_to_known.setdefault(item["sys_object_id"], names[0])
    descr_of_known = {i["sys_object_id"]: i["sys_descr"] for i in result["snmp"] if i["known"]}
    for item in result["snmp"]:
        oid = item["sys_object_id"]
        if item["known"] or oid not in oid_to_known:
            continue
        # sysObjectID CHUNG (Net-SNMP / Windows - vd Rectifier ZTE chạy Linux) -> chỉ gợi ý khi
        # sysDescr cũng giống, tránh gợi ý nhầm mọi máy Linux là "giống Rectifier".
        if oid.split(".")[6:7] in (["8072"], ["311"]) and \
                item["sys_descr"][:24] != descr_of_known.get(oid, "")[:24]:
            continue
        item["similar_to"] = oid_to_known[oid]

    if do_modbus:
        tcp_hosts = [h for h in hosts if include_known or h not in known_by_ip]
        try:
            result["modbus"] = asyncio.run(_tcp_scan(tcp_hosts, ports, timeout=timeout))
        except Exception as err:  # noqa: BLE001
            result["errors"].append(f"Modbus TCP: {err}")
        result["modbus_skipped_known"] = sorted(set(hosts) & set(known_by_ip) - set(tcp_hosts),
                                                key=lambda ip: ipaddress.ip_address(ip))
        for item in result["modbus"]:
            item["known"] = ", ".join(d["name"] for d in known_by_ip.get(item["ip"], [])) or None

    if do_bacnet:
        try:
            result["bacnet"] = asyncio.run(_bacnet_scan(hosts, timeout=max(timeout, 2)))
        except Exception as err:  # noqa: BLE001
            result["errors"].append(f"BACnet: {err}")

    parts = []
    if do_snmp:
        parts.append(f"SNMP {len(result['snmp'])} ({sum(1 for d in result['snmp'] if not d.get('known'))} new)")
    if do_modbus:
        parts.append(f"Modbus TCP {len(result['modbus'])} ({sum(1 for d in result['modbus'] if not d.get('known'))} new)")
    if do_bacnet:
        parts.append(f"BACnet {len(result['bacnet'])}")
    result["duration_s"] = round(time.time() - started, 1)
    result["summary"] = " · ".join(parts)
    # community dùng được của từng IP: chỉ để collector thêm thiết bị, KHÔNG công bố (collector bỏ "_secrets")
    result["_secrets"] = {d["ip"]: d.pop("_community") for d in result["snmp"] if "_community" in d}
    log.info("Quét xong %ss: %s", result["duration_s"], result["summary"])
    return result
