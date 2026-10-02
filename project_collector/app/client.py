"""Blocking/async protocol clients for the Project integration."""
from __future__ import annotations

import asyncio
import struct
import threading
import time
from typing import Any

_MODBUS_TYPE_INFO = {
    "int16": ("h", 1), "uint16": ("H", 1),
    "int32": ("i", 2), "uint32": ("I", 2), "float32": ("f", 2),
    "int64": ("q", 4), "uint64": ("Q", 4), "float64": ("d", 4),
}


def _decode_modbus(registers, data_type, word_order="abcd"):
    fmt, needed = _MODBUS_TYPE_INFO.get(data_type, (None, None))
    if fmt is None:
        return registers[0] if registers else None
    words = list(registers[:needed])
    if word_order == "cdab":
        words = list(reversed(words))
    raw = b"".join(struct.pack(">H", w) for w in words)
    return struct.unpack(">" + fmt, raw)[0]


_HOST_LOCKS: dict = {}
_HOST_LOCKS_GUARD = threading.Lock()
MODBUS_REQUEST_GAP = 0.05  # giây nghỉ giữa 2 lần hỏi - gateway rẻ tiền cần thời gian
MODBUS_TIMEOUT = 3  # giây chờ 1 phản hồi
MODBUS_RETRIES = 1  # số lần thử lại khi không có phản hồi (pymodbus mặc định 3)
MODBUS_ABORT_AFTER = 2  # số điểm LIÊN TIẾP không phản hồi -> bỏ phần còn lại của chu kỳ
MODBUS_GATEWAY_RETRY_CODES = (10, 11)  # gateway path unavailable / target failed to respond
MODBUS_GATEWAY_RETRY_DELAY = 0.3  # giây nghỉ trước khi hỏi lại


def _no_response(exc: Exception) -> bool:
    """Lỗi kiểu thiết bị / gateway không trả lời (timeout, mất kết nối) - khác với
    lỗi thanh ghi sai (thiết bị có trả lời nhưng báo lỗi)."""
    text = f"{type(exc).__name__} {exc}".lower()
    return any(s in text for s in ("no response", "timeout", "timed out", "connection", "broken pipe"))


def _tcp_client(host: str, port: int, timeout: float):
    from pymodbus.client import ModbusTcpClient

    try:
        return ModbusTcpClient(host, port=int(port), timeout=timeout, retries=MODBUS_RETRIES)
    except TypeError:  # pymodbus cũ không có tham số retries
        return ModbusTcpClient(host, port=int(port), timeout=timeout)


def _host_lock(host: str) -> threading.Lock:
    """1 khoá / IP gateway: các thiết bị dùng chung gateway đọc lần lượt."""
    with _HOST_LOCKS_GUARD:
        return _HOST_LOCKS.setdefault(host, threading.Lock())


def modbus_read_points(host: str, port: int, unit_id: int, points: list, timeout: float = MODBUS_TIMEOUT) -> dict:
    """Blocking: đọc MỌI điểm của 1 thiết bị qua 1 kết nối TCP, lần lượt, giữ
    khoá theo IP gateway. Trả {key: giá trị | Exception}. Must run via executor.

    Lỗi đã gặp 2026-09-27: mở/đóng 1 kết nối cho MỖI điểm + nhiều thiết bị hỏi
    cùng lúc 1 gateway (192.168.25.17) -> Connection refused, "unpack requires a
    buffer", trả nhầm gói tin -> giá trị rác (-7.7e34 kWh).
    Lỗi đã gặp 2026-09-28: gateway không trả lời lúc HA khởi động -> 16 điểm x 5 s
    x 3 lần thử + 2 thiết bị xếp hàng > 5 phút -> HA huỷ setup (setup_error, mất dữ
    liệu). Nay: timeout 3 s, thử lại 1 lần, MODBUS_ABORT_AFTER điểm liên tiếp không
    phản hồi thì bỏ phần còn lại (tối đa ~10 s / thiết bị khi gateway chết).
    """
    result: dict = {}
    with _host_lock(host):
        client = _tcp_client(host, port, timeout)
        try:
            if not client.connect():
                err = ConnectionError(f"Không kết nối được Modbus TCP tới {host}:{port}")
                return {p["key"]: err for p in points}
            silent = 0
            for index, point in enumerate(points):
                try:
                    result[point["key"]] = _modbus_read_with(client, unit_id, point)
                    silent = 0
                except Exception as exc:  # noqa: BLE001 - lỗi 1 điểm không chặn điểm khác
                    result[point["key"]] = exc
                    silent = silent + 1 if _no_response(exc) else 0
                    if silent >= MODBUS_ABORT_AFTER:
                        skip = TimeoutError(f"{host}:{port} không phản hồi - bỏ qua các điểm còn lại chu kỳ này")
                        for rest in points[index + 1:]:
                            result[rest["key"]] = skip
                        break
                time.sleep(MODBUS_REQUEST_GAP)
        finally:
            client.close()
    return result


def modbus_read_point(host: str, port: int, unit_id: int, point: dict, timeout: float = MODBUS_TIMEOUT) -> Any:
    """Blocking Modbus TCP read for a single point (dùng cho Test connection). Must run via executor."""
    with _host_lock(host):
        client = _tcp_client(host, port, timeout)
        try:
            if not client.connect():
                raise ConnectionError(f"Không kết nối được Modbus TCP tới {host}:{port}")
            return _modbus_read_with(client, unit_id, point)
        finally:
            client.close()


def _modbus_read_with(client, unit_id: int, point: dict) -> Any:
    """Đọc 1 điểm trên kết nối đã mở."""
    readers = {
        "holding": client.read_holding_registers,
        "input": client.read_input_registers,
        "coil": client.read_coils,
        "discrete": client.read_discrete_inputs,
    }
    fn = readers.get(point.get("input_type", "holding"), client.read_holding_registers)
    data_type = point.get("data_type", "uint16")
    _, needed = _MODBUS_TYPE_INFO.get(data_type, (None, 1))
    count = needed or 1
    def _read():
        try:
            return fn(address=int(point["address"]), count=count, device_id=int(unit_id))
        except TypeError:
            return fn(address=int(point["address"]), count=count, slave=int(unit_id))

    rr = _read()
    # Mã 10/11 = gateway báo đường RS485 / thiết bị không trả lời (trả về ngay, không
    # chờ timeout) -> hỏi lại 1 lần (2026-09-30, Power Meter unit 6 chập chờn).
    if rr.isError() and getattr(rr, "exception_code", None) in MODBUS_GATEWAY_RETRY_CODES:
        time.sleep(MODBUS_GATEWAY_RETRY_DELAY)
        rr = _read()
    if rr.isError():
        raise IOError(f"Modbus error tại address {point['address']}: {rr}")
    values = getattr(rr, "registers", None) or getattr(rr, "bits", None)
    if not values or len(values) < count:
        raise IOError(f"Phản hồi thiếu dữ liệu tại address {point['address']} ({values})")
    value = _decode_modbus(values, data_type, point.get("word_order", "abcd"))
    scale = point.get("scale", 1)
    if isinstance(value, (int, float)):
        value = round(value * scale, point.get("round", 2))
    return value


def snmp_get_point(host: str, port: int, community: str, version: str, oid: str,
                    timeout: float = 5) -> str:
    """Blocking SNMP GET for a single OID. Must run via executor.

    Chạy được với cả pysnmp 7.x (API mới `v3arch.asyncio`, `get_cmd`,
    `await UdpTransportTarget.create(...)` - bắt buộc cho integration khác như
    apc_modbus) lẫn pysnmp 6.x (API cũ `getCmd`).
    """
    mp_model = 1 if version == "2c" else 0

    try:
        from pysnmp.hlapi.v3arch.asyncio import (
            SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
            ObjectType, ObjectIdentity, get_cmd,
        )
        new_api = True
    except ImportError:
        from pysnmp.hlapi.asyncio import (  # pysnmp 6.x
            SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
            ObjectType, ObjectIdentity, getCmd as get_cmd,
        )
        new_api = False

    async def _run():
        if new_api:
            target = await UdpTransportTarget.create((host, int(port)), timeout=timeout, retries=1)
        else:
            target = UdpTransportTarget((host, int(port)), timeout=timeout, retries=1)
        return await get_cmd(
            SnmpEngine(),
            CommunityData(community, mpModel=mp_model),
            target,
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )

    error_indication, error_status, error_index, var_binds = asyncio.run(_run())
    if error_indication:
        raise IOError(f"SNMP error ({host} {oid}): {error_indication}")
    if error_status:
        raise IOError(f"SNMP error ({host} {oid}): {error_status.prettyPrint()}")
    return var_binds[0][1].prettyPrint()


SNMP_BATCH = 10  # số OID / 1 gói GET


class _SnmpStatusError(IOError):
    """Thiết bị trả lời nhưng báo lỗi (vd v1 noSuchName) - khác với không phản hồi."""


def snmp_get_points(host: str, port: int, community: str, version: str, oids: list,
                    timeout: float = 5) -> dict:
    """Blocking: GET nhiều OID qua 1 SnmpEngine, gom SNMP_BATCH OID / gói. Must run via executor.

    Trả {oid: chuỗi | Exception}. SNMP v1: 1 OID sai làm hỏng cả gói -> hỏi lại từng OID
    của gói đó. Gói bị timeout -> các gói còn lại đánh lỗi luôn (khỏi chờ thêm).
    Thêm 2026-09-28: UPS ~60 điểm, trước đây mỗi điểm 1 SnmpEngine + 1 lần hỏi.
    """
    return asyncio.run(_snmp_get_points_async(host, port, community, version, oids, timeout))


async def _snmp_get_points_async(host, port, community, version, oids, timeout):
    try:
        from pysnmp.hlapi.v3arch.asyncio import (
            SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
            ObjectType, ObjectIdentity, get_cmd,
        )
        target = await UdpTransportTarget.create((host, int(port)), timeout=timeout, retries=1)
    except ImportError:
        from pysnmp.hlapi.asyncio import (  # pysnmp 6.x
            SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
            ObjectType, ObjectIdentity, getCmd as get_cmd,
        )
        target = UdpTransportTarget((host, int(port)), timeout=timeout, retries=1)
    engine = SnmpEngine()
    auth = CommunityData(community, mpModel=1 if version == "2c" else 0)

    async def _get(batch):
        error_indication, error_status, _index, var_binds = await get_cmd(
            engine, auth, target, ContextData(), *[ObjectType(ObjectIdentity(o)) for o in batch],
        )
        if error_indication:
            raise IOError(f"SNMP error ({host}): {error_indication}")
        if error_status:
            raise _SnmpStatusError(f"SNMP error ({host}): {error_status.prettyPrint()}")
        values = []
        for oid, var_bind in zip(batch, var_binds):
            value = var_bind[1]
            if type(value).__name__ in ("NoSuchObject", "NoSuchInstance", "EndOfMibView"):
                values.append(_SnmpStatusError(f"SNMP ({host}): không có OID {oid}"))
            else:
                values.append(value.prettyPrint())
        return values

    out: dict = {}
    for start in range(0, len(oids), SNMP_BATCH):
        batch = oids[start:start + SNMP_BATCH]
        try:
            out.update(zip(batch, await _get(batch)))
        except _SnmpStatusError:
            for oid in batch:
                try:
                    out[oid] = (await _get([oid]))[0]
                except Exception as exc:  # noqa: BLE001
                    out[oid] = exc
        except Exception as exc:  # noqa: BLE001 - timeout / mất kết nối
            out.update({oid: exc for oid in oids[start:]})
            break
    closer = getattr(engine, "close_dispatcher", None)
    if closer:
        try:
            closer()
        except Exception:  # noqa: BLE001
            pass
    return out


async def bacnet_read_point(host: str, port: int, device_instance: int, object_type: str,
                             object_instance: int, property_name: str,
                             timeout: float = 5) -> Any:
    """Async BACnet/IP ReadProperty for a single point."""
    from bacpypes3.app import Application
    from bacpypes3.local.device import DeviceObject
    from bacpypes3.pdu import Address
    from bacpypes3.primitivedata import ObjectIdentifier

    device_object = DeviceObject(
        objectIdentifier=("device", int(device_instance)),
        objectName="project-integration",
        vendorIdentifier=999,
    )
    app = Application.from_object_list([device_object])
    try:
        oid = ObjectIdentifier(f"{object_type},{int(object_instance)}")
        value = await asyncio.wait_for(
            app.read_property(Address(host), oid, property_name), timeout=timeout
        )
        return value
    finally:
        app.close()


def format_snmp_value(point: dict, raw_value: Any) -> Any:
    """Apply optional format/scale rules (used mainly by the UPS SNMP template).

    Thêm 2026-09-28 (đồng bộ UPS SNMP với apc_modbus, xem ups_snmp_points.py):
    `map` {"mã": "chữ"}; `bit` n hoặc [n, ...] (tính từ 1) trong chuỗi bit APC -> `on`/`off`
    (mặc định Yes/No, có 1 bit bật là on); `valid_min` (nhỏ hơn -> None);
    format `mdy_date` ("11/18/2025" -> "2025-11-18"), `ticks_min` (TimeTicks -> phút).
    """
    raw_text = str(raw_value).strip()
    if "map" in point:
        return point["map"].get(raw_text, f"Unknown ({raw_text})")
    if "bit" in point:
        bits = point["bit"] if isinstance(point["bit"], list) else [point["bit"]]
        if not raw_text or any(c not in "01" for c in raw_text):
            return None
        on = any(0 < int(b) <= len(raw_text) and raw_text[int(b) - 1] == "1" for b in bits)
        return point.get("on", "Yes") if on else point.get("off", "No")
    fmt = point.get("format")
    if fmt == "mdy_date":
        parts = raw_text.split("/")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            year = int(parts[2]) + (2000 if len(parts[2]) == 2 else 0)
            return f"{year:04d}-{int(parts[0]):02d}-{int(parts[1]):02d}"
        return raw_text or None
    if fmt == "ticks_min":
        try:
            return round(int(raw_text.split()[0].replace(",", "")) / 6000, 1)
        except (TypeError, ValueError, IndexError):
            return None
    if fmt == "runtime":
        try:
            centiseconds = int(str(raw_value).split()[0].replace(",", ""))
        except (TypeError, ValueError):
            return str(raw_value)
        total_seconds = centiseconds // 100
        hours, rem = divmod(total_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s"
    if fmt == "output_status":
        # Mã APC upsBasicOutputStatus (2026-09-28 sửa: bản cũ theo UPS-MIB, "4" bị hiểu
        # nhầm là Bypass - APC 4 = Smart Boost). Giữ chữ cũ cho mã đúng (luật cảnh báo dùng).
        mapping = {"1": "Unknown", "2": "Normal (Online)", "3": "Battery",
                   "4": "Normal (Smart Boost)", "6": "Bypass", "7": "Off", "8": "Rebooting",
                   "9": "Bypass", "10": "Bypass (Hardware Failure)", "12": "Normal (Smart Trim)",
                   "13": "Normal (ECO)", "15": "Battery (Self Test)", "16": "Bypass (Emergency)"}
        return mapping.get(str(raw_value), f"Unknown ({raw_value})")
    if fmt == "alarms":
        try:
            count = int(raw_value)
        except (TypeError, ValueError):
            count = 0
        # Trạng thái thiết bị luôn tiếng Anh (2026-09-28). Trước: "Bình thường" / "Có n cảnh báo..."
        return "Normal" if count == 0 else f"{count} Active Alarm(s)"

    scale = point.get("scale", 1)
    try:
        if "valid_min" in point and float(raw_value) < point["valid_min"]:
            return None
        value = round(float(raw_value) * scale, point.get("round", 1))
        if value == int(value):
            value = int(value)
        return value
    except (TypeError, ValueError):
        return raw_value
