# Changelog

## 0.6.0 – 2026-10-02
- Giao diện quản lý thiết bị (Ingress, thanh bên HA → Project Collector, chỉ admin):
  thêm / sửa / xoá thiết bị, IP / cổng / unit ID / community / chu kỳ, bảng điểm đo
  (OID, thanh ghi, kiểu dữ liệu, hệ số, đơn vị, device class, bảng mã...), đọc thử, CSV, mẫu.
- 7 mẫu thiết bị dựng sẵn (APC UPS, ZTE rectifier, power meter Modbus, Delta EMS2000, sensor môi
  trường, DPM380, Generic SNMP) + lưu mẫu từ thiết bị.
- Sửa / xoá thiết bị áp dụng ngay, giữ entity_id của điểm cùng key; xoá điểm/thiết bị xoá entity.
- Tự tạo `devices.json` rỗng khi cài lần đầu; sao lưu 10 bản `devices.json` mỗi lần sửa.

## 0.5.0
- Công tắc **Polling** bật / tắt đọc từng thiết bị (mục Configuration của thiết bị trong HA).

## 0.4.0
- Thêm thiết bị từ kết quả quét (chọn thiết bị tìm thấy → mẫu → Add device).

## 0.3.0
- Tuỳ chọn quét theo từng giao thức chỉnh trong HA (dải IP, SNMP community / version, cổng Modbus...).

## 0.2.x
- Quét thiết bị SNMP / Modbus TCP / BACnet.

## 0.1.0
- Đọc thiết bị (client của integration Project) → MQTT discovery, giữ entity_id cũ.
