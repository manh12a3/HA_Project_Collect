# Project Collector - add-on cục bộ (2026-10-02). Ảnh Python thuần (không dùng
# base s6 của HA) -> init: false trong config.yaml.
FROM python:3.12-alpine

RUN pip install --no-cache-dir \
      "pymodbus>=3.6.9,<4" \
      "pysnmp>=7.1.0,<8" \
      "bacpypes3>=0.0.102" \
      "paho-mqtt>=2.0,<3"

# client.py = BẢN SAO custom_components/project/client.py (chép khi build bằng
# scripts\deploy-collector.ps1) - sửa cách đọc thiết bị thì sửa ở integration rồi build lại.
COPY app /app
WORKDIR /app
CMD ["python3", "-u", "/app/collector.py"]
