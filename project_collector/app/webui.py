"""Giao diện quản lý Project Collector qua Ingress của HA (2026-10-02).

Tương đương menu Configure của integration Project cũ: danh sách thiết bị, thêm / sửa / xoá thiết bị,
bảng điểm đo (OID / thanh ghi...), đọc thử, mẫu thiết bị, quét. Chỉ nhận kết nối từ Ingress của HA
(172.30.32.2) -> chỉ người đã đăng nhập HA (admin, panel_admin) mới mở được.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

log = logging.getLogger("project_collector.webui")
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
ALLOWED_CLIENTS = ("172.30.32.2", "127.0.0.1")


def start_webui(manager, port=8099):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ProjectCollector"

        def log_message(self, fmt, *args):  # log HTTP gọn
            log.debug("%s - %s", self.client_address[0], fmt % args)

        # ---- tiện ích ----
        def _send(self, code, body, ctype="application/json; charset=utf-8"):
            data = body if isinstance(body, (bytes, bytearray)) else json.dumps(body, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if not n:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8") or "{}")

        def _allowed(self):
            if self.client_address[0] in ALLOWED_CLIENTS:
                return True
            self._send(403, {"error": "Only via Home Assistant (Ingress)"})
            return False

        def _route(self, method):
            if not self._allowed():
                return
            path = urlparse(self.path).path.rstrip("/") or "/"
            parts = [unquote(p) for p in path.split("/") if p]
            try:
                if method == "GET" and parts in ([], ["index.html"]):
                    with open(os.path.join(WEB_DIR, "index.html"), "rb") as f:
                        return self._send(200, f.read(), "text/html; charset=utf-8")
                if not parts or parts[0] != "api":
                    return self._send(404, {"error": "Not found"})
                p = parts[1:]
                if p == ["info"] and method == "GET":
                    return self._send(200, {"version": manager.version, "site": manager.site})
                if p == ["devices"]:
                    if method == "GET":
                        return self._send(200, manager.list())
                    if method == "POST":
                        return self._send(200, manager.save(self._body()))
                if len(p) == 2 and p[0] == "devices":
                    if method == "GET":
                        return self._send(200, manager.get(p[1]))
                    if method == "PUT":
                        return self._send(200, manager.save(self._body(), p[1]))
                    if method == "DELETE":
                        manager.delete(p[1])
                        return self._send(200, {"ok": True})
                if len(p) == 3 and p[0] == "devices" and p[2] == "polling" and method == "POST":
                    return self._send(200, manager.polling(p[1], self._body().get("on")))
                if p == ["test"] and method == "POST":
                    b = self._body()
                    return self._send(200, manager.test(b.get("device") or {}, b.get("keys")))
                if p == ["templates"]:
                    if method == "GET":
                        return self._send(200, manager.templates())
                    if method == "POST":
                        b = self._body()
                        return self._send(200, manager.save_template(b.get("device_id"), b.get("name")))
                if len(p) == 2 and p[0] == "templates" and method == "DELETE":
                    manager.delete_template(p[1])
                    return self._send(200, {"ok": True})
                if p == ["scan"]:
                    if method == "GET":
                        return self._send(200, manager.scan_result())
                    if method == "POST":
                        manager.scan_start()
                        return self._send(200, {"ok": True})
                return self._send(404, {"error": "Not found"})
            except KeyError as err:
                return self._send(404, {"error": str(err).strip("'")})
            except (ValueError, TypeError) as err:
                return self._send(400, {"error": str(err)})
            except Exception as err:  # noqa: BLE001
                log.exception("Lỗi giao diện %s %s", method, self.path)
                return self._send(500, {"error": f"{type(err).__name__}: {err}"})

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

        def do_PUT(self):
            self._route("PUT")

        def do_DELETE(self):
            self._route("DELETE")

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="webui", daemon=True).start()
    log.info("Giao diện quản lý: cổng %s (Ingress)", port)
    return server
