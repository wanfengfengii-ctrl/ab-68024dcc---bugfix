"""测试夹具：在本机随机端口启动接收模拟器与 API（含投递池）。"""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from typing import Optional

from relay.api import Config, create_server as create_api_server
from relay.receiver import FaultController, create_server as create_receiver_server
from relay.store import AlertStore, ReceiverStore

SECRET = "unit-test-secret"


class LocalHarness:
    def __init__(self, *, backoff_base: float = 0.01, timeout: float = 1.0) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="relay-test-"))
        self.faults = FaultController()
        self.receiver_store = ReceiverStore(str(self.tmp / "receiver.db"))
        self.receiver = create_receiver_server(
            "127.0.0.1", 0, self.receiver_store, SECRET, self.faults
        )
        self._receiver_thread = threading.Thread(
            target=self.receiver.serve_forever, daemon=True
        )
        self._receiver_thread.start()
        self.receiver_port = self.receiver.server_address[1]

        self.cfg = Config(
            receiver_host="127.0.0.1",
            receiver_port=self.receiver_port,
            secret=SECRET,
            backoff_base=backoff_base,
            timeout=timeout,
            sweep_interval=0.05,
            workers=2,
        )
        self.api_store: Optional[AlertStore] = None
        self.api = None
        self.pool = None
        self.api_port = 0
        self.start_api()

    def start_api(self) -> None:
        self.api_store = AlertStore(str(self.tmp / "api.db"))
        self.api, self.pool = create_api_server(
            "127.0.0.1", 0, self.api_store, self.cfg
        )
        self._api_thread = threading.Thread(
            target=self.api.serve_forever, daemon=True
        )
        self._api_thread.start()
        self.api_port = self.api.server_address[1]

    def stop_api(self) -> None:
        assert self.pool and self.api and self.api_store
        self.pool.stop()
        self.api.shutdown()
        self.api.server_close()
        self.api_store.close()
        self.api = None
        self.pool = None

    def restart_api(self) -> None:
        """模拟投递中 API 进程重启：旧进程全停，新进程重开同一数据库。"""
        self.stop_api()
        self.start_api()

    @property
    def api_base(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    @property
    def admin_base(self) -> str:
        return f"http://127.0.0.1:{self.receiver_port}"

    def stop(self) -> None:
        if self.api is not None:
            self.stop_api()
        self.receiver.shutdown()
        self.receiver.server_close()
        self.receiver_store.close()
