"""Run unit tests without loading local secrets or permitting outbound sockets."""
import os
from pathlib import Path
import socket
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# A deterministic environment is part of this runner, not production behavior.
os.environ.clear()
os.environ.update(CSZY_LOAD_DOTENV="0", ALPACA_MODE="paper", TRADE_ENV="paper", LOG_DIR="/tmp/cszy-test-logs")

class OfflineSocket(socket.socket):
    def connect(self, *args, **kwargs):
        raise RuntimeError("Unit tests may not connect to external services")
    connect_ex = connect

socket.socket = OfflineSocket
result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover("tests"))
sys.exit(not result.wasSuccessful())
