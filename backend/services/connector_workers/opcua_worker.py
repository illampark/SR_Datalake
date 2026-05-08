"""OPC-UA polling worker — asyncua sync client."""
import logging

from asyncua.sync import Client

from backend.models.collector import OpcuaConnector

from .base import BaseConnectorWorker

logger = logging.getLogger(__name__)


class OpcuaWorker(BaseConnectorWorker):
    connector_type = "opcua"
    model = OpcuaConnector

    def _snapshot_meta(self, c, tags):
        return {
            "server_url": c.server_url,
            "auth_type": c.auth_type or "anonymous",
            "username": c.username or "",
            "password": c.password or "",
            "security_policy": c.security_policy or "None",
            "security_mode": c.security_mode or "None",
            "polling_interval": c.polling_interval,
            "tags": [
                {
                    "name": t.tag_name,
                    "node_id": t.node_id,
                    "dtype": t.data_type or "float",
                }
                for t in tags
            ],
        }

    def _connect(self, meta):
        client = Client(meta["server_url"], timeout=5)
        if meta["security_policy"] != "None":
            try:
                client.set_security_string(
                    f"{meta['security_policy']},{meta['security_mode']},"
                )
            except Exception:
                logger.warning("[opcua/%d] security_string apply failed (정책=%s)",
                               self.connector_id, meta["security_policy"])
        if meta["auth_type"] == "username" and meta["username"]:
            client.set_user(meta["username"])
            client.set_password(meta["password"])
        client.connect()
        self._client = client

    def _close_client(self):
        try:
            self._client.disconnect()
        except Exception:
            pass

    def _read_all_tags(self, meta):
        if not self._client or not meta.get("tags"):
            return []
        out = []
        for tag in meta["tags"]:
            try:
                node = self._client.get_node(tag["node_id"])
                val = node.get_value()
                out.append((tag["name"], val, tag["dtype"], "", 100))
            except Exception as e:
                logger.debug("[opcua/%d] read fail node=%s err=%s",
                             self.connector_id, tag["node_id"], e)
                continue
        return out
