import json
import logging
import threading

try:
    import websocket  # websocket-client library
except ImportError:  # pragma: no cover - runtime guard
    websocket = None


# Dedicated logger so MOFL logs are clearly marked and independent
logger = logging.getLogger("MOFL")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[MOFL] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class MOFLWebSocket:
    """
    Lightweight Motilal Oswal WebSocket client.

    - Connects to the official MO OpenAPI WebSocket endpoint.
    - Authenticates using clientid, authtoken and apikey.
    - Subscribes to order updates by default (can be extended to trades/market data).
    - Forwards all raw JSON messages to the provided callback(channel, message).
    """

    WS_URL = "wss://openapi.motilaloswal.com/ws"

    def __init__(self, api_key, access_token, user_id, callback_func, entity_id=None):
        """
        :param api_key: MOFL API key
        :param access_token: AuthToken / JWT from REST login
        :param user_id: Trading client id (e.g. CLIENT_ID from Redis)
        :param callback_func: function(channel: str, message: str) -> None
        """
        self.api_key = api_key
        # Internally we use MO field names: clientid + authtoken
        self.client_id = user_id
        self.auth_token = access_token
        self.entity_id = entity_id
        self.callback_func = callback_func

        self.ws = None
        self._thread = None
        self._should_run = False

        # Channels
        self.CH_MOFL_RESPONSE = "mofl.response"
        self.CH_BLITZ_RESPONSE = "blitz.response"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(self):
        """Start the WebSocket connection in a background thread."""
        if websocket is None:
            logger.error("websocket-client package is not installed; cannot start WebSocket")
            return

        if not (self.api_key and self.client_id and self.auth_token):
            logger.error("Missing credentials for MOFL WebSocket (api_key/client_id/auth_token)")
            return

        self._should_run = True

        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self._thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self._thread.start()
        logger.info(f"WebSocket thread started for entity={self.entity_id} client_id={self.client_id}")

    def stop(self):
        """Stop the WebSocket connection."""
        self._should_run = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        logger.info(f"WebSocket connection closed (manual stop) for entity={self.entity_id}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _publish(self, channel, message):
        """Invoke external callback safely."""
        if self.callback_func:
            try:
                self.callback_func(channel, message)
            except Exception as e:  # pragma: no cover - defensive
                logger.error(f"Error in WebSocket callback: {e}")

    def _send_json(self, payload: dict):
        try:
            if self.ws:
                self.ws.send(json.dumps(payload))
        except Exception as e:
            logger.error(f"Failed to send payload on WebSocket: {e}")

    # ------------------------------------------------------------------
    # websocket-client callbacks
    # ------------------------------------------------------------------
    def _on_open(self, ws):
        logger.info(f"WebSocket connected for entity={self.entity_id}, sending auth...")

        # Auth message as per MO OpenAPI spec
        auth_msg = {
            "clientid": self.client_id,
            "authtoken": self.auth_token,
            "apikey": self.api_key,
        }
        self._send_json(auth_msg)

        # Subscribe to order updates by default
        sub_msg = {
            "clientid": self.client_id,
            "action": "OrderSubscribe",
        }
        self._send_json(sub_msg)

        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "MOFL",
            "entity_id": self.entity_id,
            "data": {"source": "mofl_websocket", "status": "CONNECTED"},
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_message(self, ws, message):
        """
        Raw messages are forwarded as-is to `mofl.response`.
        Higher-level mapping (e.g. to OrderLog) can be added later.
        """
        logger.info(f"message (entity={self.entity_id}): {message}")
        self._publish(self.CH_MOFL_RESPONSE, message)

    def _on_error(self, ws, error):
        logger.error(f"error: {error} (entity={self.entity_id})")
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "MOFL",
            "entity_id": self.entity_id,
            "data": {"source": "mofl_websocket", "status": "ERROR", "error": str(error)},
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_close(self, ws, code, reason):
        logger.warning(f"closed: {code} {reason} (entity={self.entity_id})")
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "MOFL",
            "entity_id": self.entity_id,
            "data": {
                "source": "mofl_websocket",
                "status": "DISCONNECTED",
                "code": code,
                "reason": reason,
            },
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

        if self._should_run and self.ws is not None:
            # Simple auto-reconnect with small backoff
            import time

            logger.info(f"Reconnecting WebSocket for entity={self.entity_id} in 3 seconds...")
            time.sleep(3)
            try:
                self.ws.run_forever()
            except Exception as e:
                logger.error(f"Failed to reconnect WebSocket: {e}")


