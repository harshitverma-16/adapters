import json
import logging
import sys
import os

sys.path.append(os.getcwd()) 

from kiteconnect import KiteTicker
from common.broker_order_mapper import BrokerOrderMapper

# Dedicated logger so Zerodha logs are clearly marked
logger = logging.getLogger("Zerodha")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[Zerodha] %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False

class ZerodhaWebSocket:
    def __init__(self, api_key, access_token, user_id, callback_func, entity_id=None):
        """
        Initialize the WebSocket.
        :param callback_func: Function(channel, message) to publish data externally.
        """
        self.api_key = api_key
        self.access_token = access_token.strip() if access_token else None
        self.user_id = user_id
        self.entity_id = entity_id
        
        # External Callback for publishing (Decoupled from Redis)
        self.callback_func = callback_func
        
        # Channel Names (Still useful to define here for routing)
        self.CH_ZERODHA_RESPONSE = "zerodha.response"
        self.CH_BLITZ_RESPONSE = "blitz.response"
        
        self.kws = None
        self.is_connected = False
        self.should_reconnect = True 

    def start(self):
        """Initializes the KiteTicker and connects in a background thread."""
        logger.info(f"Initializing KiteTicker for entity={self.entity_id} user_id={self.user_id}...")
        self.should_reconnect = True
        
        ws_token = f"{self.access_token}&user_id={self.user_id}"
        
        logging.debug(f"API_KEY: {self.api_key}")
        logging.debug(f"WS_TOKEN: {ws_token[:15]}... (Length: {len(ws_token)})")
        
        self.kws = KiteTicker(self.api_key, ws_token)

        self.kws.on_connect = self._on_connect
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_order_update = self._on_order_update
        self.kws.on_ticks = self._on_ticks

        self.kws.connect(threaded=True)

    def stop(self):
        """Stops the WebSocket connection."""
        self.should_reconnect = False
        if self.kws:
            self.kws.close()
            logging.info("WebSocket connection closed manually.")

    def subscribe(self, tokens, mode="full"):
        if self.is_connected and self.kws:
            logging.info(f"Subscribing to: {tokens} | Mode: {mode}")
            self.kws.subscribe(tokens)
            self.kws.set_mode(self.kws.MODE_FULL if mode == "full" else self.kws.MODE_QUOTE, tokens)
        else:
            logging.warning("Cannot subscribe: WebSocket is not connected.")

    # ----------------------------------------------------------------
    # WEBSOCKET CALLBACKS
    # ----------------------------------------------------------------

    def _publish(self, channel, message):
        """Helper to invoke external callback safely."""
        if self.callback_func:
            self.callback_func(channel, message)

    def _on_connect(self, ws, response):
        self.is_connected = True
        logger.info(f"WebSocket connected for entity={self.entity_id} user_id={self.user_id}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "entity_id": self.entity_id,
            "data": {"source": "zerodha_websocket", "status": "CONNECTED"}
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        logger.warning(f"WebSocket closed for entity={self.entity_id}: {code} {reason}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "entity_id": self.entity_id,
            "data": {"source": "zerodha_websocket", "status": "DISCONNECTED", "code": code, "reason": reason}
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))
        
        if self.should_reconnect:
            import time
            logger.info(f"Reconnecting WebSocket for entity={self.entity_id} in 3 seconds...")
            time.sleep(3)
            if self.kws:
                 self.kws.connect(threaded=True)
        else:
            logger.info("Reconnection suppressed (Manual Stop).")
    
    def _on_error(self, ws, code, reason):
        logger.error(f"WebSocket error for entity={self.entity_id}: {code} {reason}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "entity_id": self.entity_id,
            "data": {"source": "zerodha_websocket", "status": "ERROR", "code": code, "reason": reason}
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_order_update(self, ws, data):
        try:
            logger.info(f"Order Update: {data.get('order_id')} [{data.get('status')}] (entity={self.entity_id})")
            
            # 1. Publish Raw Zerodha Data (Unmodified) to Zerodha Channel
            self._publish(self.CH_ZERODHA_RESPONSE, json.dumps(data))
            logger.info(f"Published RAW update to {self.CH_ZERODHA_RESPONSE}")
            
            # 2. Map to Blitz OrderLog format
            order_log = BrokerOrderMapper.map("zerodha", data)
            
            # 3. Publish STANDARDIZED Blitz format to Blitz Channel
            blitz_response = {
                "message_type": "ORDER_UPDATE",
                "broker": "Zerodha",
                "data": order_log.to_dict()
            }
            self._publish(self.CH_BLITZ_RESPONSE, json.dumps(blitz_response))
            logger.info(f"Published standardized order to {self.CH_BLITZ_RESPONSE}")

        except Exception as e:
            logger.error(f"Error processing order update: {e}")

    def _on_ticks(self, ws, ticks):
        try:
            response = {
                "message_type": "MARKET_DATA",
                "broker": "Zerodha",
                "data": ticks
            }
            # Wrapper for datetime serialization
            self._publish(self.CH_BLITZ_RESPONSE, json.dumps(response, default=str))
        except Exception as e:
            logger.error(f"Error publishing ticks: {e}")


if __name__ == "__main__":
    print("This file is designed to be imported by zerodha_connector.py")
