import json
import logging
import sys
import os

sys.path.append(os.getcwd()) 

from kiteconnect import KiteTicker
from Zerodha.mapper import ZerodhaMapper
from common.redis_publisher import RedisPublisher

logging.basicConfig(level=logging.INFO, format='[WebSocket] %(message)s')

class ZerodhaWebSocket:
    def __init__(self, api_key, access_token, user_id):
        """
        Initialize the WebSocket.
        """
        self.api_key = api_key
        self.access_token = access_token.strip() if access_token else None
        self.user_id = user_id
        
        self.kws = None
        self.is_connected = False
        self.should_reconnect = True 

    def start(self):
        """Initializes the KiteTicker and connects in a background thread."""
        logging.info("Initializing KiteTicker...")
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



    # WEBSOCKET CALLBACKS

    def _on_connect(self, ws, response):
        self.is_connected = True
        logging.info("WebSocket connected")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "data": {"source": "zerodha_websocket", "status": "CONNECTED"}
        }
        logging.info(f"Connected Event: {event}")

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        logging.warning(f"WebSocket closed: {code} {reason}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "data": {"source": "zerodha_websocket", "status": "DISCONNECTED", "code": code, "reason": reason}
        }
        logging.info(f"Close Event: {event}")
        
        if self.should_reconnect:
            import time
            logging.info("Reconnecting in 3 seconds...")
            time.sleep(3)
            if self.kws:
                 self.kws.connect(threaded=True)
        else:
            logging.info("Reconnection suppressed (Manual Stop).")
    
    def _on_error(self, ws, code, reason):
        logging.error(f"WebSocket error: {code} {reason}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "data": {"source": "zerodha_websocket", "status": "ERROR", "code": code, "reason": reason}
        }
        logging.info(f"Error Event: {event}")

    def _on_order_update(self, ws, data):
        try:
            logging.info(f"Order Update: {data.get('order_id')} [{data.get('status')}]")
            
            # 1. Publish Raw Zerodha Data (Unmodified) to Zerodha Channel
            logging.info(f"[WS RAW ZERODHA] {json.dumps(data)}")
            
            # 2. Use ZerodhaMapper for standardization
            blitz_response = ZerodhaMapper.to_blitz(data, "orders")
            if blitz_response:
                 logging.info(f"[WS BLITZ STANDARD] {json.dumps(blitz_response)}")

        except Exception as e:
            logging.error(f"Error processing order update: {e}")



if __name__ == "__main__":
    print("This file is designed to be imported by zerodha_connector.py")
