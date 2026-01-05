import json
import logging
import sys
import os

sys.path.append(os.getcwd())
import config

from kiteconnect import KiteTicker
from common.broker_order_mapper import BrokerOrderMapper

logging.basicConfig(level=logging.INFO, format='[WebSocket] %(message)s')

class ZerodhaWebSocket:
    def __init__(self, api_key, access_token, user_id, callback_func):
        """
        Initialize the WebSocket.
        :param callback_func: Function(channel, message) to publish data externally.
        """
        self.api_key = api_key
        self.access_token = access_token.strip() if access_token else None
        self.user_id = user_id
        
        # External Callback for publishing (Decoupled from Redis)
        self.callback_func = callback_func
        
        # Channel Names (Still useful to define here for routing)
        self.CH_ZERODHA_RESPONSE = config.CH_ZERODHA_RESPONSES
        self.CH_BLITZ_RESPONSE = config.CH_BLITZ_RESPONSES
        
        self.kws = None
        self.is_connected = False
        self.should_reconnect = True
        self.order_cache = {}  # For deduplication 

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
        logging.info("WebSocket connected")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "data": {"source": "zerodha_websocket", "status": "CONNECTED"}
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        logging.warning(f"WebSocket closed: {code} {reason}")
        
        event = {
            "message_type": "SYSTEM_EVENT",
            "broker": "Zerodha",
            "data": {"source": "zerodha_websocket", "status": "DISCONNECTED", "code": code, "reason": reason}
        }
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))
        
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
        self._publish(self.CH_BLITZ_RESPONSE, json.dumps(event))

    def _on_order_update(self, ws, data):
        #-----------------for duplication---------
        try:
            order_id = data.get('order_id')
            status = data.get('status')
            filled_qty = data.get('filled_quantity', 0)
            cancelled_qty = data.get('cancelled_quantity', 0)
            
            # --- DEDUPLICATION LOGIC ---
            current_state = (status, filled_qty, cancelled_qty)
            last_state = self.order_cache.get(order_id)
            
            if last_state == current_state:
                #logging.info(f"Ignored duplicate update for {order_id}: {status}")
                return
            
            self.order_cache[order_id] = current_state
            # ---------------------------
            
            logging.info(f"Order Update: {order_id} [{status}]")
            logging.info(f"Zerodha raw response: {data}")
            
            # 1. Publish Raw Zerodha Data (Unmodified) to Zerodha Channel
            self._publish(self.CH_ZERODHA_RESPONSE, json.dumps(data))
            logging.info(f"Published RAW update to {self.CH_ZERODHA_RESPONSE}")
            
            # 2. Map to Blitz OrderLog format
            order_log = BrokerOrderMapper.map("zerodha", data)
            
            # 3. Publish STANDARDIZED Blitz format to Blitz Channel
            blitz_response = {
                #"message_type": "ORDER_UPDATE",
                "broker": "Zerodha",
                "data": order_log.to_dict()
            }
            self._publish(self.CH_BLITZ_RESPONSE, json.dumps(blitz_response))
            logging.info(f"Published standardized order to {self.CH_BLITZ_RESPONSE}")
            logging.info(f"Blitz response: {blitz_response}")

        except Exception as e:
            logging.error(f"Error processing order update: {e}")


    # -----------  Market data -----------
    # def _on_ticks(self, ws, ticks):
    #     try:
    #         response = {
    #             "message_type": "MARKET_DATA",
    #             "broker": "Zerodha",
    #             "data": ticks
    #         }
    #         # Wrapper for datetime serialization
    #         self._publish(self.CH_BLITZ_RESPONSE, json.dumps(response, default=str))
    #     except Exception as e:
    #         logging.error(f"Error publishing ticks: {e}")


if __name__ == "__main__":
    print("This file is designed to be imported by zerodha_connector.py")
