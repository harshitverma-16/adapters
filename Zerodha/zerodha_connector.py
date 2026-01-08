import json
import redis
import threading
import logging
import sys
import os

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from Zerodha.zerodha_adapter import ZerodhaAdapter
from Zerodha.zerodha_websocket import ZerodhaWebSocket
from common.broker_order_mapper import OrderLog

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class ZerodhaConnector:
    def __init__(self):
        logging.info("[Connector] Connecting to Redis...")
        self.redis = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        self.is_running = False
        
        # Simple dictionary for order ID mapping: blitz_id -> zerodha_id
        self.blitz_to_zerodha = {}
        self.zerodha_to_blitz = {}
        
        logging.info("[Connector] Connected to Redis successfully.")
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "Connected to Redis successfully.")


        logging.info("Initializing Zerodha Connector...")
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "Initializing Zerodha Connector...")
        self.adapter = ZerodhaAdapter(config.API_KEY, config.API_SECRET, config.REDIRECT_URL)
        self.websocket = None  # Will be initialized after login
        logging.info("Zerodha Connector initialized successfully.")
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "Zerodha Connector initialized successfully.")
        logging.info("Login using this URL:")
        logging.info(self.adapter.auth_api.generate_login_url())
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "Login using this URL: " + self.adapter.auth_api.generate_login_url())

        try:
            token = input("Paste 'request_token' from browser here: ").strip()
            if token:
                self.adapter.login(token)
                logging.info("Login Successful")
                self.redis.publish(config.CH_ZERODHA_RESPONSES, "Login Successful") #have multiple connections 
                
                # Initialize and start WebSocket
                self._start_websocket()
        except Exception as e:
            logging.error(f"Login Failed: {e} !!")
            self.redis.publish(config.CH_ZERODHA_RESPONSES, f"Login Failed: {e} !!")


    def start(self):
        self.pubsub.subscribe(config.CH_BLITZ_REQUESTS)
        self.is_running = True
        logging.info(f"[Connector] Online and listening on '{config.CH_BLITZ_REQUESTS}'...")
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "[Connector] Online and listening on" + config.CH_BLITZ_REQUESTS)

        for message in self.pubsub.listen():
            if not self.is_running:
                break
            if message["type"] == "message":
                # message is added by redis in payload automatically {
                # "type": "message",            # ← ADDED BY REDIS
                # "pattern": None,
                # "channel": "adapter.channel",
                # "data": '{"request_id":"place_001","action":"PLACE_ORDER","data":{...}}'
                # }
                threading.Thread(target=self._process_message, args=(message["data"],)).start()

    def stop(self):
        self.is_running = False
        self.pubsub.unsubscribe()
        
        # Stop WebSocket
        if self.websocket:
            self.websocket.stop()
            
        self.adapter.logout()
        logging.info("[Connector] Stopped.")
        self.redis.publish(config.CH_ZERODHA_RESPONSES, "[Connector] Stopped.")

    def _process_message(self, raw_data):
        try:
            payload = json.loads(raw_data)
            #req_id = payload.get("request_id")
            action = payload.get("action")
            blitz_data = payload.get("data", {})

            logging.info(f" -> Received: {action}")
            self.redis.publish(config.CH_ZERODHA_RESPONSES, f" -> Received: {action}")
            
            result = None
            status = "SUCCESS"
            error_msg = None

            try:
                if action == "GET_LOGIN_URL":
                    url = self.adapter.auth_api.generate_login_url()
                    result = {"login_url": url}

                elif action == "LOGIN":
                    req_token = blitz_data.get("request_token")
                    if not req_token:
                        raise ValueError("Missing 'request_token'")
                    result = self.adapter.login(req_token)
                    # Start websocket after login
                    self._start_websocket()

                elif action == "LOGOUT":
                    if self.websocket:
                        self.websocket.stop()
                    self.adapter.logout()
                    result = {"message": "Logged out successfully"}
                

                elif action == "PLACE_ORDER":
                    # Get Blitz order ID (required)
                    blitz_order_id = blitz_data.get("BlitzOrderID") or blitz_data.get("blitz_order_id")
                    if not blitz_order_id:
                        raise ValueError("'BlitzOrderID' is required for PLACE_ORDER")
                    
                    params = self._blitz_to_zerodha(blitz_data)
                    logging.info(f"Zerodha payload: {params}")
                    zerodha_result = self.adapter.place_order(
                        symbol=params["symbol"],
                        qty=params["qty"],
                        order_type=params["order_type"],
                        transaction_type=params["transaction_type"],
                        product=params["product"],
                        exchange=params["exchange"],
                        price=params["price"],
                        trigger_price=params["trigger_price"],
                        validity=params["validity"]
                    )
                    
                    # Extract Zerodha order ID from result
                    # Response format: {'status': 'success', 'data': {'order_id': '...'}}
                    if isinstance(zerodha_result, dict):
                        # Check if order_id is nested in 'data' field
                        if "data" in zerodha_result and isinstance(zerodha_result["data"], dict):
                            zerodha_order_id = zerodha_result["data"].get("order_id")
                        else:
                            zerodha_order_id = zerodha_result.get("order_id")
                    else:
                        zerodha_order_id = zerodha_result
                    
                    # Store the mapping in dictionary
                    self.blitz_to_zerodha[blitz_order_id] = str(zerodha_order_id)
                    self.zerodha_to_blitz[str(zerodha_order_id)] = blitz_order_id
                    logging.info(f"Mapped: {blitz_order_id} → {zerodha_order_id}")
                    
                    # Return both IDs in result
                    result = {
                        "blitz_order_id": blitz_order_id,
                        "zerodha_order_id": zerodha_order_id,
                        "raw_response": zerodha_result
                    }


                elif action == "MODIFY_ORDER":
                    # Resolve order ID (accepts both Blitz and Zerodha IDs)
                    zerodha_order_id = self._resolve_order_id(blitz_data)
                    
                    result = self.adapter.modify_order(
                        order_id=zerodha_order_id,
                        order_type=blitz_data.get("orderType", "LIMIT"),
                        qty=int(blitz_data.get("quantity", 0)),
                        validity=blitz_data.get("validity", "DAY"),
                        price=blitz_data.get("price")
                    )

                elif action == "CANCEL_ORDER":
                    # Resolve order ID (accepts both Blitz and Zerodha IDs)
                    zerodha_order_id = self._resolve_order_id(blitz_data)
                    result = self.adapter.cancel_order(zerodha_order_id)

                elif action == "GET_ORDERS":
                    result = self.adapter.get_orders()
                    print(result)

                elif action == "GET_ORDER_DETAILS":
                    # Resolve order ID (accepts both Blitz and Zerodha IDs)
                    zerodha_order_id = self._resolve_order_id(blitz_data)
                    result = self.adapter.get_order_details(zerodha_order_id)
                    print(result)

                elif action == "GET_HOLDINGS":
                    result = self.adapter.get_holdings()
                    print(result)

                elif action == "GET_POSITIONS":
                    result = self.adapter.get_positions()
                    print(result)

                else:
                    raise ValueError(f"Unknown Action: {action}")

            except Exception as e:
                logging.error(f" !! Error executing {action}: {e}")
                status = "ERROR"
                error_msg = str(e)
                # Publish error to Zerodha channel
                self.redis.publish(config.CH_ZERODHA_RESPONSES, f" !! Error executing {action}: {e}")

            #self._send_response_to_blitz(result, error_msg)

        except json.JSONDecodeError:
            logging.critical(" !! Critical: Failed to decode JSON message from Redis")

    def _resolve_order_id(self, data):
        """Helper to resolve order ID from either blitz_order_id or order_id."""
        # Check for Blitz order ID first
        blitz_order_id = data.get("BlitzOrderID")
        if blitz_order_id:
            zerodha_order_id = self.blitz_to_zerodha.get(blitz_order_id)
            if not zerodha_order_id:
                raise ValueError(f"Blitz order ID '{blitz_order_id}' not found in mapping")
            logging.info(f"Resolved Blitz ID {blitz_order_id} → Zerodha ID {zerodha_order_id}")
            return zerodha_order_id
        
        # Fall back to direct Zerodha order ID
        order_id = data.get("order_id")
        if order_id:
            return order_id
        
        raise ValueError("Either 'blitz_order_id' or 'order_id' must be provided")
    
    def _blitz_to_zerodha(self, data):
        #symbol_parts = data.get("symbol", "").split("|")
        #exchange = symbol_parts[0]
        
        # Use 'symbol' instead of 'tradingsymbol' to match Adapter.place_order arguments
        #symbol = symbol_parts[1] if len(symbol_parts) > 1 else data.get("symbol", "")
        
        payload = {
            "symbol": data.get("InstrumentName"),
            "exchange": data.get("ExchangeSegment")[:3],
            "transaction_type": data.get("orderSide"),
            "order_type": data.get("orderType"),
            "qty": int(data.get("quantity", 0)), # Renamed to qty and ensure int
            "product": data.get("product"),
            "price": data.get("price"),
            "trigger_price": data.get("stopPrice"),
            "validity": data.get("tif")
        }
        return payload

    def _start_websocket(self):
        """Initialize and start the WebSocket connection."""
        try:
            if not self.adapter.access_token:
                logging.warning("Cannot start WebSocket: Not logged in")
                return
            
            logging.info("Starting WebSocket connection...")
            self.websocket = ZerodhaWebSocket(
                api_key=config.API_KEY,
                access_token=self.adapter.access_token,
                user_id=config.USER_ID,
                callback_func=self._publish_websocket_data,
                order_id_mapper=self.zerodha_to_blitz  # Pass the mapping dictionary
            )
            logging.info("WebSocket started successfully")
            self.redis.publish(config.CH_ZERODHA_RESPONSES, "WebSocket started successfully")
        except Exception as e:
            logging.error(f"Failed to start WebSocket: {e}")
            self.redis.publish(config.CH_ZERODHA_RESPONSES, f"Failed to start WebSocket: {e}")

    def _publish_websocket_data(self, channel, message):
        """Callback to publish data from WebSocket to Redis."""
        try:
            self.redis.publish(channel, message)
        except Exception as e:
            logging.error(f"Failed to publish WebSocket data to Redis: {e}")




if __name__ == "__main__":
    connector = ZerodhaConnector()
    connector.start()
