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
                self.redis.publish(config.CH_ZERODHA_RESPONSES, "Login Successful")
                
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
                
                elif action == "SUBSCRIBE_MARKET_DATA":
                    # Subscribe to market data
                    tokens = blitz_data.get("tokens", [])
                    mode = blitz_data.get("mode", "full")
                    self._subscribe_market_data(tokens, mode)
                    result = {"message": f"Subscribed to {len(tokens)} instruments"}

                elif action == "PLACE_ORDER":
                    params = self._blitz_to_zerodha(blitz_data)
                    logging.info(f"Zerodha payload: {params}")
                    result = self.adapter.place_order(
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
                    
                    # # --- Publish Standardized "PENDING" Order to blitz.response ---
                    # try:
                    #     order_id = result if isinstance(result, str) else result.get("order_id")
                        
                    #     # Create synthetic OrderLog
                    #     o = OrderLog()
                    #     o.ExchangeOrderId = order_id
                    #     o.InstrumentName = f"{params['exchange']}:{params['symbol']}"
                    #     o.OrderSide = params["transaction_type"]
                    #     o.OrderType = params["order_type"]
                    #     o.OrderQuantity = params["qty"]
                    #     o.OrderPrice = params["price"]
                    #     o.OrderStatus = "PENDING"  # Initial status
                    #     o.UserText = {"source": "connector_place_order", "params": params}
                        
                    #     blitz_response = {
                    #         "message_type": "ORDER_UPDATE",
                    #         "broker": "Zerodha",
                    #         "data": o.to_dict()
                    #     }
                    #     self.redis.publish(config.CH_BLITZ_RESPONSES, json.dumps(blitz_response))
                    #     logging.info(f"Published PENDING order {order_id} to blitz.response")
                    # except Exception as e:
                    #     logging.error(f"Failed to publish standardized response: {e}")

                elif action == "MODIFY_ORDER":
                    result = self.adapter.modify_order(
                        order_id=blitz_data.get("BOID"),
                        order_type=blitz_data.get("orderType", "LIMIT"),
                        qty=int(blitz_data.get("quantity", 0)),
                        validity=blitz_data.get("validity", "DAY"),
                        price=blitz_data.get("price")
                    )

                elif action == "CANCEL_ORDER":
                    result = self.adapter.cancel_order(blitz_data.get("BOID"))

                elif action == "GET_ORDERS":
                    result = self.adapter.get_orders()

                elif action == "GET_ORDER_DETAILS":
                    result = self.adapter.get_order_details(blitz_data.get("BOID"))

                elif action == "GET_HOLDINGS":
                    result = self.adapter.get_holdings()

                elif action == "GET_POSITIONS":
                    result = self.adapter.get_positions()

                else:
                    raise ValueError(f"Unknown Action: {action}")

            except Exception as e:
                logging.error(f" !! Error executing {action}: {e}")
                status = "ERROR"
                error_msg = str(e)
                # Publish error to Zerodha channel
                self.redis.publish(config.CH_ZERODHA_RESPONSES, f" !! Error executing {action}: {e}")

            self._send_response_to_blitz(status, result, error_msg)

        except json.JSONDecodeError:
            logging.critical(" !! Critical: Failed to decode JSON message from Redis")

    def _blitz_to_zerodha(self, data):
        symbol_parts = data.get("symbol", "").split("|")
        exchange = symbol_parts[0] if len(symbol_parts) > 1 else "NSE"
        
        # Use 'symbol' instead of 'tradingsymbol' to match Adapter.place_order arguments
        symbol = symbol_parts[1] if len(symbol_parts) > 1 else data.get("symbol", "")
        
        payload = {
            "symbol": symbol,
            "exchange": exchange,
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
                callback_func=self._publish_websocket_data
            )
            self.websocket.start()
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

    def _subscribe_market_data(self, tokens, mode="full"):
        """Subscribe to market data for given instrument tokens."""
        if not self.websocket:
            raise RuntimeError("WebSocket not initialized. Login first.")
        
        self.websocket.subscribe(tokens, mode)
        logging.info(f"Subscribed to {len(tokens)} instruments in {mode} mode")

    def _send_response_to_blitz(self, status, data, error):
        response = {
            #"broker": "Zerodha",
            #"request_id": req_id,
            "status": status,
            "data": data,
            "error": error
        }
        self.redis.publish(config.CH_BLITZ_RESPONSES, json.dumps(response))

if __name__ == "__main__":
    connector = ZerodhaConnector()
    connector.start()
