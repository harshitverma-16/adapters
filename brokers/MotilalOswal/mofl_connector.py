import json
import threading
import logging
import redis

from brokers.MotilalOswal.mofl_adapter import MotilalOswalAdapter
from brokers.MotilalOswal.mofl_websocket import MOFLWebSocket
from common.broker_order_mapper import OrderLog

# ====================== CONFIG ======================
CH_BLITZ_REQUESTS = "adapter.channel"   # Incoming Blitz commands
CH_MOFL_RESPONSES = "mofl.response"    # Outgoing MOFL responses

# ====================== LOGGING ======================
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


class MOFLConnector:
    def __init__(self, entity_id, creds):
        """
        entity_id: mandatory, used for TPOMS
        creds: dict containing keys like api_key, api_secret, access_token, user_id
        """
        if not entity_id:
            raise ValueError("entity_id is mandatory for TPOMS connector")
        if not creds:
            raise ValueError(f"Credentials must be provided by TPOMS for entity {entity_id}")

        self.entity_id = entity_id
        self.creds = creds

        # Support both TPOMS-style lowercase keys and MOFL API-style uppercase keys
        self.api_key = creds.get("api_key") or creds.get("API_KEY")
        self.client_code = (
            creds.get("client_code")
            or creds.get("CLIENT_ID")
            or creds.get("client_id")
            or creds.get("user_id")
        )
        # Auth token / JWT from MOFL login
        self.access_token = (
            creds.get("access_token")
            or creds.get("jwt_token")
            or creds.get("AuthToken")
        )
        self.user_id = creds.get("user_id")

        if not self.api_key or not self.client_code:
            raise ValueError(f"Missing api_key or client_code for entity {entity_id}")

        self.redis = redis.Redis(decode_responses=True)
        self.pubsub = self.redis.pubsub()
        self.is_running = False
        self.adapter = MotilalOswalAdapter(
            api_key=self.api_key,
            client_code=self.client_code,
            jwt_token=self.access_token,
            redis_client=self.redis,
        )
        self.websocket = None

        logging.info(f"[MOFLConnector] Initialized for entity {self.entity_id}")

        # Auto start WebSocket if access_token exists
        if self.access_token:
            self._start_websocket()
        else:
            logging.warning(f"[MOFLConnector] No access token. Manual login required for entity {self.entity_id}")

    # ====================== START / STOP ======================
    def start(self):
        """Start listening to Blitz commands"""
        self.pubsub.subscribe(CH_BLITZ_REQUESTS)
        self.is_running = True
        logging.info(f"[MOFLConnector] Listening on '{CH_BLITZ_REQUESTS}' for entity {self.entity_id}...")

        for message in self.pubsub.listen():
            if not self.is_running:
                break
            if message["type"] == "message":
                threading.Thread(target=self._process_message, args=(message["data"],)).start()

    def stop(self):
        """Stop connector gracefully"""
        self.is_running = False
        self.pubsub.unsubscribe()
        if self.websocket:
            self.websocket.stop()
        if self.adapter:
            self.adapter.logout()
        logging.info(f"[MOFLConnector] Stopped entity {self.entity_id}")

    # ====================== MESSAGE PROCESSING ======================
    def _process_message(self, raw_data):
        try:
            payload = json.loads(raw_data)
            req_id = payload.get("request_id")
            action = payload.get("action")
            data = payload.get("data", {})
            target_entity = payload.get("entity_id")
            target_broker = payload.get("broker")

            # Ignore messages not meant for this entity/broker
            if target_entity and target_entity != self.entity_id:
                return
            if target_broker and target_broker != "MOFL":
                return

            logging.info(f"[{self.entity_id}] -> Received: {action} [ID: {req_id}]")

            result = None
            status = "SUCCESS"
            error_msg = None

            try:
                if action == "LOGIN":
                    token = data.get("access_token") or data.get("jwt_token")
                    result = self.adapter.login(jwt_token=token)
                elif action == "GET_ORDERS":
                    result = self.adapter.get_orders()
                elif action == "GET_ORDER_DETAILS":
                    result = self.adapter.get_order_details(data.get("order_id"))
                elif action == "GET_HOLDINGS":
                    result = self.adapter.get_holdings()
                elif action == "GET_POSITIONS":
                    result = self.adapter.get_positions()
                elif action == "PLACE_ORDER":
                    params = self._blitz_to_mofl(data)
                    result = self.adapter.place_order(**params)
                    self._publish_order_update(params, result)
                else:
                    logging.warning(f"[{self.entity_id}] Action '{action}' not implemented")
            except Exception as e:
                logging.error(f"[{self.entity_id}] Error executing {action}: {e}")
                status = "ERROR"
                error_msg = str(e)

            self._send_response_to_blitz(req_id, status, result, error_msg)
        except json.JSONDecodeError:
            logging.critical(f"[{self.entity_id}] Failed to decode JSON message from Redis")

    # ====================== DATA MAPPING ======================
    def _blitz_to_mofl(self, data):
        symbol_parts = data.get("symbol", "").split("|")
        exchange = symbol_parts[0] if len(symbol_parts) > 1 else "NSE"
        symbol = symbol_parts[1] if len(symbol_parts) > 1 else data.get("symbol")

        return {
            "symbol": symbol,
            "exchange": exchange,
            "transaction_type": data.get("orderSide"),
            "order_type": data.get("orderType"),
            "qty": int(data.get("quantity", 0)),
            "product": data.get("product"),
            "price": data.get("price"),
            "trigger_price": data.get("stopPrice"),
            "validity": data.get("tif")
        }

    def _start_websocket(self):
        """
        Placeholder for MOFL WebSocket start.
        Implement actual MOFLWebSocket wiring here when available.
        """
        if not self.access_token:
            logging.warning(f"[{self.entity_id}] Cannot start WebSocket: no access token")
            return

        logging.warning(f"[{self.entity_id}] MOFL WebSocket not implemented; skipping start")

    # ====================== WEBSOCKET ======================
    def _start_websocket(self):
        if not self.access_token:
            logging.warning(f"[{self.entity_id}] Cannot start WebSocket: no access token")
            return

        self.websocket = MOFLWebSocket(
            api_key=self.api_key,
            access_token=self.access_token,
            user_id=self.client_code,
            callback_func=self._publish_websocket_data,
            entity_id=self.entity_id,
        )
        self.websocket.start()
        logging.info(f"[{self.entity_id}] WebSocket started")

    def _publish_websocket_data(self, channel, message):
        try:
            self.redis.publish(channel, message)
        except Exception as e:
            logging.error(f"[{self.entity_id}] Failed to publish WebSocket data: {e}")

    # ====================== ORDER UPDATE ======================
    def _publish_order_update(self, params, order_response):
        try:
            order_id = order_response if isinstance(order_response, str) else order_response.get("order_id")
            o = OrderLog()
            o.ExchangeOrderId = order_id
            o.InstrumentName = f"{params['exchange']}:{params['symbol']}"
            o.OrderSide = params["transaction_type"]
            o.OrderType = params["order_type"]
            o.OrderQuantity = params["qty"]
            o.OrderPrice = params["price"]
            o.OrderStatus = "PENDING"
            o.UserText = {"source": "mofl_connector_place_order", "params": params}

            blitz_response = {
                "message_type": "ORDER_UPDATE",
                "broker": "MOFL",
                "entity_id": self.entity_id,
                "data": o.to_dict()
            }
            self.redis.publish("blitz.response", json.dumps(blitz_response))
            logging.info(f"[{self.entity_id}] Published PENDING order {order_id}")
        except Exception as e:
            logging.error(f"[{self.entity_id}] Failed to publish order update: {e}")

    # ====================== RESPONSE ======================
    def _send_response_to_blitz(self, req_id, status, data, error):
        response = {
            "broker": "MOFL",
            "entity_id": self.entity_id,
            "request_id": req_id,
            "status": status,
            "data": data,
            "error": error
        }
        logging.info(f"[{self.entity_id}] [BLITZ RESPONSE] req_id={req_id} status={status} error={error}")
        self.redis.publish(CH_MOFL_RESPONSES, json.dumps(response))
