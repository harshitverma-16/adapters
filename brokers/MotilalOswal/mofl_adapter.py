import json
import threading
import time

from brokers.MotilalOswal.api.order import MotilalOrderAPI
from brokers.MotilalOswal.api.portfolio import MotilalOswalPortfolioAPI


class MotilalOswalAdapter:
    """
    Thin wrapper around Motilal Oswal APIs.
    Keeps login state and optionally publishes lifecycle events to Redis
    via the provided redis_client (if any).
    """

    TERMINAL_STATES = {"COMPLETE", "CANCELLED", "REJECTED"}

    def __init__(self, api_key, client_code, jwt_token, redis_client=None):
        if not api_key or not client_code:
            raise ValueError("api_key and client_code are required")

        self.api_key = api_key
        self.client_code = client_code
        self.access_token = jwt_token

        # Optional redis client for publishing events (used by connector)
        self.redis = redis_client
        self.channel_prefix = "motilal"

        self.order_api = None
        self.portfolio_api = None

        self.monitored_orders = {}
        self.stop_monitoring = False
        self.monitor_thread = None

        if self.access_token:
            self._initialize_apis()
            self._publish("auth", {"event": "LOGIN_RESTORED"})

    # ------------------ Authentication ------------------
    def _initialize_apis(self):
        self.order_api = MotilalOrderAPI(self.api_key, self.client_code, self.access_token)
        self.portfolio_api = MotilalOswalPortfolioAPI(self.api_key, self.client_code, self.access_token)

    def login(self, jwt_token=None):
        """
        Simply sets/refreshes JWT token and reinitializes APIs.
        The actual token generation is expected to be handled upstream.
        """
        if jwt_token:
            self.access_token = jwt_token

        if not self.access_token:
            raise RuntimeError("JWT token is required to login")

        self._initialize_apis()
        self._publish("auth", {"event": "LOGIN_SUCCESS", "client_code": self.client_code})
        self._start_order_monitor()
        return {"access_token": self.access_token}

    def logout(self):
        self.stop_monitoring = True
        if self.monitor_thread:
            self.monitor_thread.join()

        self.access_token = None
        self.order_api = None
        self.portfolio_api = None
        self._publish("auth", {"event": "LOGOUT", "client_code": self.client_code})

    def is_logged_in(self):
        return bool(self.access_token and self.order_api)

    def _ensure_login(self):
        if not self.is_logged_in():
            raise RuntimeError("User not logged in")

    # ------------------ Monitoring ------------------
    def _start_order_monitor(self):
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.stop_monitoring = False
            self.monitor_thread = threading.Thread(
                target=self._poll_orders,
                daemon=True
            )
            self.monitor_thread.start()

    def _poll_orders(self):
        while not self.stop_monitoring:
            if not self.monitored_orders:
                time.sleep(1)
                continue

            try:
                response = self.order_api.get_orders()
                all_orders = response.get("data", [])
                api_orders_map = {order.get("order_id"): order for order in all_orders}

                for order_id in list(self.monitored_orders.keys()):
                    if order_id not in api_orders_map:
                        continue

                    api_order = api_orders_map[order_id]
                    current_status = api_order.get("status")
                    local_data = self.monitored_orders[order_id]
                    last_status = local_data.get("last_status")

                    if current_status != last_status:
                        self._handle_status_change(
                            order_id,
                            last_status,
                            current_status,
                            api_order
                        )
                        self.monitored_orders[order_id]["last_status"] = current_status

                    if current_status in self.TERMINAL_STATES:
                        self.monitored_orders.pop(order_id, None)

            except Exception as e:
                self._publish("orders", {"event": "MONITORING_ERROR", "error": str(e)})

            time.sleep(1)

    def _handle_status_change(self, order_id, old_status, new_status, order_data):
        event_type = "ORDER_UPDATED"

        if new_status == "OPEN" and old_status == "INITIALIZED":
            event_type = "ORDER_ACCEPTED"
        elif new_status == "COMPLETE":
            event_type = "ORDER_TRADED"
        elif new_status == "CANCELLED":
            event_type = "ORDER_CANCELLED"
        elif new_status == "REJECTED":
            event_type = "ORDER_REJECTED"

        self._publish(
            "orders",
            {
                "event": event_type,
                "order_id": order_id,
                "previous_status": old_status,
                "current_status": new_status,
                "details": order_data
            }
        )

    # ------------------ Orders API ------------------
    def place_order(
        self,
        symbol,
        qty,
        order_type,
        transaction_type="BUY",
        product="MIS",
        exchange="NSE",
        price=0,
        trigger_price=0,
        validity="DAY",
        amo=False
    ):
        self._ensure_login()

        response = self.order_api.place_order(
            symbol=symbol,
            exchange=exchange,
            side=transaction_type,
            quantity=qty,
            amoorder=amo,
            order_type=order_type,
            product_type=product,
            price=price or 0,
            trigger_price=trigger_price or 0,
            validity=validity or "DAY",
        )

        order_id = None
        if isinstance(response, dict):
            order_id = response.get("data", {}).get("order_id") or response.get("order_id")

        if order_id:
            self.monitored_orders[order_id] = {
                "last_status": "INITIALIZED",
                "symbol": symbol,
                "qty": qty,
                "transaction_type": transaction_type
            }

        self._publish(
            "orders",
            {
                "event": "ORDER_PLACED_REQ",
                "request": {
                    "symbol": symbol,
                    "qty": qty,
                    "order_type": order_type,
                    "transaction_type": transaction_type,
                },
                "response": response
            }
        )
        return response

    def get_orders(self):
        self._ensure_login()
        response = self.order_api.get_orders()
        self._publish("orders", {"event": "ORDERS_FETCHED", "response": response})
        return response

    def get_order_details(self, order_id):
        self._ensure_login()
        response = self.order_api.get_order_history(order_id)
        self._publish(
            "orders",
            {"event": "ORDER_HISTORY_FETCHED", "order_id": order_id, "response": response},
        )
        return response

    # ------------------ Portfolio API ------------------
    def get_holdings(self):
        self._ensure_login()
        response = self.portfolio_api.get_holdings()
        self._publish("portfolio", {"event": "HOLDINGS_FETCHED", "response": response})
        return response

    def get_positions(self):
        self._ensure_login()
        response = self.portfolio_api.get_positions()
        self._publish("portfolio", {"event": "POSITIONS_FETCHED", "response": response})
        return response

    # ------------------ Utilities ------------------
    def _publish(self, channel_suffix, payload):
        if not self.redis:
            return
        channel = f"{self.channel_prefix}.{channel_suffix}"
        try:
            self.redis.publish(channel, payload if isinstance(payload, str) else json.dumps(payload))
        except Exception:
            # Keep adapter resilient; connector will log if needed
            pass
