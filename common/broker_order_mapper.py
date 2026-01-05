import json
import logging
from datetime import datetime
from typing import Dict, Any

class OrderLog:
    """
    Standardized Order Log format for Blitz.
    """
    def __init__(self):
        self.ExchangeOrderId = None
        self.ExecutionId = None
        self.Account = None
        self.InstrumentId = 0
        self.OrderQuantity = 0
        self.OrderPrice = 0.0
        self.OrderSide = ""
        self.OrderType = ""
        self.OrderStatus = ""
        self.InstrumentName = None
        self.LeavesQuantity = 0
        self.CumulativeQuantity = 0
        self.OrderTriggerPrice = 0.0
        self.CancelledQuantity = 0
        self.OrderGeneratedDateTime = 0
        self.ExchangeTransactTime = 0
        self.AverageTradedPrice = 0.0
        #self.UserText = {}  # raw broker JSON for reference

    def to_dict(self):
        """Convert OrderLog to dictionary."""
        return {
            "ExchangeOrderId": self.ExchangeOrderId,
            "ExecutionId": self.ExecutionId,
            "Account": self.Account,
            "InstrumentId": self.InstrumentId,
            "OrderQuantity": self.OrderQuantity,
            "OrderPrice": self.OrderPrice,
            "OrderSide": self.OrderSide,
            "OrderType": self.OrderType,
            "OrderStatus": self.OrderStatus,
            "InstrumentName": self.InstrumentName,
            "LeavesQuantity": self.LeavesQuantity,
            "CumulativeQuantity": self.CumulativeQuantity,
            "OrderTriggerPrice": self.OrderTriggerPrice,
            "CancelledQuantity": getattr(self, "CancelledQuantity", 0),
            "OrderGeneratedDateTime": self.OrderGeneratedDateTime,
            "ExchangeTransactTime": self.ExchangeTransactTime,
            "AverageTradedPrice": self.AverageTradedPrice,
            #"UserText": self.UserText,
        }

    def to_json(self):
        """Convert OrderLog to JSON string."""
        return json.dumps(self.to_dict())


class BrokerOrderMapper:
    """
    Converts broker-specific order events into Blitz OrderLog
    """

    @staticmethod
    def map(broker_name: str, raw_data) -> OrderLog:
        """raw_data should be a dict, not a string"""
        order_log = OrderLog()
        try:
            data = raw_data if isinstance(raw_data, dict) else json.loads(raw_data)

            if broker_name.lower() == "zerodha":
                BrokerOrderMapper._map_zerodha(data, order_log)
            # Add other brokers here if needed
            else:
                raise ValueError(f"Unsupported broker: {broker_name}")

            # UserText removed - attribute is commented out

        except Exception as e:
            logging.error(f"[OrderLog Mapper Error] {e}")

        return order_log

    # ─────────────────────────────
    # ZERODHA
    # ─────────────────────────────
    @staticmethod
    def _map_zerodha(data: dict, o: OrderLog):
        # Handle cases where data might be nested in 'details' or direct
        details = data.get("details", data)

        o.ExchangeOrderId = details.get("exchange_order_id", details.get("order_id"))
        o.ExecutionId = details.get("order_id")
        o.Account = details.get("account_id")

        o.InstrumentName = details.get("tradingsymbol")
        o.InstrumentId = details.get("instrument_token", 0)

        o.OrderQuantity = int(details.get("quantity", 0))
        o.OrderPrice = float(details.get("price", 0.0))
        o.OrderTriggerPrice = float(details.get("trigger_price", 0.0))

        o.CumulativeQuantity = int(details.get("filled_quantity", 0))
        o.LeavesQuantity = int(details.get("pending_quantity", 0))
        o.CancelledQuantity = int(details.get("cancelled_quantity", 0))

        # Use status mapping
        o.OrderSide = details.get("transaction_type", "").upper()  # "BUY" or "SELL"
        o.OrderType = details.get("order_type", "").upper()        # "LIMIT" or "MARKET"
        
        raw_status = details.get("status", "").upper()
        o.OrderStatus = BrokerOrderMapper._map_status(raw_status)

        o.AverageTradedPrice = float(details.get("average_price", 0.0))
        o.OrderGeneratedDateTime = (details.get("order_timestamp"))
        o.ExchangeTransactTime = (details.get("exchange_timestamp"))
        # o.OrderGeneratedDateTime = BrokerOrderMapper._to_epoch(details.get("order_timestamp"))
        # o.ExchangeTransactTime = BrokerOrderMapper._to_epoch(details.get("exchange_timestamp"))

    # ─────────────────────────────
    # HELPERS
    # ─────────────────────────────
    @staticmethod
    def _map_status(status: str) -> str:
        mapping = {
            "OPEN": "NEW",
            "COMPLETE": "FILLED",
            "CANCELLED": "CANCELLED",
            "REJECTED": "REJECTED",
        }
        return mapping.get(status, status)

    @staticmethod
    def _to_epoch(ts) -> int:
        if not ts:
            return 0
        try:
            return int(datetime.fromisoformat(str(ts)).timestamp() * 1000)
        except Exception:
            return 0
