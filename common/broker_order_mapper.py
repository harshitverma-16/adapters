import json
import logging
from datetime import datetime
from typing import Dict, Any

class OrderLog:
    """
    Standardized Order Log format for Blitz.
    """
    def __init__(self):
        self.Id = 0
        self.EntityId = ""
        self.InstrumentId = 0
        self.ExchangeSegment = ""
        self.InstrumentName = None
        self.BlitzOrderId = 0
        self.ExchangeOrderId = None
        self.ExecutionId = None
        self.OrderType = ""
        self.OrderSide = ""
        self.OrderStatus = ""
        self.OrderQuantity = 0
        self.OrderPrice = 0.0
        self.OrderStopPrice = 0.0
        self.OrderTriggerPrice = 0.0
        self.LastTradedQuantity = 0
        self.LastTradedPrice = 0.0
        self.LeavesQuantity = 0
        self.TIF = ""
        self.OrderDisclosedQuantity = 0
        self.ExchangeTransactTime = 0
        self.AverageTradedPrice = 0.0
        self.IsOrderCompleted = None
        self.UserText = ""
        self.ExecutionType = ""
        self.CorrelationOrderId = None

    def to_dict(self):
        """Convert OrderLog to dictionary."""
        return {
            "Id": self.Id,
            "EntityId": self.EntityId,
            "InstrumentId": self.InstrumentId,
            "ExchangeSegment": self.ExchangeSegment,
            "InstrumentName": self.InstrumentName,
            "BlitzOrderId": self.BlitzOrderId,
            "ExchangeOrderId": self.ExchangeOrderId,
            "ExecutionId": self.ExecutionId,
            "OrderType": self.OrderType,
            "OrderSide": self.OrderSide,
            "OrderStatus": self.OrderStatus,
            "OrderQuantity": self.OrderQuantity,
            "OrderPrice": self.OrderPrice,
            "OrderStopPrice": self.OrderStopPrice,
            "OrderTriggerPrice": self.OrderTriggerPrice,
            "LastTradedQuantity": self.LastTradedQuantity,
            "LastTradedPrice": self.LastTradedPrice,
            "LeavesQuantity": self.LeavesQuantity,
            "TIF": self.TIF,
            "OrderDisclosedQuantity": self.OrderDisclosedQuantity,
            "ExchangeTransactTime": self.ExchangeTransactTime,
            "AverageTradedPrice": self.AverageTradedPrice,
            "IsOrderCompleted": self.IsOrderCompleted,
            "UserText": self.UserText,
            "ExecutionType": self.ExecutionType,
            "CorrelationOrderId": self.CorrelationOrderId,
        }

    def to_json(self):
        """Convert OrderLog to JSON string."""
        return json.dumps(self.to_dict())


class BrokerOrderMapper:
    """
    Converts broker-specific order events into Blitz OrderLog
    """

    @staticmethod
    def map(broker_name: str, raw_data, blitz_order_id: str = None) -> OrderLog:
        """raw_data should be a dict, not a string"""
        order_log = OrderLog()
        try:
            data = raw_data if isinstance(raw_data, dict) else json.loads(raw_data)

            if broker_name.lower() == "zerodha":
                BrokerOrderMapper._map_zerodha(data, order_log, blitz_order_id)
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
    def _map_zerodha(data: dict, o: OrderLog, blitz_order_id: str = None):
        # Handle cases where data might be nested in 'details' or direct
        details = data.get("details", data)

        o.Id = 0
        o.EntityId = ""
        o.InstrumentId = details.get("instrument_token", 0)
        o.ExchangeSegment = details.get("exchange")
        o.InstrumentName = details.get("tradingsymbol")
        # Use provided Blitz ID if available, otherwise fall back to Zerodha order_id
        o.BlitzOrderId = blitz_order_id #if blitz_order_id else details.get("order_id")
        o.ExchangeOrderId = details.get("exchange_order_id", details.get("order_id"))
        o.ExecutionId = 0
        o.OrderType = details.get("order_type", "").upper()
        o.OrderSide = details.get("transaction_type", "").upper()
        raw_status = details.get("status", "").upper()
        o.OrderStatus = BrokerOrderMapper._map_status(raw_status)
        o.OrderQuantity = int(details.get("quantity", 0))
        o.OrderPrice = float(details.get("price", 0.0))
        o.OrderStopPrice = 0.0
        o.OrderTriggerPrice = float(details.get("trigger_price", 0.0))
        o.LastTradedQuantity = 0
        o.LastTradedPrice = 0.0
        o.LeavesQuantity = int(details.get("pending_quantity", 0))
        o.TIF = details.get("validity", "")
        o.OrderDisclosedQuantity = int(details.get("disclosed_quantity", 0))
        o.ExchangeTransactTime = details.get("exchange_timestamp")
        o.AverageTradedPrice = float(details.get("average_price", 0.0))
        o.AverageTradedPrice = float(details.get("average_price", 0.0))
        o.IsOrderCompleted = o.OrderStatus in ["FILLED", "CANCELLED", "REJECTED"]
        status_msg = details.get("status_message") or details.get("status_message_raw")
        o.UserText = status_msg if status_msg else ""
        o.ExecutionType = ""
        o.CorrelationOrderId = 0



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

    # @staticmethod
    # def _to_epoch(ts) -> int:
    #     if not ts:
    #         return 0
    #     try:
    #         return int(datetime.fromisoformat(str(ts)).timestamp() * 1000)
    #     except Exception:
    #         return 0
