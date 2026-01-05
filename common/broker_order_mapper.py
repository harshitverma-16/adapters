import json
import logging
from datetime import datetime
from typing import Dict, Any

class OrderLog:
    """
    Standardized Order Log format for Blitz.
    """
    def __init__(self):
        # Existing Identifiers
        self.ExchangeOrderId = None
        self.ExecutionId = None
        self.Account = None
        self.InstrumentId = 0
        self.InstrumentName = None
        
        # Identifiers & Metadata
        self.Id = 0
        self.EntityId = ""
        self.ExchangeSegment = ""
        self.ExchangeInstrumentId = 0
        self.InstrumentType = 0
        self.BlitzOrderId = 0
        self.ClientId = ""
        self.OrderTag = ""
        self.AlgoId = ""
        self.AlgoCategoryId = ""
        self.ClearingFirmId = ""
        self.PANId = ""
        self.StrategyTag = ""
        self.SequenceNumber = 0
        self.CorrelationOrderId = None

        # Order Details
        self.OrderQuantity = 0
        self.OrderPrice = 0.0
        self.OrderSide = ""
        self.OrderType = ""
        self.OrderStatus = ""
        self.OrderStopPrice = 0.0
        self.OrderTriggerPrice = 0.0
        self.OrderDisclosedQuantity = 0
        self.MinimumQuantity = 0
        self.TIF = ""  # e.g. "DAY"
        self.ExecutionType = "" # e.g. "MANUAL"

        # Trade/Execution Details
        self.LeavesQuantity = 0
        self.CumulativeQuantity = 0
        self.LastTradedQuantity = 0
        self.LastTradedPrice = 0.0
        self.AverageTradedPrice = 0.0
        self.AverageTradedValue = 0.0
        self.CancelledQuantity = 0
        self.OrderModificationCount = 0
        self.OrderTradeCount = 0

        # Timing
        self.OrderGeneratedDateTime = 0
        self.LastRequestDateTime = 0
        self.ExchangeTransactTime = 0
        self.OrderExpiryDate = 0

        # Flags & Status
        self.IsFictiveOrder = False
        self.IsOrderCompleted = None
        self.RejectType = "NONE"
        self.RejectTypeReason = ""
        
        # User Data
        self.UserText = ""

    def to_dict(self):
        """Convert OrderLog to dictionary."""
        return {
            "Id": self.Id,
            "EntityId": self.EntityId,
            "InstrumentId": self.InstrumentId,
            "ExchangeSegment": self.ExchangeSegment,
            "ExchangeInstrumentId": self.ExchangeInstrumentId,
            "InstrumentName": self.InstrumentName,
            "InstrumentType": self.InstrumentType,
            "BlitzOrderId": self.BlitzOrderId,
            "ExchangeOrderId": self.ExchangeOrderId,
            "ExecutionId": self.ExecutionId,
            "Account": self.Account,
            "ClientId": self.ClientId,
            "OrderType": self.OrderType,
            "OrderSide": self.OrderSide,
            "OrderStatus": self.OrderStatus,
            "OrderQuantity": self.OrderQuantity,
            "OrderPrice": self.OrderPrice,
            "OrderStopPrice": self.OrderStopPrice,
            "OrderTriggerPrice": self.OrderTriggerPrice,
            "LastTradedQuantity": self.LastTradedQuantity,
            "LastTradedPrice": self.LastTradedPrice,
            "CumulativeQuantity": self.CumulativeQuantity,
            "LeavesQuantity": self.LeavesQuantity,
            "TIF": self.TIF,
            "OrderExpiryDate": self.OrderExpiryDate,
            "OrderDisclosedQuantity": self.OrderDisclosedQuantity,
            "MinimumQuantity": self.MinimumQuantity,
            "OrderGeneratedDateTime": self.OrderGeneratedDateTime,
            "LastRequestDateTime": self.LastRequestDateTime,
            "ExchangeTransactTime": self.ExchangeTransactTime,
            "OrderModificationCount": self.OrderModificationCount,
            "OrderTradeCount": self.OrderTradeCount,
            "AverageTradedPrice": self.AverageTradedPrice,
            "AverageTradedValue": self.AverageTradedValue,
            "IsFictiveOrder": self.IsFictiveOrder,
            "RejectType": self.RejectType,
            "RejectTypeReason": self.RejectTypeReason,
            "OrderTag": self.OrderTag,
            "AlgoId": self.AlgoId,
            "AlgoCategoryId": self.AlgoCategoryId,
            "ClearingFirmId": self.ClearingFirmId,
            "PANId": self.PANId,
            "IsOrderCompleted": self.IsOrderCompleted,
            "UserText": self.UserText,
            "ExecutionType": self.ExecutionType,
            "StrategyTag": self.StrategyTag,
            "SequenceNumber": self.SequenceNumber,
            "CorrelationOrderId": self.CorrelationOrderId,
            
            # Additional internal fields if needed
            "CancelledQuantity": getattr(self, "CancelledQuantity", 0),
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
        o.ExchangeSegment = details.get("exchange")
        #o.Account = details.get("account_id")

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
        o.IsOrderCompleted = BrokerOrderMapper._map_status(raw_status)
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
