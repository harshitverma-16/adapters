import json
import logging
from datetime import datetime
from typing import Dict, Any

class OrderLog:
    """
    Standardized Order Log format for Blitz.
    """
    def __init__(self):
        # Core Identifiers
        self.Id = 0
        self.EntityId = ""
        self.InstrumentId = 0
        self.ExchangeSegment = ""
        self.ExchangeInstrumentId = 0
        self.InstrumentName = None
        self.InstrumentType = 0
        self.BlitzOrderId = 0
        self.ExchangeOrderId = None
        self.ExecutionId = None
        self.Account = None
        self.ClientId = ""

        # Order Basics
        self.OrderType = ""
        self.OrderSide = ""
        self.OrderStatus = ""
        self.OrderQuantity = 0
        self.OrderPrice = 0.0
        self.OrderStopPrice = 0.0
        self.OrderTriggerPrice = 0.0

        # Trade / Execution
        self.LastTradedQuantity = 0
        self.LastTradedPrice = 0.0
        self.CumulativeQuantity = 0
        self.LeavesQuantity = 0

        # Time-in-Force & Expiry
        self.TIF = ""
        self.OrderExpiryDate = 0

        # Quantity Constraints
        self.OrderDisclosedQuantity = 0
        self.MinimumQuantity = 0

        # Timing
        self.OrderGeneratedDateTime = 0
        self.LastRequestDateTime = 0
        self.ExchangeTransactTime = 0

        # Counters
        self.OrderModificationCount = 0
        self.OrderTradeCount = 0

        # Averages
        self.AverageTradedPrice = 0.0
        self.AverageTradedValue = 0.0

        # Flags & Rejection
        self.IsFictiveOrder = False
        self.RejectType = "NONE"
        self.RejectTypeReason = ""

        # Tags & Algo
        self.OrderTag = ""
        self.AlgoId = ""
        self.AlgoCategoryId = ""
        self.ClearingFirmId = ""
        self.PANId = ""

        # Completion & User Data
        self.IsOrderCompleted = None
        self.UserText = ""
        self.ExecutionType = ""
        self.StrategyTag = ""

        # Sequencing
        self.SequenceNumber = 0
        self.CorrelationOrderId = None

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

        # Core Identifiers
        o.Id = 0                          # not provided
        o.EntityId = ""                   # not provided
        o.InstrumentId = details.get("instrument_token", 0)
        o.ExchangeSegment = details.get("exchange")
        o.ExchangeInstrumentId = 0        # not provided
        o.InstrumentName = details.get("tradingsymbol")
        o.InstrumentType = 0              # not provided
        o.BlitzOrderId = 0                # not provided
        o.ExchangeOrderId = details.get("exchange_order_id", details.get("order_id"))
        o.ExecutionId = details.get("order_id")
        o.Account = None                  # not provided
        o.ClientId = ""                   # not provided

        # Order Basics
        o.OrderType = details.get("order_type", "").upper()
        o.OrderSide = details.get("transaction_type", "").upper()
        raw_status = details.get("status", "").upper()
        o.OrderStatus = BrokerOrderMapper._map_status(raw_status)
        o.OrderQuantity = int(details.get("quantity", 0))
        o.OrderPrice = float(details.get("price", 0.0))
        o.OrderStopPrice = 0.0            # not provided
        o.OrderTriggerPrice = float(details.get("trigger_price", 0.0))

        # Trade / Execution
        o.LastTradedQuantity = 0          # not provided
        o.LastTradedPrice = 0.0           # not provided
        o.CumulativeQuantity = int(details.get("filled_quantity", 0))
        o.LeavesQuantity = int(details.get("pending_quantity", 0))

        # Time-in-Force & Expiry
        o.TIF = ""                        # not provided
        o.OrderExpiryDate = 0             # not provided

        # Quantity Constraints
        o.OrderDisclosedQuantity = 0      # not provided
        o.MinimumQuantity = 0             # not provided

        # Timing
        o.OrderGeneratedDateTime = details.get("order_timestamp")
        o.LastRequestDateTime = 0         # not provided
        o.ExchangeTransactTime = details.get("exchange_timestamp")

        # Counters
        o.OrderModificationCount = 0      # not provided
        o.OrderTradeCount = 0             # not provided

        # Averages
        o.AverageTradedPrice = float(details.get("average_price", 0.0))
        o.AverageTradedValue = 0.0        # not provided

        # Flags & Rejection
        o.IsFictiveOrder = False          # not provided
        o.RejectType = "NONE"             # not provided
        o.RejectTypeReason = ""           # not provided

        # Tags & Algo
        o.OrderTag = ""                   # not provided
        o.AlgoId = ""                     # not provided
        o.AlgoCategoryId = ""             # not provided
        o.ClearingFirmId = ""             # not provided
        o.PANId = ""                      # not provided

        # Completion & User Data
        o.IsOrderCompleted = BrokerOrderMapper._map_status(raw_status)
        o.UserText = ""                   # not provided
        o.ExecutionType = ""              # not provided
        o.StrategyTag = ""                # not provided

        # Sequencing
        o.SequenceNumber = 0              # not provided
        o.CorrelationOrderId = None       # not provided


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
