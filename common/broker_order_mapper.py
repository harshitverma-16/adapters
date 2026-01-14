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



