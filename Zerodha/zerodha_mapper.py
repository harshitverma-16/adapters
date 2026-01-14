import logging
from common.broker_order_mapper import OrderLog

logger = logging.getLogger(__name__)

class ZerodhaMapper:


    """
    Centralized mapper for converting:
    1. Blitz -> Zerodha (for requests)
    2. Zerodha -> Blitz (for websocket)
    """



    @staticmethod
    def to_zerodha(data):


        """
        Maps Blitz order parameters to Zerodha format.
        """


        payload = {
            "symbol": data.get("InstrumentName"),
            "exchange": data.get("ExchangeSegment")[:3] if data.get("ExchangeSegment") else "NSE", # truncated e.g. NSECM -> NSE
            "transaction_type": data.get("orderSide"),
            "order_type": data.get("orderType"),
            "qty": int(data.get("quantity", 0)),
            "product": data.get("product"),
            "price": data.get("price"),
            "trigger_price": data.get("stopPrice"),
            "validity": data.get("tif")
        }
        return payload



    @staticmethod
    def to_blitz(raw_data, request_type):
        """
        Maps Zerodha response to Blitz standard format.
        """
        try:
            mapped_data = []
            data = raw_data

            # Unwrap 'data' if present
            if isinstance(data, dict) and "data" in data:
                data = data["data"]

            if request_type == "orders":
                # Handles both list of orders and list of order history (details)
                if isinstance(data, list):
                    for item in data:
                        log_obj = OrderLog()
                        ZerodhaMapper._map_order(item, log_obj)
                        mapped_data.append(log_obj.to_dict())

            elif request_type == "positions":
                # Expecting {'net': [...], 'day': [...]}
                if isinstance(data, dict):
                    net_positions = []
                    day_positions = []
                    
                    # Process Net Positions
                    for p in data.get("net", []):
                        net_positions.append(ZerodhaMapper._map_position(p))
                    
                    # Process Day Positions
                    for p in data.get("day", []):
                        day_positions.append(ZerodhaMapper._map_position(p))

                    mapped_data = {"Net": net_positions, "Day": day_positions}
                else:
                    mapped_data = {"Net": [], "Day": []}

            elif request_type == "holdings":
                # Expecting list of holdings
                if isinstance(data, list):
                    for h in data:
                        mapped_data.append(ZerodhaMapper._map_holding(h))
            
            else:
                 mapped_data = data

            
            blitz_response = {
                "message_type": f"{request_type.upper()}_UPDATE",
                "broker": "Zerodha",
                "data": mapped_data
            }
            return blitz_response
        except Exception as e:
            logger.error(f"Failed to standardize {request_type}: {e}")
            return None

    @staticmethod
    def _map_order(data, o):
        # Handle cases where data might be nested in 'details' or direct
        details = data.get("details", data)

        o.Id = 0
        o.EntityId = ""
        o.InstrumentId = details.get("instrument_token", 0)
        o.ExchangeSegment = details.get("exchange")
        o.InstrumentName = details.get("tradingsymbol")
        o.BlitzOrderId = None # Mapped later if needed
        o.ExchangeOrderId = details.get("exchange_order_id", details.get("order_id"))
        o.ExecutionId = 0
        o.OrderType = details.get("order_type", "").upper()
        o.OrderSide = details.get("transaction_type", "").upper()
        raw_status = details.get("status", "").upper()
        o.OrderStatus = ZerodhaMapper._map_status(raw_status)
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
        o.IsOrderCompleted = o.OrderStatus in ["FILLED", "CANCELLED", "REJECTED"]
        status_msg = details.get("status_message") or details.get("status_message_raw")
        o.UserText = status_msg if status_msg else ""
        o.ExecutionType = ""
        o.CorrelationOrderId = 0

    @staticmethod
    def _map_position(p):
        return {
            "TradingSymbol": p.get("tradingsymbol"),
            "Exchange": p.get("exchange"),
            "InstrumentToken": p.get("instrument_token"),
            "Product": p.get("product"),
            "Quantity": p.get("quantity"),
            "OvernightQuantity": p.get("overnight_quantity"),
            "Multiplier": p.get("multiplier"),
            "AveragePrice": p.get("average_price"),
            "LastPrice": p.get("last_price"),
            "Value": p.get("value"),
            "PnL": p.get("pnl"),
            "M2M": p.get("m2m"),
            "Unrealised": p.get("unrealised"),
            "Realised": p.get("realised"),
            "BuyQuantity": p.get("buy_quantity"),
            "BuyPrice": p.get("buy_price"),
            "BuyValue": p.get("buy_value"),
            "SellQuantity": p.get("sell_quantity"),
            "SellPrice": p.get("sell_price"),
            "SellValue": p.get("sell_value"),
            "DayBuyQuantity": p.get("day_buy_quantity"),
            "DayBuyPrice": p.get("day_buy_price"),
            "DayBuyValue": p.get("day_buy_value"),
            "DaySellQuantity": p.get("day_sell_quantity"),
            "DaySellPrice": p.get("day_sell_price"),
            "DaySellValue": p.get("day_sell_value")
        }

    @staticmethod
    def _map_holding(h):
        return {
            "TradingSymbol": h.get("tradingsymbol"),
            "Exchange": h.get("exchange"),
            "InstrumentToken": h.get("instrument_token"),
            "ISIN": h.get("isin"),
            "Product": h.get("product"),
            "Price": h.get("price"),
            "Quantity": h.get("quantity"),
            "T1Quantity": h.get("t1_quantity"),
            "RealisedQuantity": h.get("realised_quantity"),
            "CollateralQuantity": h.get("collateral_quantity"),
            "CollateralType": h.get("collateral_type"),
            "AveragePrice": h.get("average_price"),
            "LastPrice": h.get("last_price"),
            "PnL": h.get("pnl"),
            "DayChange": h.get("day_change"),
            "DayChangePercentage": h.get("day_change_percentage")
        }

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
    def extract_order_id(result):


        """
        Robustly extracts Zerodha order_id from various response formats.
        """

        
        if isinstance(result, dict):
            # Check if order_id is nested in 'data' field
            if "data" in result and isinstance(result["data"], dict):
                return result["data"].get("order_id")
            else:
                return result.get("order_id")
        else:
            return result

    @staticmethod
    def resolve_order_id(data, id_mapping):
        """
        Resolves order ID from BlitzOrderID using the provided mapping.
        BlitzOrderID is mandatory.
        """
        blitz_order_id = data.get("BlitzOrderID")
        
        if not blitz_order_id:
             raise ValueError("Missing mandatory field: 'BlitzOrderID'")

        zerodha_order_id = id_mapping.get(blitz_order_id)
        if zerodha_order_id:
            logging.info(f"Resolved Blitz ID {blitz_order_id} -> Zerodha ID {zerodha_order_id}")
            return zerodha_order_id
        
        raise ValueError(f"Blitz order ID '{blitz_order_id}' not found in mapping")
