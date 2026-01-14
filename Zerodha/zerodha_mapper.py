import logging
from common.broker_order_mapper import BrokerOrderMapper

logger = logging.getLogger(__name__)

class ZerodhaMapper:


    """
    Centralized mapper for converting:
    1. Blitz -> Zerodha (for requests)
    2. Zerodha -> Blitz (for responses/websocket)
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
        Wraps the internal BrokerOrderMapper with the Blitz envelope.
        """


        try:
            mapped_data = BrokerOrderMapper.zerodha_to_blitz(raw_data, request_type)
            
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
