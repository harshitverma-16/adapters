
import json
import logging

from Zerodha.zerodha_adapter import ZerodhaAdapter
from Zerodha.zerodha_mapper import ZerodhaMapper

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class ZerodhaConnector:
    def __init__(self, entity_id=None, creds=None):
        """
        entity_id: optional, used for mapping
        creds: dict containing keys like api_key, api_secret, access_token, redirect_url
        """
        self.entity_id = entity_id
        self.creds = creds or {}

        api_key = self.creds.get("api_key", "")
        api_secret = self.creds.get("api_secret", "")
        redirect_url = self.creds.get("redirect_url", "http://localhost")
        self.access_token = self.creds.get("access_token")
        self.user_id = self.creds.get("user_id", "")

        logging.info("Initializing Zerodha Adapter...")
        self.adapter = ZerodhaAdapter(api_key, api_secret, redirect_url, access_token=self.access_token)
        logging.info("Zerodha Connector initialized successfully.")


        # ID Mapping Init
        self.blitz_to_zerodha = {}
        self.zerodha_to_blitz = {}



    def process_command(self, payload):
        """
        Public method to process a command dictionary directly.
        """
        action = payload.get("action")
        blitz_data = payload.get("data", {})

        logging.info(f" -> Received: {action}")

        result = None
        status = "SUCCESS"
        error_msg = None

        try:
            if action == "PLACE_ORDER":
                params = ZerodhaMapper.to_zerodha(blitz_data)
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

                try:
                    # Extract Zerodha order ID using Mapper
                    order_id = ZerodhaMapper.extract_order_id(result)
                    blitz_id = blitz_data.get("BlitzOrderID")
                    
                    # Store Mapping
                    if blitz_id and order_id:
                        self.blitz_to_zerodha[blitz_id] = str(order_id)
                        self.zerodha_to_blitz[str(order_id)] = blitz_id
                        logging.info(f"Mapped: {blitz_id} -> {order_id}")

                    result = {
                        "blitz_order_id": blitz_id,
                        "zerodha_order_id": order_id,
                        "raw_response": result 
                    }

                except Exception as e:
                    logging.error(f"Failed to process response mapping: {e}")



            elif action == "MODIFY_ORDER":
                zerodha_order_id = self._resolve_order_id(blitz_data)
                params = ZerodhaMapper.to_zerodha(blitz_data)
                
                result = self.adapter.modify_order(
                    order_id=zerodha_order_id,
                    order_type=params.get("order_type"),
                    qty=params.get("qty"),
                    validity=params.get("validity"),
                    price=params.get("price")
                )
                
                blitz_response = ZerodhaMapper.to_blitz(result, "orders")
                if blitz_response:
                     logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")

                     


            elif action == "CANCEL_ORDER":
                 zerodha_order_id = self._resolve_order_id(blitz_data)
                 result = self.adapter.cancel_order(zerodha_order_id)
                 
                 blitz_response = ZerodhaMapper.to_blitz(result, "orders")
                 if blitz_response:
                     logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")
            
            
            elif action == "GET_ORDERS":
                result = self.adapter.get_orders()
                logging.info(f"[RAW ZERODHA RESPONSE] {json.dumps(result, default=str)}")
                blitz_response = ZerodhaMapper.to_blitz(result, "orders")
                if blitz_response:
                    logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")

            
            
            elif action == "GET_ORDER_DETAILS":
                # Resolve order ID
                zerodha_order_id = self._resolve_order_id(blitz_data)
                result = self.adapter.get_order_details(zerodha_order_id)
                
                blitz_response = ZerodhaMapper.to_blitz([result], "orders")
                if blitz_response:
                    logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")

            
            
            elif action == "GET_HOLDINGS":
                result = self.adapter.get_holdings()
                blitz_response = ZerodhaMapper.to_blitz(result, "holdings")
                if blitz_response:
                    logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")



            elif action == "GET_POSITIONS":
                result = self.adapter.get_positions()
                blitz_response = ZerodhaMapper.to_blitz(result, "positions")
                if blitz_response:
                    logging.info(f"[BLITZ STANDARD RESPONSE] {json.dumps(blitz_response)}")
            



            elif action == "GET_LOGIN_URL":
                url = self.adapter.auth_api.generate_login_url()
                result = {"login_url": url}



            elif action == "LOGIN":
                 # Handle LOGIN specially if passed via this channel
                 req_token = blitz_data.get("request_token")
                 if not req_token:
                     raise ValueError("Missing 'request_token'")
                 result = self.adapter.login(req_token)
                 
                 # Re-init WebSocket if login successful (this overrides the old one)
                 self.access_token = self.adapter.access_token
                 
            else:

                logging.warning(f"Action '{action}' not implemented in automated mode")
                
        except Exception as e:
            logging.error(f" !! Error executing {action}: {e}")
            status = "ERROR"
            error_msg = str(e)
            
        self._send_response_to_blitz(status, result, error_msg)





    def _resolve_order_id(self, data):
        """Helper to resolve order ID from either blitz_order_id or order_id."""
        # Check for Blitz order ID first (BlitzOrderID or blitz_order_id)
        blitz_order_id = data.get("BlitzOrderID") or data.get("blitz_order_id")
        if blitz_order_id:
            zerodha_order_id = self.blitz_to_zerodha.get(blitz_order_id)
            if not zerodha_order_id:
                pass 
            else:
               logging.info(f"Resolved Blitz ID {blitz_order_id} -> Zerodha ID {zerodha_order_id}")
               return zerodha_order_id
        
        # Fall back to direct Zerodha order ID
        order_id = data.get("order_id")
        if order_id:
            return order_id
        
        # If we failed to map blitz id and no order_id
        if blitz_order_id:
             raise ValueError(f"Blitz order ID '{blitz_order_id}' not found in mapping")

        raise ValueError("Either 'BlitzOrderID' or 'order_id' must be provided")

    def _send_response_to_blitz(self, status, data, error):
        response = {
            "broker": "Zerodha",
            "status": status,
            "data": data,
            "error": error
        }

        #  Print to terminal
        logging.info(
            "[RAW ZERODHA RESPONSE] status=%s | error=%s | data=%s",
            status,
            error,
            data
        )

if __name__ == "__main__":
    connector = ZerodhaConnector()
    connector.start()
