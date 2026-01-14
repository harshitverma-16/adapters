
import json
import logging


from Zerodha.zerodha_mapper import ZerodhaMapper
from Zerodha.api.order import ZerodhaOrderAPI
from Zerodha.api.portfolio import ZerodhaPortfolioAPI
from Zerodha.api.auth import ZerodhaAuthAPI

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class ZerodhaAdapter:
    def __init__(self, entity_id=None, creds=None):
        """
        entity_id: optional, used for mapping
        creds: dict containing keys like api_key, api_secret, access_token, redirect_url
        """
        self.entity_id = entity_id
        self.creds = creds or {}

        self.api_key = self.creds.get("api_key", "")
        self.api_secret = self.creds.get("api_secret", "")
        self.redirect_url = self.creds.get("redirect_url", "http://localhost")
        self.access_token = self.creds.get("access_token")
        self.user_id = self.creds.get("user_id", "")

        self.order_api = ZerodhaOrderAPI(access_token=self.access_token, api_key=self.api_key)
        self.portfolio_api = ZerodhaPortfolioAPI(access_token=self.access_token, api_key=self.api_key)
        self.auth_api = ZerodhaAuthAPI(api_key=self.api_key, api_secret=self.api_secret, redirect_url=self.redirect_url)


        logging.info("Zerodha Adapter initialized successfully.")


        # ID Mapping Init
        self.blitz_to_zerodha = {}
        self.zerodha_to_blitz = {}



    def process_command(self, payload):


        """
        Public method to process a command dictionary directly.
        """


        action = payload.get("action")
        blitz_data = payload.get("data", {})

        logging.info(f"Received: {action}")

        api_response = None
        error_msg = None

        try:
            if action == "PLACE_ORDER":
                params = ZerodhaMapper.to_zerodha(blitz_data)
                api_response = self.order_api.place_order(
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
                    order_id = ZerodhaMapper.extract_order_id(api_response)
                    blitz_id = blitz_data.get("BlitzOrderID")
                    
                    # Store Mapping
                    if blitz_id and order_id:
                        self.blitz_to_zerodha[blitz_id] = str(order_id)
                        self.zerodha_to_blitz[str(order_id)] = blitz_id
                        logging.info(f"Mapped: {blitz_id} -> {order_id}")

                    api_response = {
                        "blitz_order_id": blitz_id,
                        "zerodha_order_id": order_id,
                        "zerodha_response": api_response 
                    }

                except Exception as e:
                    logging.error(f"Failed to process response mapping: {e}")
                
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response)}")


            elif action == "MODIFY_ORDER":
                zerodha_order_id = ZerodhaMapper.resolve_order_id(blitz_data, self.blitz_to_zerodha)
                params = ZerodhaMapper.to_zerodha(blitz_data)
                
                api_response = self.order_api.modify_order(
                    order_id=zerodha_order_id,
                    order_type=params.get("order_type"),
                    qty=params.get("qty"),
                    validity=params.get("validity"),
                    price=params.get("price")
                )

                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response)}")
                
                blitz_response = ZerodhaMapper.to_blitz(api_response, "orders")
                logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                


            elif action == "CANCEL_ORDER":
                 zerodha_order_id = ZerodhaMapper.resolve_order_id(blitz_data, self.blitz_to_zerodha)
                 api_response = self.order_api.cancel_order(zerodha_order_id)
                 logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response)}")
                 
                 blitz_response = ZerodhaMapper.to_blitz(api_response, "orders")
                 if blitz_response:
                    logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                 
            
            

            elif action == "GET_ORDERS":
                api_response = self.order_api.get_orders()
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")
                blitz_response = ZerodhaMapper.to_blitz(api_response, "orders")
                if blitz_response:
                    logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                

            
            
            elif action == "GET_ORDER_DETAILS":
                # Resolve order ID
                zerodha_order_id = ZerodhaMapper.resolve_order_id(blitz_data, self.blitz_to_zerodha)
                api_response = self.order_api.get_order_details(zerodha_order_id)
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")
                
                blitz_response = ZerodhaMapper.to_blitz([api_response], "orders")
                if blitz_response:
                    logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                

            
            elif action == "GET_HOLDINGS":
                api_response = self.portfolio_api.get_holdings()
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")


                blitz_response = ZerodhaMapper.to_blitz(api_response, "holdings")
                if blitz_response:
                    logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                


            elif action == "GET_POSITIONS":
                api_response = self.portfolio_api.get_positions()
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")
                
                blitz_response = ZerodhaMapper.to_blitz(api_response, "positions")
                if blitz_response:
                    logging.info(f"[BLITZ RESPONSE] {json.dumps(blitz_response)}")
                
                
            


            elif action == "GET_LOGIN_URL":
                url = self.auth_api.generate_login_url()
                api_response = {"login_url": url}
                logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")


            elif action == "LOGIN":
                 # Handle LOGIN specially if passed via this channel
                 req_token = blitz_data.get("request_token")
                 if not req_token:
                     raise ValueError("Missing 'request_token'")
                 api_response = self.auth_api.login(req_token)
                 
                 # Re-init WebSocket if login successful (this overrides the old one)
                 self.access_token = self.auth_api.access_token
                 logging.info(f"[ZERODHA RESPONSE] {json.dumps(api_response, default=str)}")
                 
            else:

                logging.warning(f"Action '{action}' not implemented in automated mode")
                
        except Exception as e:
            logging.error(f" !! Error executing {action}: {e}")
            status = "ERROR"
            error_msg = str(e)
            logging.info(f"[ZERODHA RESPONSE] status={status} | error={error_msg}")






if __name__ == "__main__":
    adapter = ZerodhaAdapter()
    adapter.start()
