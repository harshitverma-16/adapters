import requests
import logging


logger = logging.getLogger(__name__)


class ZerodhaOrderAPI:
    BASE_URL = "https://api.kite.trade"

    def __init__(self, access_token, api_key):
        self.access_token = access_token
        self.api_key = api_key



    # Place Order
    def place_order(self, symbol, qty, order_type, transaction_type, product, exchange, validity, price, trigger_price):
        url = f"{self.BASE_URL}/orders/regular"

        
        # Place order request header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

        # Place order request payload
        payload = {
            "tradingsymbol": symbol,
            "exchange": exchange,
            "transaction_type": transaction_type,
            "order_type": order_type,
            "quantity": qty,
            "product": product,
            "price" : price,
            "trigger_price": trigger_price,
            "validity": validity
        }

        # Place order request
        response = requests.post(url, headers=headers, data=payload)
        place_order_response = response.json()

        logger.info(f"Response form place order API: {place_order_response}")

        # Check order placement status
        if place_order_response.get("status") == "success":
            logger.info("Order placed successfully")
        else:
            logger.warning(f"Order placement failed: {place_order_response}")
            # send it to order log history


        return place_order_response
    
    

    # Modify Order
    def modify_order(self, order_id, order_type, qty, validity, price=None):

        url = f"{self.BASE_URL}/orders/regular/{order_id}"


        # Modify order request header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }


        # Modify order request payload
        payload = {
            "order_type": order_type,
            "quantity": qty,
            "validity": validity,
            "price": price
        }


        # Modify Order Request
        response = requests.put(url, headers=headers, data=payload)
        modify_order_response = response.json()

        logger.info(f"Response form modify order API: {modify_order_response}")

        # Check order modification status
        if modify_order_response.get("status") == "success":
            logger.info("Order modified successfully")

        else:
            logger.warning(f"Order modification failed: {modify_order_response}")
        

        return modify_order_response
    

   
    # Cancel Order
    def cancel_order(self, order_id):

        url = f"{self.BASE_URL}/orders/regular/{order_id}"

        # Cancel Order Request Header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

        response = requests.delete(url, headers=headers)
        cancel_order_response = response.json()

        logger.info(f"Response form cancel order API: {cancel_order_response}")

        # Check order cancellation status
        if cancel_order_response.get("status") == "success":
            logger.info("Order cancelled successfully")

        else:
            logger.warning(f"Order cancellation failed: {cancel_order_response}")
        
        
        return cancel_order_response
    

    # Retrieve Orders
    def get_orders(self):
        
        url = f"{self.BASE_URL}/orders"

        # Get Orders Request Header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }


        # Get Orders Request
        response = requests.get(url, headers=headers)
        orders_response = response.json()
        logger.info(f"Response form get orders API: {orders_response}")

        # Check order retrieval status
        if orders_response.get("status") == "success":
            logger.info("Orders retrieved successfully")

        else:
            logger.warning(f"Order retrieval failed: {orders_response}")
        
        return orders_response
    


    # Retrieve Order by ID
    # Used for reteriving an order history using Order ID

    def get_order_by_id(self, order_id):

        url = f"{self.BASE_URL}/orders/{order_id}"

        # Get Order by ID Request Header
        headers = {
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

        response = requests.get(url, headers=headers)
        order_by_id_response = response.json()
        logger.info(f"Response form get order by ID API: {order_by_id_response}")

        # Check order retrieval status
        if order_by_id_response.get("status") == "success":
            logger.info("Order retrieved successfully")

        else:
            logger.warning(f"Order retrieval failed: {order_by_id_response}")
        
        return order_by_id_response
