from Zerodha.api.auth import ZerodhaAuthAPI
from Zerodha.api.order import ZerodhaOrderAPI
from Zerodha.api.portfolio import ZerodhaPortfolioAPI

class ZerodhaAdapter:
    def __init__(self, api_key, api_secret, redirect_url):
        
        # Setup the adapter with API keys.
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_url = redirect_url

        # 1. Prepare the Zerodha API helpers
        self.auth_api = ZerodhaAuthAPI(self.api_key, self.api_secret, self.redirect_url)
        self.order_api = None     # Will be created after login
        self.portfolio_api = None # Will be created after login
        self.access_token = None

    # ------------------ API Actions ------------------

    def login(self, request_token):
        """
        Logs in and creates the Order/Portfolio API objects.
        """
        self.access_token = self.auth_api.exchange_token(request_token)

        # Now we can create these because we have the access token
        self.order_api = ZerodhaOrderAPI(self.access_token, self.api_key)
        self.portfolio_api = ZerodhaPortfolioAPI(self.access_token, self.api_key)
        
        return {"access_token": self.access_token}

    def logout(self):
        self.access_token = None
        self.order_api = None
        self.portfolio_api = None

    def _check_login(self):
        """Helper to check if we are logged in before doing actions."""
        if not self.access_token:
            raise RuntimeError("Not logged in! LOGIN first.")

    # --- Wrapper Functions (Just call the API and return the result) ---

    def place_order(self, symbol, qty, order_type, transaction_type, product, exchange, validity, price, trigger_price):
        self._check_login()
        return self.order_api.place_order(symbol, qty, order_type, transaction_type, product, exchange, validity, price, trigger_price)

    def modify_order(self, order_id, order_type, qty, validity, price):
        self._check_login()
        return self.order_api.modify_order(order_id, order_type, qty, validity, price)

    def cancel_order(self, order_id):
        self._check_login()
        return self.order_api.cancel_order(order_id)

    def get_orders(self):
        self._check_login()
        return self.order_api.get_orders()

    def get_order_details(self, order_id):
        self._check_login()
        return self.order_api.get_order_by_id(order_id)

    def get_holdings(self):
        self._check_login()
        return self.portfolio_api.get_holdings()

    def get_positions(self):
        self._check_login()
        return self.portfolio_api.get_positions()