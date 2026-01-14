import requests
import logging

logger = logging.getLogger(__name__)

class ZerodhaPortfolioAPI:

    BASE_URL = "https://api.kite.trade"
    
    def __init__(self, access_token, api_key):
        self.access_token = access_token
        self.api_key = api_key

    
    
    # Get Holdings
    def get_holdings(self):

        url = f"{self.BASE_URL}/portfolio/holdings"

        # Get Holdings Request Header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

        # Get Holdings Request
        response = requests.get(url, headers= headers)
        holdings_response = response.json()

        logger.info(f"Response form get holdings API: {holdings_response}")

        # Check holdings retrieval status
        if holdings_response.get("status") == "success":
            logger.info("Holdings retrieved successfully")

        else:
            logger.warning(f"Holdings retrieval failed: {holdings_response}")
        
        return holdings_response
    
    
    
    # Get Positions
    def get_positions(self):
        url = f"{self.BASE_URL}/portfolio/positions"

        # Get Positions Request Header
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

        # Get Positions Request
        response = requests.get(url, headers= headers)
        positions_response = response.json()

        logger.info(f"Response form get positions API: {positions_response}")

        # Check positions retrieval status
        if positions_response.get("status") == "success":
            logger.info("Positions retrieved successfully")

        else:
            logger.warning(f"Positions retrieval failed: {positions_response}")
        
        return positions_response