import requests
import hashlib

LOGIN_URL = "https://kite.zerodha.com/connect/login"
TOKEN_URL = "https://api.kite.trade"

class ZerodhaAuthAPI:

    def __init__(self, api_key, api_secret, redirect_url):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_url = redirect_url

    # Generate Login URL
    def generate_login_url(self):
        return f"{LOGIN_URL}?v=3&api_key={self.api_key}"

    # Exchange Request Token for Access Token
    def exchange_token(self, request_token):
        url = f"{TOKEN_URL}/session/token"
        checksum = hashlib.sha256(
            f"{self.api_key}{request_token}{self.api_secret}".encode()
        ).hexdigest()

        payload = {
            "api_key": self.api_key,
            "request_token": request_token,
            "checksum": checksum
        }

        res = requests.post(url, data=payload)
        res.raise_for_status()
        return res.json()["data"]["access_token"]

    # get user profile
    def get_profile(self):
        url = f"{TOKEN_URL}/user/profile"

        payload = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }
        res = requests.get(url, headers=payload)
        res.raise_for_status()
        return res.json()

    # get margin
    def get_margin(self):
        url = f"{TOKEN_URL}/user/margins"
        payload = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }
        res = requests.get(url, headers=payload)
        res.raise_for_status()
        return res.json()

    #logout
    def logout(self):
        url = f"{TOKEN_URL}/session/token?api_key={self.api_key}&access_token={self.access_token}"
        payload = {
            "X-Kite-Version": "3",
        }

        res = requests.delete(url, headers=payload)
        res.raise_for_status()
        return res.json()

