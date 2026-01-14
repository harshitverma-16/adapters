# import requests
# import hashlib

# LOGIN_URL = "https://kite.zerodha.com/connect/login"
# TOKEN_URL = "https://api.kite.trade/session/token"

# class ZerodhaAuthAPI:

#     def __init__(self, api_key, api_secret, redirect_url):
#         self.api_key = api_key
#         self.api_secret = api_secret
#         self.redirect_url = redirect_url

#     def generate_login_url(self):
#         return f"{LOGIN_URL}?v=3&api_key={self.api_key}"

#     def exchange_token(self, request_token):
#         checksum = hashlib.sha256(
#             f"{self.api_key}{request_token}{self.api_secret}".encode()
#         ).hexdigest()

#         payload = {
#             "api_key": self.api_key,
#             "request_token": request_token,
#             "checksum": checksum
#         }

#         res = requests.post(TOKEN_URL, data=payload)
#         res.raise_for_status()
#         return res.json()["data"]["access_token"]

# #!/usr/bin/env python3
# import os
# import json
# import redis
# import requests
# import hashlib
# import logging

# # ========================= CONFIG =========================
# LOGIN_URL = "https://kite.zerodha.com/connect/login"
# TOKEN_URL = "https://api.kite.trade/session/token"

# REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# REDIS_DB = int(os.getenv("REDIS_DB", 0))

# # ========================= LOGGING =========================
# logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# # ========================= REDIS CLIENT =========================
# redis_client = redis.Redis(
#     host=REDIS_HOST,
#     port=REDIS_PORT,
#     db=REDIS_DB,
#     decode_responses=True
# )
# def publish_login_url(entity_id, broker, login_url):
#     """
#     Publish the login URL to Redis channel so the user can login in browser
#     """
#     payload = {
#         "action": "LOGIN_URL",
#         "entity_id": entity_id,
#         "broker": broker,
#         "data": {
#             "login_url": login_url
#         }
#     }
#     redis_client.publish(CHANNEL, json.dumps(payload))
#     logging.info(f"Published login URL for {entity_id} → {broker} on channel '{CHANNEL}'")

# # ========================= HELPERS =========================
# def get_all_entities():
#     """Fetch all ENTITY:* keys"""
#     return [k for k in redis_client.keys("ENTITY:*")]

# def get_broker_credentials(entity_id, broker):
#     """
#     Fetch credentials for a broker under an entity
#     """
#     entity_key = f"ENTITY:{entity_id}"
#     entity_json = redis_client.get(entity_key)
#     if not entity_json:
#         raise ValueError(f"No data found for {entity_key}")

#     entity_data = json.loads(entity_json)
#     brokers = entity_data.get("brokers", {})

#     if broker not in brokers:
#         raise ValueError(f"Broker '{broker}' not registered for entity '{entity_id}'")

#     return brokers[broker]

# def store_access_token(entity_id, broker, access_token):
#     """
#     Store access_token back under ENTITY → BROKER
#     """
#     entity_key = f"ENTITY:{entity_id}"
#     entity_data = json.loads(redis_client.get(entity_key))

#     entity_data["brokers"][broker]["access_token"] = access_token
#     redis_client.set(entity_key, json.dumps(entity_data))

#     logging.info(f"Access token stored for {entity_id} → {broker}")

# # ========================= ZERODHA AUTH =========================
# class ZerodhaAuthAPI:
#     def __init__(self, entity_id):
#         self.entity_id = entity_id
#         self.broker = "Zerodha"

#         creds = get_broker_credentials(entity_id, self.broker)

#         self.api_key = creds["api_key"]
#         self.api_secret = creds["api_secret"]
#         self.redirect_url = creds.get("redirect_url")
#         self.access_token = creds.get("access_token")

#         logging.info(f"[ZerodhaAuthAPI] Initialized for {entity_id}")

#     def generate_login_url(self):
#         return f"{LOGIN_URL}?v=3&api_key={self.api_key}"

#     def exchange_token(self, request_token):
#         checksum = hashlib.sha256(
#             f"{self.api_key}{request_token}{self.api_secret}".encode()
#         ).hexdigest()

#         payload = {
#             "api_key": self.api_key,
#             "request_token": request_token,
#             "checksum": checksum
#         }

#         res = requests.post(TOKEN_URL, data=payload)
#         res.raise_for_status()

#         data = res.json().get("data", {})
#         access_token = data.get("access_token")
#         if not access_token:
#             raise ValueError("access_token missing in Zerodha response")

#         self.access_token = access_token
#         store_access_token(self.entity_id, self.broker, access_token)
#         return access_token

# # ========================= MAIN =========================
# if __name__ == "__main__":
#     logging.info("=== Zerodha Login Runner Started ===")

#     entity_keys = get_all_entities()
#     if not entity_keys:
#         logging.error("No ENTITY keys found in Redis")
#         exit(1)

#     for entity_key in entity_keys:
#         entity_id = entity_key.split("ENTITY:")[1]

#         try:
#             entity_data = json.loads(redis_client.get(entity_key))
#             if "Zerodha" not in entity_data.get("brokers", {}):
#                 continue  # Entity has no Zerodha

#             logging.info(f"Processing entity: {entity_id}")

#             auth = ZerodhaAuthAPI(entity_id)
#             print(f"\nLogin URL for {entity_id}:")
#             print(auth.generate_login_url())

#             request_token = input(f"Paste request_token for {entity_id}: ").strip()
#             if not request_token:
#                 logging.warning("No request_token provided, skipping")
#                 continue

#             token = auth.exchange_token(request_token)
#             print(f"Access token for {entity_id}: {token}")

#         except Exception as e:
#             logging.error(f"{entity_id} failed: {e}")

#     logging.info("=== Login Flow Finished ===")
#!/usr/bin/env python3
"""
Zerodha Multi-Entity Login Runner
---------------------------------
This script does the following:
1. Reads all entities from Redis that have Zerodha credentials.
2. Publishes login URLs to a Redis channel for users to open in a browser.
3. Accepts request tokens via Redis channel.
4. Exchanges request tokens for access tokens and stores them in Redis.

Redis Structure:
ENTITY:<entity_id> -> {"brokers": {"Zerodha": {"api_key": ..., "api_secret": ..., "redirect_url": ..., "access_token": ...}}}

Redis Channel:
- Used to publish login URLs and receive request tokens.
"""

import os
import json
import time
import redis
import hashlib
import requests
import logging

# ========================= CONFIG =========================
LOGIN_URL = "https://kite.zerodha.com/connect/login"
TOKEN_URL = "https://api.kite.trade/session/token"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
CHANNEL = os.getenv("ADAPTER_CHANNEL", "adapter.channel")

# ========================= LOGGING =========================
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ========================= REDIS CLIENT =========================
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# ========================= HELPERS =========================
def get_all_entities():
    """Return all entity_ids stored in Redis"""
    return [k.replace("ENTITY:", "") for k in redis_client.keys("ENTITY:*")]

def get_broker_credentials(entity_id, broker):
    """Fetch broker credentials for a given entity"""
    entity_key = f"ENTITY:{entity_id}"
    data = redis_client.get(entity_key)
    if not data:
        raise ValueError(f"No data found for entity {entity_id}")
    brokers = json.loads(data).get("brokers", {})
    if broker not in brokers:
        raise ValueError(f"Broker '{broker}' not registered for entity {entity_id}")
    return brokers[broker]

def store_access_token(entity_id, broker, access_token):
    """Store access_token in Redis under entity -> broker"""
    entity_key = f"ENTITY:{entity_id}"
    entity_data = json.loads(redis_client.get(entity_key))
    entity_data["brokers"][broker]["access_token"] = access_token
    redis_client.set(entity_key, json.dumps(entity_data))
    logging.info(f"Access token stored for {entity_id} → {broker}")

def publish_login_url(entity_id, broker, login_url):
    """Publish login URL to Redis channel"""
    payload = {
        "action": "LOGIN_URL",
        "entity_id": entity_id,
        "broker": broker,
        "data": {"login_url": login_url}
    }
    redis_client.publish(CHANNEL, json.dumps(payload))
    logging.info(f"Published login URL for {entity_id} → {broker} on channel '{CHANNEL}'")
    logging.info(login_url)

def wait_for_request_token(entity_id, broker="Zerodha", timeout=300):
    """
    Subscribe to the Redis channel and wait for a REQUEST_TOKEN message
    for the given entity and broker.
    """
    pubsub = redis_client.pubsub()
    pubsub.subscribe(CHANNEL)
    logging.info(f"[Listener] Waiting for REQUEST_TOKEN for {entity_id} → {broker} on '{CHANNEL}'")

    start_time = time.time()
    for message in pubsub.listen():
        if message['type'] != 'message':
            continue
        try:
            data = json.loads(message['data'])
        except Exception:
            continue

        # Only process REQUEST_TOKEN messages
        if data.get("action") == "REQUEST_TOKEN":
            msg_entity = data.get("entity_id")
            msg_broker = data.get("broker")
            token = data.get("data", {}).get("request_token")

            if msg_entity == entity_id and msg_broker == broker and token:
                logging.info(f"[Listener] REQUEST_TOKEN received for {entity_id} → {broker}")
                return token

        # Timeout check
        if time.time() - start_time > timeout:
            logging.warning(f"[Listener] Timeout waiting for REQUEST_TOKEN for {entity_id}")
            break

    return None

# ========================= ZERODHA AUTH =========================
class ZerodhaAuthAPI:
    """Zerodha authentication helper"""

    def __init__(self, api_key, api_secret, redirect_url):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_url = redirect_url
        self.access_token = None
        self.entity_id = None # Optional, for context if needed

    def generate_login_url(self):
        """Return the login URL for the user to open in a browser"""
        return f"{LOGIN_URL}?v=3&api_key={self.api_key}"

    def exchange_token(self, request_token):
        """Exchange request_token for access_token"""
        if not request_token:
            raise ValueError("Request token is required for exchange")

        checksum = hashlib.sha256(f"{self.api_key}{request_token}{self.api_secret}".encode()).hexdigest()
        payload = {"api_key": self.api_key, "request_token": request_token, "checksum": checksum}

        res = requests.post(TOKEN_URL, data=payload)
        res.raise_for_status()

        data = res.json().get("data", {})
        access_token = data.get("access_token")
        if not access_token:
            raise ValueError("access_token missing in Zerodha response")

        self.access_token = access_token
        return access_token

# ========================= MAIN RUNNER =========================
def main():
    logging.info("=== Zerodha Login Runner Started ===")

    entities = get_all_entities()
    if not entities:
        logging.error("No ENTITY keys found in Redis")
        return

    for entity_id in entities:
        try:
            entity_data = json.loads(redis_client.get(f"ENTITY:{entity_id}"))
            if "Zerodha" not in entity_data.get("brokers", {}):
                continue  # skip entities without Zerodha

            logging.info(f"Processing entity: {entity_id}")

            brokers = entity_data.get("brokers", {})
            creds = brokers["Zerodha"]
            
            # Pass credentials explicitly
            auth = ZerodhaAuthAPI(creds["api_key"], creds["api_secret"], creds.get("redirect_url"))
            auth.entity_id = entity_id 

            # Publish login URL to Redis channel
            login_url = auth.generate_login_url()
            publish_login_url(entity_id, "Zerodha", login_url)

            # Wait for request token from channel
            request_token = wait_for_request_token(entity_id, "Zerodha")
            if not request_token:
                logging.warning("No request_token received from channel, skipping")
                continue

            # Exchange token and store
            token = auth.exchange_token(request_token)
            store_access_token(entity_id, "Zerodha", token)
            print(f"Access token for {entity_id}: {token}")

        except Exception as e:
            logging.error(f"{entity_id} failed: {e}")

    logging.info("=== Zerodha Login Runner Finished ===")
# ========================= ENTRY POINT =========================
if __name__ == "__main__":
    main()
