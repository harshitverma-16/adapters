import os
import json
import redis
import hashlib
import requests
import logging
import sys

# ========================= CONFIG =========================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
CHANNEL = "blitz.requests" # Updated to match TPOMS config

ZERODHA_LOGIN_URL = "https://kite.zerodha.com/connect/login"
ZERODHA_TOKEN_URL = "https://api.kite.trade/session/token"

BROKER = "Zerodha"

# ========================= LOGGING =========================
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ========================= REDIS =========================
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# ========================= DEFAULT ENTITY =========================
# NOTE: TPOMS expects keys prefixed with "ENTITY:"
DEFAULT_ENTITY_ID = "ENTITY:Harshit" 
RAW_ENTITY_ID = "Harshit" # For display/logic

DEFAULT_API_KEY = "2i4ayyawcrptt24h"
DEFAULT_API_SECRET = "2lxel09zt42jim5veokpgg6slrih2fpa"
USER_ID = "RGZ539"
DEFAULT_REDIRECT_URL = "http://localhost:8080/"

def create_default_entity():
    """Create default entity if it doesn't exist"""
    if not redis_client.exists(DEFAULT_ENTITY_ID):
        creds_data = {
            "user_id": USER_ID,
            "api_key": DEFAULT_API_KEY,
            "api_secret": DEFAULT_API_SECRET,
            "redirect_url": DEFAULT_REDIRECT_URL,
            "access_token": "",
            "active": False
        }
        # TPOMS EXPECTS FORMAT: { "brokers": { "Zerodha": { ... } } }
        entity_data = {
            "broker": BROKER,
            "creds": creds_data
        }
        redis_client.set(DEFAULT_ENTITY_ID, json.dumps(entity_data))
        logging.info(f"Created default entity {DEFAULT_ENTITY_ID} in Redis")
    else:
        logging.info(f"Default entity {DEFAULT_ENTITY_ID} already exists in Redis")

# ========================= HELPERS =========================
def get_all_entities():
    """Get all entity keys starting with ENTITY: from Redis"""
    return redis_client.keys("ENTITY:*")

def get_creds_from_redis(redis_key):
    """Get credentials from Redis"""
    if not redis_client.exists(redis_key):
        return None
    entity_data = json.loads(redis_client.get(redis_key))
    
    # Navigate: -> brokers -> Zerodha
    return entity_data.get("brokers", {}).get(BROKER)

def update_access_token(redis_key, new_access_token):
    """Update access token inside the same entity if different"""
    if not redis_client.exists(redis_key):
        logging.error(f"Entity {redis_key} not found in Redis")
        return False
    
    entity_data = json.loads(redis_client.get(redis_key))
    brokers = entity_data.get("brokers", {})
    
    if BROKER not in brokers:
        logging.error(f"Broker {BROKER} not found for entity {redis_key}")
        return False
    
    creds = brokers[BROKER]
    old_token = creds.get("access_token", "")
    
    if old_token != new_access_token:
        creds["access_token"] = new_access_token
        creds["active"] = True
        
        # Save back
        brokers[BROKER] = creds
        entity_data["brokers"] = brokers
        
        redis_client.set(redis_key, json.dumps(entity_data))
        logging.info(f"Updated access token for {redis_key}")
        return True
    else:
        logging.info(f"Access token unchanged for {redis_key}")
        return False

def exchange_request_token(api_key, api_secret, request_token):
    checksum = hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()
    payload = {"api_key": api_key, "request_token": request_token, "checksum": checksum}
    try:
        res = requests.post(ZERODHA_TOKEN_URL, data=payload)
        res.raise_for_status()
        data = res.json().get("data", {})
        return data.get("access_token")
    except Exception as e:
        logging.error(f"Token exchange failed: {e}")
        if res:
             logging.error(f"Response: {res.text}")
        return None

# ========================= MAIN =========================
def main():
    logging.info("=== Zerodha Login Runner Started ===")

    create_default_entity()

    entity_keys = get_all_entities()
    if not entity_keys:
        logging.error("No entities found in Redis")
        return

    for redis_key in entity_keys:
        try:
            creds = get_creds_from_redis(redis_key)
            if not creds:
                continue
            
            api_key = creds.get("api_key")
            api_secret = creds.get("api_secret")
            
            entity_display_name = redis_key.replace("ENTITY:", "")

            print(f"\n--- Processing {entity_display_name} ---")

            # Step 1: Generate login URL
            if not api_key:
                print("Skipping: Missing API Key")
                continue

            login_url = f"{ZERODHA_LOGIN_URL}?v=3&api_key={api_key}"
            print(f"Open this URL in browser and login:\n{login_url}")

            # Step 2: User inputs request token
            request_token = input(f"Paste the request token for {entity_display_name} (or press Enter to skip): ").strip()
            if not request_token:
                logging.warning("No request token entered, skipping")
                continue

            # Step 3: Exchange request token for access token
            access_token = exchange_request_token(api_key, api_secret, request_token)
            if not access_token:
                logging.error("Failed to get access token")
                continue

            # Step 4: Update access token inside the same entity
            updated = update_access_token(redis_key, access_token)

            # Step 5: Notify TPOMS (Optional, but good practice to let it know to reload)
            # TPOMS.py loads credentials on startup. To hot-reload, we might need to restart it 
            # or send a special 'RELOAD' command if supported. 
            # Currently TPOMS loads on startup, so restart is recommended or we can trust 
            # that future get_connector calls *might* re-read if we optimized TPOMS, 
            # but currently TPOMS caches connectors.
            
            print(f"✅ Zerodha login complete for {entity_display_name}.")
            print("NOTE: You may need to restart TPOMS.py to pick up the new token if it's already running.")

        except Exception as e:
            logging.error(f"{redis_key} failed: {e}")

    logging.info("=== Zerodha Login Runner Finished ===")

if __name__ == "__main__":
    main()
