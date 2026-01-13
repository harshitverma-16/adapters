# import os
# import json
# import redis
# import logging

# # ========================= CONFIG =========================
# REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# REDIS_DB = int(os.getenv("REDIS_DB", 0))

# logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# # ========================= REDIS CLIENT =========================
# redis_client = redis.Redis(
#     host=REDIS_HOST,
#     port=REDIS_PORT,
#     db=REDIS_DB,
#     decode_responses=True
# )

# # ========================= LOAD ALL CREDENTIALS =========================
# def load_all_credentials():
#     """
#     Loads all entities from Redis that have a 'brokers' section.
#     Returns: dict mapping entity_id -> entity_data
#     """
#     creds_in_memory = {}
#     for key in redis_client.keys("*"):
#         try:
#             data = redis_client.get(key)
#             if not data:
#                 continue
#             entity = json.loads(data)
#             if "brokers" in entity:
#                 creds_in_memory[key] = entity
#         except Exception as e:
#             logging.error(f"Failed to load key {key}: {e}")
#     return creds_in_memory

# # ========================= CREATE BROKER MAPPING =========================
# def create_broker_mapping(all_creds, broker_name):
#     """
#     Returns a mapping of entity_id -> broker credentials for a specific broker
#     Only includes active brokers
#     """
#     broker_map = {
#         entity_id: entity["brokers"][broker_name]
#         for entity_id, entity in all_creds.items()
#         if broker_name in entity["brokers"] and entity["brokers"][broker_name].get("active")
#     }
#     return broker_map

# # ========================= USAGE EXAMPLE =========================
# if __name__ == "__main__":
#     all_creds = load_all_credentials()
#     if not all_creds:
#         logging.info("No credentials found in Redis.")
#     else:
#         logging.info(f"Loaded {len(all_creds)} entities from Redis.")

#         # Example: get mapping for Zerodha
#         zerodha_map = create_broker_mapping(all_creds, "Zerodha")
#         logging.info(f"Active Zerodha brokers: {len(zerodha_map)}")
#         for entity_id, creds in zerodha_map.items():
#             print(f"\nEntity ID: {entity_id} (Zerodha)")
#             print(json.dumps(creds, indent=2))

#         # Example: get mapping for Upstox
#         upstox_map = create_broker_mapping(all_creds, "Upstox")
#         logging.info(f"Active Upstox brokers: {len(upstox_map)}")
#         for entity_id, creds in upstox_map.items():
#             print(f"\nEntity ID: {entity_id} (Upstox)")
#             print(json.dumps(creds, indent=2))
import os
import sys
import json
import threading
import redis
import logging

# ------------------- Add root folder to sys.path -------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ------------------- Imports -------------------
from brokers.MotilalOswal.mofl_connector import MOFLConnector
from brokers.Zerodha.zerodha_connector import ZerodhaConnector

# ========================= CONFIG =========================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ========================= REDIS CLIENT =========================
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# ========================= BROKER CONNECTOR MAPPING =========================
BROKER_CONNECTOR_MAP = {
    # "Zerodha": ZerodhaConnector,
    "MOFL": MOFLConnector,
    #"Upstox": UpstoxConnector
}

# ========================= LOAD ALL CREDENTIALS =========================
def load_all_credentials():
    """
    Loads all entities from Redis that have a 'brokers' section.
    Returns: dict mapping entity_id -> entity_data
    """
    creds_in_memory = {}
    for key in redis_client.keys("*"):
        try:
            data = redis_client.get(key)
            if not data:
                continue
            entity = json.loads(data)
            if "brokers" in entity:
                creds_in_memory[key] = entity
        except Exception as e:
            logging.error(f"Failed to load key {key}: {e}")
    return creds_in_memory

# ========================= CREATE ACTIVE BROKER MAPPING =========================
def get_active_broker_map(all_creds):
    """
    Returns a mapping of broker_name -> dict of entity_id -> creds
    """
    broker_map = {}
    for entity_id, entity in all_creds.items():
        brokers = entity.get("brokers", {})
        for broker_name, creds in brokers.items():
            if creds.get("active"):
                if broker_name not in broker_map:
                    broker_map[broker_name] = {}
                broker_map[broker_name][entity_id] = creds
    return broker_map

# ========================= START CONNECTORS =========================
def start_all_connectors():
    all_creds = load_all_credentials()
    if not all_creds:
        logging.error("No credentials found in Redis.")
        return

    logging.info(f"Loaded {len(all_creds)} entities from Redis.")

    active_broker_map = get_active_broker_map(all_creds)
    threads = []
    connectors = []

    for broker_name, entities in active_broker_map.items():
        ConnectorClass = BROKER_CONNECTOR_MAP.get(broker_name)
        if not ConnectorClass:
            logging.warning(f"No connector found for broker: {broker_name}")
            continue

        for entity_id, creds in entities.items():
            logging.info(f"Starting {broker_name} connector for {entity_id}")
            connector = ConnectorClass(entity_id, creds)
            connectors.append(connector)
            t = threading.Thread(target=connector.start, daemon=True)
            t.start()
            threads.append(t)

    # Keep main thread alive
    def shutdown_handler(signum, frame):
        logging.info("Shutting down all connectors...")
        for c in connectors:
            try:
                c.stop()
            except Exception:
                pass
        sys.exit(0)

    for t in threads:
        t.join()

# ========================= MAIN =========================
if __name__ == "__main__":
    start_all_connectors()
