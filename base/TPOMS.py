import os
import sys
import json
import redis
import threading
import logging

# Ensure adapters directory is in path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from Zerodha.zerodha_adapter import ZerodhaAdapter
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='[TPOMS] %(message)s')

class TradeListener:
    def __init__(self):
        self.redis_host = config.REDIS_HOST
        self.redis_port = config.REDIS_PORT
        self.request_channel = config.CH_BLITZ_REQUESTS
        
        logging.info(f"Connecting to Redis at {self.redis_host}:{self.redis_port}...")
        self.redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        
        # Store active connector instances
        # Key: f"{broker_name}:{entity_id}" -> Value: ConnectorInstance
        self.connectors = {}
        
        self.is_running = False

    def stop(self):
        self.is_running = False
        self.pubsub.unsubscribe()
        for connector in self.connectors.values():
            connector.stop()
        logging.info("TPOMS Listener stopped.")

    def get_connector(self, broker, entity_id, creds=None):
        """
        Retrieves an existing connector or creates a new one.
        """
        key = f"{broker}:{entity_id}"
        
        if key in self.connectors:
            return self.connectors[key]
        
        if broker == "Zerodha":
            logging.info(f"Initializing new Zerodha connector for {entity_id}")
            # Initialize Connector (this starts WebSocket if access_token is present)
            connector = ZerodhaAdapter(entity_id=entity_id, creds=creds)
            self.connectors[key] = connector
            return connector
        
        else:
            logging.warning(f"Unknown broker: {broker}")
            return None

    def load_all_credentials(self):
        """
        Loads all entity credentials from Redis on startup.
        Expected Redis Key Format: "ENTITY:<entity_id>" -> JSON
        """
        logging.info("Loading existing credentials from Redis...")
        try:
            keys = self.redis.keys("ENTITY:*")
            for key in keys:
                entity_data = self.redis.get(key)
                if entity_data:
                    payload = json.loads(entity_data)
                    entity_id = key.split(":", 1)[1]
                    
                    # Support legacy "brokers" dict
                    brokers = payload.get("brokers", {})
                    for broker_name, creds in brokers.items():
                        self.get_connector(broker_name, entity_id, creds)
                        logging.info(f"Loaded credentials for {entity_id} -> {broker_name}")

                    # Support new simplified schema
                    if "broker" in payload and "creds" in payload:
                        broker_name = payload["broker"]
                        creds = payload["creds"]
                        self.get_connector(broker_name, entity_id, creds)
                        logging.info(f"Loaded credentials for {entity_id} -> {broker_name}")
        except Exception as e:
            logging.error(f"Failed to load credentials: {e}")

    def save_token_to_redis(self, entity_id, broker, access_token):
        """
        Updates the access_token in Redis for persistence.
        """
        key = f"ENTITY:{entity_id}"
        try:
            data = self.redis.get(key)
            if data:
                payload = json.loads(data)
                if "brokers" in payload and broker in payload["brokers"]:
                    payload["brokers"][broker]["access_token"] = access_token
                    self.redis.set(key, json.dumps(payload))
                    logging.info(f"Persisted new access token for {entity_id} -> {broker}")
                else:
                    logging.warning(f"Cannot save token: Broker {broker} not found in {key}")
            else:
                logging.warning(f"Cannot save token: Entity {key} not found")
        except Exception as e:
            logging.error(f"Failed to save token to Redis: {e}")

    def start(self):
        """
        Main loop: Listen to Redis and route messages.
        """
        # Load existing sessions first
        self.load_all_credentials()

        self.pubsub.subscribe(self.request_channel)
        self.is_running = True
        logging.info(f"TPOMS Listener started on channel: {self.request_channel}")
        
        for message in self.pubsub.listen():
            if not self.is_running:
                break
            
            if message["type"] == "message":
                threading.Thread(target=self._process_message, args=(message["data"],)).start()

    def _process_message(self, raw_data):
        try:
            payload = json.loads(raw_data)
            
            # 1. Basic Validation
            broker = payload.get("broker")
            entity_id = payload.get("entity_id")
            
            if not broker or not entity_id:
                logging.warning(f"Ignored message missing broker or entity_id: {raw_data}")
                return

            # 2. Extract Credentials if provided
            creds = payload.get("credentials") 
            
            # 3. Get Connector
            connector = self.get_connector(broker, entity_id, creds)
            
            if connector:
                # 4. Route Command
                action = payload.get("action")
                connector.process_command(payload)
                
                # 5. Persistence Hook for LOGIN
                if action == "LOGIN":
                    # Check if login was successful by inspecting connector state
                    # Since process_command runs the logic, assuming we can get the new token if it succeeded.
                    # Ideally, process_command should return status, but it's void/async in this architecture.
                    # We can check connector.access_token directly.
                    if connector.access_token:
                         self.save_token_to_redis(entity_id, broker, connector.access_token)

            else:
                logging.error(f"Could not create connector for {broker}:{entity_id}")

        except json.JSONDecodeError:
            logging.error("Failed to decode JSON message")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    listener = TradeListener()
    try:
        listener.start()
    except KeyboardInterrupt:
        listener.stop()
