# import redis 
# import threading
# from Zerodha.zerodha_adapter import ZerodhaAdapter

# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
# CH_REQUESTS = "blitz.requests"

# class BaseAdapter:
#     def __init__(self, broker_name, config: dict):
#         self.broker_name = broker_name
#         self.config = config

#         # Initialize Redis Connection
#         try:
#             self.redis_client = redis.Redis(
#                 host=REDIS_HOST, 
#                 port=REDIS_PORT, 
#                 decode_responses=True
#             )
#             self.pubsub = self.redis_client.pubsub()
#         except Exception as e:
#             print(f"[System] Redis Connection Error: {e}")
#             raise e

#         # Initialize the specific Broker Adapter
#         self.broker = self.get_broker()

#     def get_broker(self):
#         if self.broker_name == "Zerodha":
#             return ZerodhaAdapter(self.config)
#         else:
#             raise ValueError(f"Unknown Broker: {self.broker_name}")

#     def start(self):
#         """Starts the listening process in a separate thread."""
#         thread = threading.Thread(target=self._listen_to_redis, daemon=True)
#         thread.start()
#         print(f"[{self.broker_name}] Listening on channel: {CH_REQUESTS}")
#         return thread

#     def _listen_to_redis(self):
#         """Blocking loop that waits for Redis messages and forwards them."""
#         self.pubsub.subscribe(CH_REQUESTS)

#         for message in self.pubsub.listen():
#             if message['type'] == 'message':
#                 # Pass the raw data and the redis client (to send responses) to the adapter
#                 self.broker.process_message(message['data'], self.redis_client)
        
        

