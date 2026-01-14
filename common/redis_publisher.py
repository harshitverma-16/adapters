import redis
import json
import logging


logger = logging.getLogger(__name__)


# Initialize Redis Connection once
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
def publish_message(channel, message_dict):
    """
    Publishes a dictionary as a JSON string to the specified Redis channel.
    """
    try:
        payload = json.dumps(message_dict, default=str)
        r.publish(channel, payload)
        logger.info(f"Published to {channel}: {payload}")
    except Exception as e:
        logger.error(f"Failed to publish to Redis: {e}")