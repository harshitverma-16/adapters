"""
Simple example showing Blitz Order ID usage
The connector now uses a simple dictionary to map Blitz IDs to Zerodha IDs
"""

import os
import json
import redis
import time

# Redis connection
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
CHANNEL = "blitz.request"

def publish(payload, label):
    print(f"\n📤 {label}")
    print(json.dumps(payload, indent=2))
    r.publish(CHANNEL, json.dumps(payload))
    time.sleep(0.5)

# ============================ EXAMPLES ============================

# 1. PLACE ORDER with your custom Blitz ID
place_order = {
    "action": "PLACE_ORDER",
    "data": {
        "blitz_order_id": "123",  # ← Your custom ID
        "quantity": 1,
        "product": "MIS",
        "tif": "DAY",
        "price": 11.20,
        "orderType": "LIMIT",
        "symbol": "NSE|IDEA",
        "orderSide": "BUY",
        "stopPrice": 0
    }
}

# 2. MODIFY ORDER using the same Blitz ID
modify_order = {
    "action": "MODIFY_ORDER",
    "data": {
        "blitz_order_id": "123",  # ← Use same ID, no need for Zerodha ID!
        "quantity": 2,
        "price": 11.2
    }
}

# 3. CANCEL ORDER using the same Blitz ID
cancel_order = {
    "action": "CANCEL_ORDER",
    "data": {
        "blitz_order_id": "MY_ORDER_001"
    }
}

# ============================ MAIN ================================

if __name__ == "__main__":
    print("=" * 60)
    print("Blitz Order ID Example - Simplified Dictionary Approach")
    print("=" * 60)
    
    # Place order with Blitz ID
    #publish(place_order, "PLACE ORDER with Blitz ID")
    
    # Modify using Blitz ID (uncomment to use)
    publish(modify_order, "MODIFY ORDER with Blitz ID")
    
    # Cancel using Blitz ID (uncomment to use)
    # publish(cancel_order, "CANCEL ORDER with Blitz ID")
    
    print("\n✅ How it works:")
    print("   1. Connector stores: blitz_to_zerodha['MY_ORDER_001'] = '260106151469089'")
    print("   2. When you modify/cancel, it looks up the Zerodha ID automatically")
    print("   3. Simple dictionary - no extra files needed!")
