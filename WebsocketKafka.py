import json
import websocket
import threading
from kafka import KafkaProducer

ALPACA_API_KEY = "PK515XOV4WEW0NV1SIDJ"
ALPACA_SECRET_KEY = "09qDxGCUdtXOtjYgV1uglO5W6vkuf6UMLhMMeGpu"

ALPACA_STREAM_URL = "wss://stream.data.alpaca.markets/v2/test"

# Kafka config
KAFKA_TOPIC = "tick-data-new"
KAFKA_SERVER = "172.19.50.120:9092"

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_open(ws):
    print("WebSocket connection opened.")

    # Authenticate
    auth_msg = {
        "action": "auth",
        "key": ALPACA_API_KEY,
        "secret": ALPACA_SECRET_KEY
    }
    ws.send(json.dumps(auth_msg))

    # After authentication, subscribe to stock trades (choose your symbols)
    subscribe_msg = {
        "action": "subscribe",
        "trades": ["FAKEPACA"]  # List your stock symbols here
    }
    ws.send(json.dumps(subscribe_msg))

def on_message(ws, message):
    print("Received message:", message)

    try:
        data = json.loads(message)

        # Send each tick to Kafka
        for tick in data:
            if tick.get("T") == "t":
                producer.send(KAFKA_TOPIC, value={"symbol":tick['S'], "price": tick['p'], "size": tick['s'], "timestamp": tick['t']})

    except Exception as e:
        print("Error processing message:", e)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed:", close_status_code, close_msg)

def start_websocket():
    ws = websocket.WebSocketApp(
        ALPACA_STREAM_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # Run WebSocket in a separate thread
    wst = threading.Thread(target=ws.run_forever)
    wst.start()

if __name__ == "__main__":
    start_websocket()
