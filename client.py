import asyncio
from websockets.sync.client import connect


def hello():
    while True:
        with connect("ws://localhost:8765") as websocket:
            message = websocket.recv()
            print(f"Received: {message}")


hello()
