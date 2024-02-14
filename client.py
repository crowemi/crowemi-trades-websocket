import asyncio
from websockets.sync.client import connect


def client():
    while True:
        with connect("ws://localhost:8765") as websocket:
            message = websocket.recv()
            print(message)
            # TODO: push message to kafka


client()
