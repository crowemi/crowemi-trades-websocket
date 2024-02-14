import os
import json
import base64
from datetime import datetime, timedelta
from time import sleep
from multiprocessing import Queue, Process
import functools

from pymongo import MongoClient
import asyncio
from websockets.server import serve


def update_period() -> datetime:
    now = datetime.utcnow()
    return datetime(
        now.year,
        now.month,
        now.day,
        now.hour,
        now.minute,
        0,
        0,
    )

class Producer:
    def __init__(self, queue: Queue) -> None:
        self.queue = queue

    def get_record(self, record_dt: datetime, **kwargs) -> dict:
        yesterday = record_dt - timedelta(days=1)
        timestamp = int(yesterday.timestamp() * 1000)
        mongo_uri = os.getenv("MONGODB_URI")

        if mongo_uri:
            with MongoClient(mongo_uri) as client:
                db = client.get_database(
                    kwargs.get("database", "data")
                )  # default: data
                collection = db.get_collection(
                    kwargs.get("collection", "C:EURUSD/minute/1")
                )  # default: C:EURUSD/minute/1
                record = collection.find_one(
                    {"timestamp": timestamp}, {"_id": 0, "timestamp_d": 0}
                )
                ret = record if record is not None else {}
                return ret
        else:
            raise ("No mongo uri found")

    async def run(
        self,
    ):
        try:
            while True:
                next_period = self.queue.get()
                if next_period:
                    print(f"Next period: {next_period}")
                    return json.dumps(self.get_record(next_period))
        except Exception as e:
            print(e)


async def producer_handler(websocket, record_queue: Queue):
    producer = Producer(record_queue)
    message = await producer.run()
    print(message)
    await websocket.send(message)

def watcher(queue: Queue):
    current_period = update_period()
    last_period = current_period - timedelta(minutes=1)
    print(f"current_period: {current_period}")
    print(f"last_period: {last_period}")

    while True:
        if current_period > last_period:
            print(f"current period: {current_period}")
            queue.put_nowait(current_period)
            last_period = current_period
        else:
            sleep(10)
            current_period = update_period()

async def main():
    queue = Queue()
    watch = Process(target=watcher, args=(queue,))
    watch.start()

    queued_producer_handeler = functools.partial(producer_handler, record_queue=queue)
    async with serve(queued_producer_handeler, "localhost", 8765):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    # start the websocket server
    asyncio.run(main())
