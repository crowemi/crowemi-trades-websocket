import os
import json
import base64
from datetime import datetime, timedelta
from pymongo import MongoClient
import asyncio
from websockets.server import serve


class Producer:
    def __init__(self) -> None:
        now = datetime.utcnow()
        self.next_period = datetime(
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute,
            0,
            0,
        )
        self.current_period = datetime(
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute,
            0,
            0,
        )

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
                # 1690231860000
                # 1689638220000
                return record
        else:
            raise ("No mongo uri found")

    async def run(
        self,
    ):
        try:
            # IMPORTANT! we need to make sure that the seconds & microseconds are 0
            while True:
                while self.current_period < self.next_period:
                    # wait for the next period
                    await asyncio.sleep(10)
                    print(
                        f"current_period: {self.current_period}; next_period: {self.next_period}"
                    )
                    self.current_period = datetime(
                        datetime.utcnow().year,
                        datetime.utcnow().month,
                        datetime.utcnow().day,
                        datetime.utcnow().hour,
                        datetime.utcnow().minute,
                        0,
                        0,
                    )

                record = json.dumps(self.get_record(self.next_period))
                self.next_period = self.next_period + timedelta(minutes=1)
                return record
        except Exception as e:
            print(e)


async def producer_handler(websocket):
    producer = Producer()
    while True:
        message = await producer.run()
        print(message)
        await websocket.send(message)


async def main():
    async with serve(producer_handler, "localhost", 8765):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    # start the websocket server
    asyncio.run(main())
