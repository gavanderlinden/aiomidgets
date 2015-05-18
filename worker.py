__author__ = 'tlinden'

import asyncio
import pickle


class Client(asyncio.Protocol):

    def connection_made(self, transport):
        self.connected = True
        self.transport = transport

    def data_received(self, data):
        asyncio.async(self.process_data(data))
        asyncio.sleep(3)

    def process_data(self, data):
        job, payload = pickle.loads(data)
        print("JOB", job)
        print("PAYLOAD", payload)
        yield from asyncio.sleep(3)
        self.transport.write(bytes("OK", "UTF-8"))

    def connection_lost(self, exc):
        print("ENDED")
        loop.stop()

    @asyncio.coroutine
    def communicate(self):
        protocol, client = yield from loop.create_connection(
            Client,
            "127.0.0.1",
            4444
        )
        client.transport.write(bytes("OK", "UTF-8"))
        yield from asyncio.sleep(2)

c = Client()
asyncio.Task(c.communicate())
loop = asyncio.get_event_loop()
loop.run_forever()