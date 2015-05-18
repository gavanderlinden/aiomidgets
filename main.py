__author__ = 'tlinden'

import asyncio
import pickle

loop = asyncio.get_event_loop()


class Echo(asyncio.Protocol):

    def connection_made(self, transport):
        print("CONNECTION MADE")
        self.transport = transport

    def data_received(self, data):
        asyncio.async(c.process_data(data, self.transport))

    def connection_lost(self, exc):
        server.close()

class Core(object):

    def __init__(self):
        self.jobs = asyncio.Queue()
        self.workers = asyncio.Queue()

    @asyncio.coroutine
    def process_data(self, data, transport):
        message = data.decode("UTF8")
        if message == "OK":
            yield from self.workers.put([id, transport])
            print("WORKERS", self.workers.qsize())

    @asyncio.coroutine
    def add_job(self):
        while True:
            print("NUMBER OF JOBS", self.jobs.qsize())
            yield from self.jobs.put("test")
            yield from asyncio.sleep(3)

    @asyncio.coroutine
    def get_job(self):
        while True:
            job = yield from self.jobs.get()
            id, worker = yield from self.workers.get()
            worker.write(pickle.dumps([job, "me"]))

c = Core()

asyncio.Task(c.add_job())
asyncio.Task(c.get_job())
server = loop.run_until_complete(loop.create_server(Echo, '127.0.0.1', 4444))
loop.run_until_complete(server.wait_closed())