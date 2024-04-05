import queue
from abc import ABC, abstractmethod


class TOPIC:
    event_stream = "event_stream"

class Publisher:
    def __init__(self):
        self.subscribers = {}

    def subscribe(self, subscriber, topic):
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(subscriber)

    def publish(self, message, topic):
        if topic in self.subscribers:
            for subscriber in self.subscribers[topic]:
                subscriber.queue.put(message)

class Subscriber(ABC):
    def __init__(self, name):
        self.name = name
        self.queue = queue.Queue()

    def read(self, ):
        return self.queue.get()

