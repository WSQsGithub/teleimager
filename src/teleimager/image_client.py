"""Backward-compatible imports for TeleImager client APIs."""

from teleimager.networking.zmq_config import ZMQ_Requester, ZMQ_Responser
from teleimager.networking.zmq_streaming import (
    ZMQ_PublisherManager,
    ZMQ_PublisherThread,
    ZMQ_SubscriberManager,
    ZMQ_SubscriberThread,
)
from teleimager.utilities.buffers import SimpleFPSMonitor, TeleImage, TripleRingBuffer
from teleimager.video_streaming.client import ImageClient, main

__all__ = [
    "TripleRingBuffer",
    "SimpleFPSMonitor",
    "TeleImage",
    "ZMQ_PublisherThread",
    "ZMQ_PublisherManager",
    "ZMQ_SubscriberThread",
    "ZMQ_SubscriberManager",
    "ZMQ_Responser",
    "ZMQ_Requester",
    "ImageClient",
    "main",
]


if __name__ == "__main__":
    main()
