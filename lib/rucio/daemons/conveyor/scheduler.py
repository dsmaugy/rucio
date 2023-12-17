
"""
The scheduler is a daemon that orders transfer requests before sending them to the submitter.
"""

import threading
from typing import TYPE_CHECKING, Optional

from rucio.core.monitor import MetricManager

if TYPE_CHECKING:
    from rucio.daemons.common import HeartbeatHandler

GRACEFUL_STOP = threading.Event()
METRICS = MetricManager(module=__name__) # TODO: do we want to keep track of metrics or nah?
DAEMON_NAME = 'conveyor-scheduler'


# TODO: should create producer/consumer objects and add them to the ProducerConsumerDaemon
# TODO: handle run once vs. event-based runs?
# main entrypoint to scheduler daemon
def scheduler():
    pass

def _handle_requests():
    pass

def _fetch_requests():
    pass