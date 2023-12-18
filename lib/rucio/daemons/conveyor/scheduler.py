
"""
The scheduler is a daemon that orders transfer requests before sending them to the submitter.
"""

import logging
import threading
from types import FrameType
from typing import TYPE_CHECKING, Optional

import rucio.db.sqla.util

from rucio.common import exception
from rucio.common.logging import setup_logging
from rucio.core.monitor import MetricManager
from rucio.core.topology import Topology, ExpiringObjectCache
from rucio.core.request import list_and_mark_transfer_requests_and_source_replicas
from rucio.daemons.common import db_workqueue, ProducerConsumerDaemon
from rucio.db.sqla.constants import RequestState, RequestType



if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from rucio.daemons.common import HeartbeatHandler

GRACEFUL_STOP = threading.Event()
METRICS = MetricManager(module=__name__) # TODO: do we want to keep track of metrics or nah?
DAEMON_NAME = 'conveyor-scheduler'


def stop(signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """
    Graceful exit.
    """

    GRACEFUL_STOP.set()

def run(
        once: bool,
        sleep_time: int,
        bulk=100,
):
    """
    Running the scheduler daemon either once or by default in a loop until stop is called.
    """
    setup_logging(process_name=DAEMON_NAME)

    if rucio.db.sqla.util.is_old_db():
        raise exception.DatabaseException('Database was not updated, daemon won\'t start')
    
    cached_topology = ExpiringObjectCache(ttl=300, new_obj_fnc=lambda: Topology()) # TODO: we ignore "ignore_availablity" here

    scheduler(once=once, sleep_time=sleep_time, bulk=bulk, cached_topology=cached_topology)

# TODO: should create producer/consumer objects and add them to the ProducerConsumerDaemon
# TODO: handle run once vs. event-based runs?
# main entrypoint to scheduler daemon
def scheduler(once: bool, 
              sleep_time: int, 
              bulk: int, 
              cached_topology,
              partition_wait_time=10, 
              executable: str = DAEMON_NAME
              ):
    logging.debug("Starting Scheduling Cycle")

    @db_workqueue(
        once=once,
        graceful_stop=GRACEFUL_STOP,
        executable=executable,
        partition_wait_time=partition_wait_time,
        sleep_time=sleep_time)
    def _db_producer(*, activity, heartbeat_handler):
        return _fetch_requests(bulk=bulk, cached_topology=cached_topology, heartbeat_handler=heartbeat_handler, set_last_processed_by=not once)

    def _consumer(requests_to_schedule):
        return _handle_requests(requests_to_schedule)

    ProducerConsumerDaemon(
        producers=[_db_producer],
        consumers=[_consumer],
        graceful_stop=GRACEFUL_STOP,
    ).run()

def _handle_requests(requests_to_schedule):
    logging.debug("Scheduling Following Requests: " + str(requests_to_schedule))

def _fetch_requests(bulk: int,
                    cached_topology,
                    heartbeat_handler: "HeartbeatHandler",
                    set_last_processed_by: bool,
                    *,
                      session: "Optional[Session]" = None,):
    worker_number, total_workers, logger = heartbeat_handler.live()
    topology = cached_topology.get() if cached_topology else Topology() # TODO: ignore "ignore_availability"
    topology.configure_multihop(logger=logger, session=session)
    requests_with_sources = list_and_mark_transfer_requests_and_source_replicas(
        rse_collection=topology,
        processed_by=heartbeat_handler.short_executable if set_last_processed_by else None,
        total_workers=total_workers,
        worker_number=worker_number,
        limit=bulk,
        request_state=RequestState.PREPARING,
        # request_type=[RequestType.TRANSFER],
        processed_at_delay=1, # TODO: this is for debugging so we keep getting the from subsequent daemon runs quickly
        session=session,
    )
    return False, (topology, requests_with_sources)


"""
TODO:

- CLI binary
- How to modify preparer/throttler and submitter so that we can lie in between them
    - Go before throttler??
- Figure out scheduling framework/logic
    - Can we get VOs?
    - 

Milestone: Intercept requests in between different daemon stages
     - Get requests from preparer/throttler, have it in memory, send it off to submitter
"""