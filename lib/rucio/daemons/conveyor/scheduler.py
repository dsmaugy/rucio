
"""
The scheduler is a daemon that orders transfer requests before sending them to the submitter.
"""

from collections import defaultdict, deque
from email.policy import default
import logging
import threading
from types import FrameType
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple


import rucio.db.sqla.util

from rucio.common import exception
from rucio.common.config import config_get_bool
from rucio.common.logging import setup_logging
from rucio.common.types import InternalScope
from rucio.core.did import get_metadata, list_content
from rucio.core.monitor import MetricManager
from rucio.core.topology import Topology, ExpiringObjectCache
from rucio.core.request import list_and_mark_transfer_requests_and_source_replicas, update_request
from rucio.daemons.common import db_workqueue, ProducerConsumerDaemon
from rucio.db.sqla.constants import RequestState, RequestType, DIDType


if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from rucio.daemons.common import HeartbeatHandler
    from rucio.core.request import RequestWithSources

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
    
    if config_get_bool("conveyor", "use_preparer", default=False):
        raise exception.ConfigurationError("Preparer not enabled! Scheduler daemon only works when preparer is running.")
    
    cached_topology = ExpiringObjectCache(ttl=300, new_obj_fnc=lambda: Topology()) # TODO: we ignore "ignore_availablity" here

    scheduler(once=once, sleep_time=sleep_time, bulk=bulk, cached_topology=cached_topology)

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

# Receives the request object from `_fetch_requests` and performs the scheduling logic
def _handle_requests(elements: Tuple[Dict[str, 'SchedulerDataset'], Dict[str, int]]):
    unordered_datasets, rse_load_map = elements
    logging.debug("Scheduler Status is: %s", config_get_bool("conveyor", "use_scheduler", default=False))
    logging.debug("UNORDERED DATASETS: %s", unordered_datasets)
    logging.debug("RSE LOAD MAP: %s", rse_load_map)
    # DEBUG PRINT
    # for request_id, request in requests_to_schedule.items():
    #     logging.debug("Request %s: %s", request_id, str(request))
    #     logging.debug("\tActivity: %s | Internal Account: %s | External Account: %s | VO: %s | Priority: %d", 
    #                   request.activity, request.account.internal, request.account.external, request.account.vo, request.priority)
    #     logging.debug("\tScope: %s | Dest RSE %s | Source RSE %s", request.scope, request.dest_rse, request.requested_source)

    # SINCRONIA IMPLEMENTATION
        
    ordered_datasets = deque()

    while len(unordered_datasets) != 0:
        # 1. Find most bottlenecked RSE out of unordered datasets
        unordered_rse_loads = defaultdict(int)
        for dataset in unordered_datasets.values():
            for rse_id, rse_load in dataset.num_bytes_per_rse.items():
                unordered_rse_loads[rse_id] += rse_load
        bottlenecked_rse_id = max(unordered_rse_loads.items(), key= lambda x: x[1])[0]

        logging.debug("Bottlenecked RSE: %s with %s bytes", bottlenecked_rse_id, unordered_rse_loads[bottlenecked_rse_id])

         # 2. Find most unfair dataset on bottlenecked RSE and calculate num_bottlenecked_bytes for all unordered_datasets
        most_unfair_dataset = max(unordered_datasets.items(), key=lambda x: x[1].num_bytes_per_rse.get(bottlenecked_rse_id, 0) * x[1].weight)[0]
        logging.debug("Most Unfair Dataset: %s with %s bytes on RSE %s", most_unfair_dataset, unordered_datasets[most_unfair_dataset].num_bytes_per_rse[bottlenecked_rse_id], bottlenecked_rse_id)

        # 3. Re-adjust weights for unordered datasets
        for dataset_name, dataset in unordered_datasets.items():
            if dataset_name == most_unfair_dataset:
                pass
            dataset.weight = dataset.weight - (unordered_datasets[most_unfair_dataset].weight * (dataset.num_bytes_per_rse[bottlenecked_rse_id] / unordered_datasets[most_unfair_dataset].num_bytes_per_rse[bottlenecked_rse_id]))
        
        # 4. Add most unfair dataset to ordered dataset list
        ordered_datasets.appendleft(unordered_datasets[most_unfair_dataset])
        del unordered_datasets[most_unfair_dataset]
        
    logging.debug("Dataset Ordering: %s", ordered_datasets)
    # Prepare the ordered datasets to send to the throttler
    # TODO
    


    # 6. Send files to throttler in order of ordered datasets

    # trivial, mark ALL requests as QUEUED for the submitter without doing any ordering
    # this just shows us that our daemon execution is correct
    # for request_id in requests_to_schedule.keys():
        # update_request(request_id=request_id, state=RequestState.QUEUED)


# The database producer function, this retreives PREPARING requests from the database packaged in a `RequestWithSources` object
def _fetch_requests(bulk: int,
                    cached_topology,
                    heartbeat_handler: "HeartbeatHandler",
                    set_last_processed_by: bool,
                    *,
                      session: "Optional[Session]" = None,):
    worker_number, total_workers, logger = heartbeat_handler.live()
    topology = cached_topology.get() if cached_topology else Topology() # TODO: ignore "ignore_availability"
    topology.configure_multihop(logger=logger, session=session)
    requests_with_sources: Dict[str, 'RequestWithSources'] = list_and_mark_transfer_requests_and_source_replicas(
        rse_collection=topology,
        processed_by=heartbeat_handler.short_executable if set_last_processed_by else None,
        total_workers=total_workers,
        worker_number=worker_number,
        limit=bulk,
        request_state=RequestState.SCHEDULNG,
        request_type=[RequestType.TRANSFER], 
        processed_at_delay=1, # TODO: this is for debugging so we keep getting the from subsequent daemon runs quickly
        session=session,
    )
    
    rse_load_map: Dict[str, int] = defaultdict(int)
    unordered_datasets: Dict[str, SchedulerDataset] = {}

    for request in requests_with_sources.values():
        # TODO: testing purposes, delete these later
        # must do this because haven't implemented filling parent_dataset_* yet 
        # TODO: find first parent dataset
        request.parent_dataset_scope = request.scope # first dataset scope found for this file
        request.parent_dataset_name = "dataset1"     # first dataset name found for this file
        dataset_identifier = str(request.parent_dataset_scope) + ":" + request.parent_dataset_name

        unordered_datasets[dataset_identifier] = unordered_datasets.get(
            dataset_identifier, 
            SchedulerDataset(request.parent_dataset_scope, request.parent_dataset_name)
        )

        parent_dataset = unordered_datasets[dataset_identifier]
        
        parent_dataset_contents = list_content(scope=request.parent_dataset_scope, name=request.parent_dataset_name, session=session)
        # add up all the bytes being processed per RSE (src + dest)
        for sibling_file in parent_dataset_contents:
            if sibling_file['type'] == DIDType.FILE:
                rse_load_map[request.dest_rse.id] += sibling_file['bytes']
                parent_dataset.num_bytes_per_rse[request.dest_rse.id] += sibling_file['bytes']
                if request.requested_source:
                    rse_load_map[request.requested_source.rse.id] += sibling_file['bytes']
                    parent_dataset.num_bytes_per_rse[request.requested_source.rse.id] += sibling_file['bytes']
    
    logging.debug("RSE Load: %s", rse_load_map)

    return False, (unordered_datasets, rse_load_map)


class SchedulerDataset:

    def __init__(self, scope: InternalScope, name: str):
        self._id = (scope, name)
        self.scope = scope
        self.name = name
        self.num_bytes_per_rse = defaultdict(int)  # number of bytes this dataset takes up in this scheduling cycle
        self.num_bottlenecked_bytes = 0 # number of bytes this dataset takes up on the currently most bottlenecked port
        self.weight = 1.0

    def __eq__(self, __value: 'SchedulerDataset') -> bool:
        return __value._id == self._id
    
    def __hash__(self) -> int:
        return hash(self._id)
