
"""
The scheduler is a daemon that orders transfer requests before sending them to the submitter.
"""

from collections import defaultdict, deque
from email.policy import default
import logging
import threading
from types import FrameType
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Tuple



import rucio.db.sqla.util

from rucio.common import exception
from rucio.common.config import config_get_bool
from rucio.common.logging import setup_logging
from rucio.common.types import InternalScope
from rucio.core.did import get_metadata, list_content, list_parent_dids
from rucio.core.monitor import MetricManager
from rucio.core.topology import Topology, ExpiringObjectCache
from rucio.core.request import list_and_mark_transfer_requests_and_source_replicas, update_request
from rucio.core.transfer import applicable_rse_transfer_limits
from rucio.daemons.common import db_workqueue, ProducerConsumerDaemon
from rucio.db.sqla.constants import RequestState, RequestType, DIDType
from rucio.db.sqla import models


if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from rucio.daemons.common import HeartbeatHandler
    from rucio.core.request import RequestWithSources

GRACEFUL_STOP = threading.Event()
METRICS = MetricManager(module=__name__) # TODO: do we want to keep track of metrics or nah?
DAEMON_NAME = 'conveyor-scheduler'

scheduling_starting_count = 0 # starting number for scheduling priority for this scheduling cycle

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
    
    if not config_get_bool("conveyor", "use_preparer", default=False):
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
def _handle_requests(elements: Tuple[Dict[str, 'RequestWithSources'], Dict[str, 'SchedulerDataset']]):
    requests_with_sources, dataset_map = elements
    logging.debug("Scheduler Status is: %s", config_get_bool("conveyor", "use_scheduler", default=False))
    logging.debug("UNORDERED DATASETS: %s", dataset_map)
    # SINCRONIA IMPLEMENTATION
        
    ordered_datasets: Deque[str] = deque()
    unordered_datsets = dataset_map.copy()

    while len(unordered_datsets) != 0:
        # 1. Find most bottlenecked RSE out of unordered datasets
        unordered_rse_loads = defaultdict(int)
        for dataset in unordered_datsets.values():
            for rse_id, rse_load in dataset.num_bytes_per_rse.items():
                unordered_rse_loads[rse_id] += rse_load
        bottlenecked_rse_id = max(unordered_rse_loads.items(), key= lambda x: x[1])[0]

        logging.debug("Bottlenecked RSE: %s with %s bytes", bottlenecked_rse_id, unordered_rse_loads[bottlenecked_rse_id])

         # 2. Find most unfair dataset on bottlenecked RSE and calculate num_bottlenecked_bytes for all unordered_datasets
        most_unfair_dataset = max(unordered_datsets.items(), key=lambda x: x[1].num_bytes_per_rse.get(bottlenecked_rse_id, 0) * x[1].weight)[0]
        logging.debug("Most Unfair Dataset: %s with %s bytes on RSE %s", most_unfair_dataset, unordered_datsets[most_unfair_dataset].num_bytes_per_rse[bottlenecked_rse_id], bottlenecked_rse_id)

        # 3. Re-adjust weights for unordered datasets
        for dataset_name, dataset in unordered_datsets.items():
            if dataset_name == most_unfair_dataset:
                pass
            dataset.weight = dataset.weight - (unordered_datsets[most_unfair_dataset].weight * (dataset.num_bytes_per_rse[bottlenecked_rse_id] / unordered_datsets[most_unfair_dataset].num_bytes_per_rse[bottlenecked_rse_id]))
        
        # 4. Add most unfair dataset to ordered dataset list
        ordered_datasets.appendleft(most_unfair_dataset)
        del unordered_datsets[most_unfair_dataset]
    logging.debug("Dataset Ordering: %s", ordered_datasets)

    # keep track of scheduling order
    scheduling_offset: Dict[str, int] = {}
    if len(ordered_datasets) > 0:
        scheduling_offset[ordered_datasets[0]] = scheduling_starting_count

    # the offsets don't line up exactly due to the multiple dataset problem
    # but this doesn't matter as only the relative values of the ordering is relevant
    for i in range(1, len(ordered_datasets)):
        scheduling_offset[ordered_datasets[i]] = scheduling_offset[ordered_datasets[i-1]] + dataset_map[ordered_datasets[i-1]].num_files
    
    for transfer_request in requests_with_sources.values():
        dataset_identifier = str(transfer_request.parent_dataset_scope) + ":" + str(transfer_request.parent_dataset_name)
        
        update_items: dict[Any, Any] = {
            models.Request.scheduling_order.name: scheduling_offset[dataset_identifier],
            models.Request.state.name: RequestState.WAITING
        }
        logging.debug("Sending transfer %s to throttler with scheduling order %d", str(transfer_request), scheduling_offset[dataset_identifier])
        # Send files to throttler in order of ordered datasets
        update_request(request_id=transfer_request.request_id, **update_items)
        scheduling_offset[dataset_identifier] += 1



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
        session=session,
    )
    
    rse_load_map: Dict[str, int] = defaultdict(int)
    unordered_datasets: Dict[str, SchedulerDataset] = {}

    for request in requests_with_sources.values():
        # assume that the first parent DID is the dataset we want to optimize for for this file
        # this is for proof-of-concept purposes
        # the optimal solution would be for clients to choose which dataset is the target dataset when submitting new transfer requests
        parent_did = next(list_parent_dids(request.scope, request.name))

        # logging.debug("parent did %s", parent_did)
        request.parent_dataset_scope = parent_did["scope"] # first dataset scope found for this file
        request.parent_dataset_name = parent_did["name"]     # first dataset name found for this file
        dataset_identifier = str(request.parent_dataset_scope) + ":" + request.parent_dataset_name

        unordered_datasets[dataset_identifier] = unordered_datasets.get(
            dataset_identifier, 
            SchedulerDataset(request.parent_dataset_scope, request.parent_dataset_name)
        )

        parent_dataset = unordered_datasets[dataset_identifier]
        
        parent_dataset_contents = list_content(scope=request.parent_dataset_scope, name=request.parent_dataset_name, session=session)
        # add up all the bytes being processed per RSE (src + dest)
        for sibling_file in parent_dataset_contents:
            parent_dataset.num_files += 1
            if sibling_file['type'] == DIDType.FILE:
                rse_load_map[request.dest_rse.id] += sibling_file['bytes']
                parent_dataset.num_bytes_per_rse[request.dest_rse.id] += sibling_file['bytes']
                if request.requested_source:
                    rse_load_map[request.requested_source.rse.id] += sibling_file['bytes']
                    parent_dataset.num_bytes_per_rse[request.requested_source.rse.id] += sibling_file['bytes']
    
    logging.debug("RSE Load: %s", rse_load_map)

    return False, (requests_with_sources, unordered_datasets)


class SchedulerDataset:

    def __init__(self, scope: InternalScope, name: str):
        self._id = (scope, name)
        self.scope = scope
        self.name = name
        self.num_bytes_per_rse = defaultdict(int)  # number of bytes this dataset takes up in this scheduling cycle
        self.weight = 1.0
        self.num_files = 0

    def dataset_equals(self, scope: InternalScope, name: str) -> bool:
        return self.scope == scope and self.name == name

    def __eq__(self, __value: 'SchedulerDataset') -> bool:
        return __value._id == self._id
    
    def __hash__(self) -> int:
        return hash(self._id)
