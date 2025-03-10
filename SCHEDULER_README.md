# Dataset Scheduler

_Darwin Do and Addison Goolsbee_
_As part of Professor Y. Richard Yang's Topics in Networked Systems course_

The Rucio dataset scheduler daemon is an optional daemon that causes file transfers to optimize for dataset completion time instead of just file transfer completion time.

Because most application tasks require all data to be available in order to start, networks should optimize for task (a.k.a. dataset) transfer completion time instead of individual file transfer completion time. The dataset scheduler Daemon was inspired by [Sincronia: Near-Optimal Newtork Design for Coflows](https://www.cs.cornell.edu/~ragarwal/pubs/sincronia.pdf)

## Relevant Files

### New Files

- `lib/rucio/daemons/conveyor/scheduler.py`: the scheduler is a new optional daemon that sits between the preparer and the throttler, and contains the bulk of the new code. This file implements the dataset scheduler algorithm every fixed amount of time, and sets an ordered set of datasets to a state where they can be picked up by the throttler
- `bin/rucio-conveyor-scheduler`: this is the command/script to run the daemon. Takes the following optional arguments:

  - `--run-once`: runs a single time
  - `--bulk`: limits the number of requests per chunk, defaults to 100
  - `--sleep-time`: the amount of time in seconds between running the scheduling algortihm. As the time increases the algorithm becomes more effective, but the chance of wasting time increases

### Relevant Modified Files

- `lib/rucio/db/sqla/constants.py`: the `RequestState` enum now has an additional state: `SCHEDULING`, for the preparer to set if the scheduler is running
- `lib/rucio/alembicrevision.py`: updated to reflect `RequestState` enum changes
- `lib/rucio/core/transfer.py`: the function `_throttler_request_state` has been modified to set the transfer state to `SCHEDULING` instead of `WAITING` when the scheduler is active
- `lib/rucio/db/sqla/models.py`: the request table has a new field, `scheduling_order`, which is used to tell the throttler what order to pass requests in if the scheduler is active (by default the order is by date created)
- `lib/rucio/core/request.py`: the function `list_and_mark_transfer_requests_and_source_replicas` now has an additional optional boolean parameter `order_by_scheduler`, which if set, returns a query sorted by `scheduling_order`
- `lib/rucio/daemons/submitter.py`: the submitter now queries orders by `scheduling_order` if the scheduler is active

## Implementation

There is a new optional boolean variable for the rucio config file: `use_scheduler`. It goes under the `[conveyor]` section. This is how other daemons konw the scheduler is running. If one attempts to start the scheduler daemon without this variable being set, it will throw an error

Here is the general program flow of a Rucio instance that has a scheduler:

1. The preparer marks transfers as `SCHEDULING`
2. Every `sleep-time` seconds, the scheduler fetches all transfers marked as `SCHEDULING`
3. The scheduler organizes the transfers into datasets, where if a transfer is part of multiple datasets, it will select the first one it finds
4. The scheduler calculates all RSEs (both source and destination) used between the transfers, the total number of bytes of each transfer at each RSE, and the total number of bytes of each dataset at each RSE
5. The scheduler performs the scheduling algorithm on the fetched transfers
6. The scheduler marks all the now-organized transfers as `WAITING` so the throttler can accept them. It also orders them in the database using the `scheduling_order` field
7. The throttler fetches all transfers marked as `WAITING`, and orders them by `scheduling_order` in the `list_and_mark_transfer_requests_and_source_replicas` function
8. From this point on, everything is the same: the throttler gradually marks the newly sorted transfers as `SUBMITTING`, the submitter submits the transfers, etc

### Scheduling Algorithm

The scheduling algorithm is a four-step process. It begins after `sleep-time` seconds with an unordered list of datasets containing transfers. Sorting the unordered list occurs in the following way:

1. **Find the most bottlenecked RSE out of unordered datasets**: using the RSE and bytes per transfer/dataset information, find the RSE that has the most data running through it. This is the bottlenecked RSE (for this iteration)
2. **Find the most unfair dataset on bottlenecked RSE and calculate num_bottlenecked_bytes for all unordered_datasets**: every dataset object in the scheduler (called a `SchedulerDataset`) has an instance variable called weight, which defaults to one. The most unfair dataset on the bottlenecked RSE is defined by the dataset with the smallest value of (weight / data through bottlenecked RSE). This is the unfair dataset
3. **Re-adjust weights for unordered datasets**: the unfair dataset is going to be removed, so we need to recalculate the weights on all the other datasets. Any other dataset with data running through the bottlenecked RSE needs to be weighted less. For each dataset, we readjust the weight to be weight = (weight - (unfair dataset's weight * (data through bottlenecked RSE / unfair dataset's data through bottlenecked RSE)))
4. **Add most unfair dataset to ordered dataset list**: append the unfair dataset to the beginning of the ordered_datasets list, which is a deque. This will become the last value, since it is always appended to the beginning. Remove the unfair dataset from the list of unordered_datasets
5. **Repeat previous steps until the unordered list is empty**

## Testing

To test, we first bootstrap all the relevant docker containers needed to run Rucio and transfer files:
```
docker compose --file etc/docker/dev/docker-compose.yml --profile storage up -d
```

Then we bootstrap the Rucio server with test RSEs, files, and the database schema inside the dev-rucio-1 container:
```
tools/run_tests.sh -ir
```

This creates a Rucio RSE rule that requires 4 transfers to happen in order to satisfy the rule.

To add throttler limits, we have to do that in code. We open a Python process and type the following:
```
from rucio.core.request import set_transfer_limit, list_transfer_limits

list(list_transfer_limits())
set_transfer_limit("XRD1", max_transfers=20)
set_transfer_limit("XRD2", max_transfers=20)
set_transfer_limit("XRD3", max_transfers=20)
set_transfer_limit("XRD4", max_transfers=20)
```

Then we run each conveyor daemon manually to see how scheduling process work:

```
rucio-conveyor-preparer --run-once
...
rucio-conveyor-scheduler --run-once
...
2023-12-20 21:08:00,474 root    1441    DEBUG   Scheduler Status is: True
2023-12-20 21:08:00,474 root    1441    DEBUG   UNORDERED DATASETS: {'test:dataset1': <rucio.daemons.conveyor.scheduler.SchedulerDataset object at 0x7f50e03199d0>, 'test:dataset2': <rucio.daemons.conveyor.scheduler.SchedulerDataset object at 0x7f50e0319ac0>}
2023-12-20 21:08:00,474 root    1441    DEBUG   Bottlenecked RSE: bf8cefdf50154bbf9e840ece281a6476 with 83886080 bytes
2023-12-20 21:08:00,474 root    1441    DEBUG   Most Unfair Dataset: test:dataset1 with 41943040 bytes on RSE bf8cefdf50154bbf9e840ece281a6476
2023-12-20 21:08:00,474 root    1441    DEBUG   Bottlenecked RSE: bf8cefdf50154bbf9e840ece281a6476 with 41943040 bytes
2023-12-20 21:08:00,474 root    1441    DEBUG   Most Unfair Dataset: test:dataset2 with 41943040 bytes on RSE bf8cefdf50154bbf9e840ece281a6476
2023-12-20 21:08:00,474 root    1441    DEBUG   Dataset Ordering: deque(['test:dataset2', 'test:dataset1'])
2023-12-20 21:08:00,475 root    1441    DEBUG   Sending transfer adb1c0953ebf4a39b198cbd2c98704ef(test:file1) to throttler with scheduling order 4
2023-12-20 21:08:00,476 root    1441    DEBUG   Sending transfer 68dd79394777435a98a47d24581fe501(test:file2) to throttler with scheduling order 5
2023-12-20 21:08:00,477 root    1441    DEBUG   Sending transfer bd55437bbdb5422ba85db95ff163ade8(test:file3) to throttler with scheduling order 0
2023-12-20 21:08:00,478 root    1441    DEBUG   Sending transfer 7cec9f39b41e4ba994dbd09200ba6a08(test:file4) to throttler with scheduling order 1
...
rucio-conveyor-throttler --run-once
...
rucio-conveyor-submitter --run-once
...
```

From this test, we see that the order picked was file3, file4 (to satisfy dataset2) and then file1, file2 (to satisfy dataset1).
