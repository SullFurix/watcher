import datetime
import time
import logging
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Callable, List, Iterable

from web3 import Web3
from web3.contract import Contract
from web3.datastructures import AttributeDict
from web3.exceptions import BlockNotFound
from eth_abi.codec import ABICodec

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
from web3.middleware import geth_poa_middleware

logger = logging.getLogger(__name__)


class TransferWatcherState(ABC):
    @abstractmethod
    def get_last_watched_block(self) -> int:
        """Number of the last block we have scanned on the previous cycle.

        :return: 0 if no blocks scanned yet
        """

    @abstractmethod
    def start_chunk(self, block_number: int):
        """Scanner is about to ask data of multiple blocks over JSON-RPC.

        Start a database session if needed.
        """

    @abstractmethod
    def end_chunk(self, block_number: int):
        """Scanner finished a number of blocks.

        Persistent any data in your state now.
        """

    @abstractmethod
    def process_event(self, block_when: datetime.datetime, event: AttributeDict) -> object:
        """Process incoming events.

        This function takes raw events from Web3, transforms them to your application internal
        format, then saves them in a database or some other state.

        :param block_when: When this block was mined

        :param event: Symbolic dictionary of the event data

        :return: Internal state structure that is the result of event tranformation.
        """

    @abstractmethod
    def delete_data(self, since_block: int) -> int:
        """Delete any data since this block was scanned.

        Purges any potential minor reorg data.
        """

class TransferWatcher:
    def __init__(self, w3: Web3, contract: Contract, state: TransferWatcherState, events: List, filters: {},
                 max_chunk_watch_size: int = 10000, max_request_retries: int = 30, request_retry_seconds: float = 3.0):

        self.logger = logger
        self.contract = contract
        self.w3 = w3
        self.state = state
        self.events = events
        self.filters = filters

        self.min_watch_chunk_size = 10  # 12 s/block = 120 seconds period
        self.max_watch_chunk_size = max_chunk_watch_size
        self.max_request_retries = max_request_retries
        self.request_retry_seconds = request_retry_seconds

        # Factor how fast we increase the chunk size if results are found
        # # (slow down watch after starting to get hits)
        self.chunk_size_decrease = 0.5

        # Factor how was we increase chunk size if no results found
        self.chunk_size_increase = 2.0

    @property
    def address(self):
        return self.token_address

    def get_block_timestamp(self, block_num) -> datetime.datetime:
        try:
            block_info = self.w3.eth.getBlock(block_num)
        except BlockNotFound:
            # Block was not mined yet,
            return None
        last_time = block_info["timestamp"]
        return datetime.datetime.utcfromtimestamp(last_time)

    def get_suggested_watch_start_block(self):
        end_block = self.get_last_watched_block()
        if end_block:
            return max(1, end_block - self.NUM_BLOCKS_REWATCH_FOR_FORKS)
        return 1

    def get_suggested_watch_end_block(self):
        # Do not watch all the way to the final block, as this
        # block might not be mined yet
        return self.w3.eth.blockNumber - 1

    def get_last_watched_block(self) -> int:
        return self.state.get_last_watched_block()

    def delete_potentially_forked_block_data(self, after_block: int):
        self.state.delete_data(after_block)

    def watch_chunk(self, start_block, end_block) -> Tuple[int, datetime.datetime, list]:
        block_timestamps = {}
        get_block_timestamp = self.get_block_timestamp

        # Cache block timestamps to reduce some RPC overhead
        # Real solution might include smarter models around block
        def get_block_when(block_num):
            if block_num not in block_timestamps:
                block_timestamps[block_num] = get_block_timestamp(block_num)
            return block_timestamps[block_num]

        all_processed = []

        for event_type in self.events:

            # Callable that takes care of the underlying web3 call
            def _fetch_events(_start_block, _end_block):
                return _fetch_events_for_all_contracts(self.w3,
                                                       event_type,
                                                       self.filters,
                                                       from_block=_start_block,
                                                       to_block=_end_block)
            # Do `n` retries on `eth_getLogs`,
            # throttle down block range if needed
            end_block, events = _retry_web3_call(
                _fetch_events,
                start_block=start_block,
                end_block=end_block,
                retries=self.max_request_retries,
                delay=self.request_retry_seconds)

            for evt in events:
                idx = evt["logIndex"]  # Integer of the log index position in the block, null when its pending

                # We cannot avoid minor chain reorganisations, but
                # at least we must avoid blocks that are not mined yet
                assert idx is not None, "Somehow tried to watch a pending block"

                block_number = evt["blockNumber"]

                # Get UTC time when this event happened (block mined timestamp)
                # from our in-memory cache
                block_when = get_block_when(block_number)

                logger.debug(f"Processing event {evt['event']}, block: {evt['blockNumber']} count: {evt['blockNumber']}")
                processed = self.state.process_event(block_when, evt)
                all_processed.append(processed)

        end_block_timestamp = get_block_when(end_block)
        return end_block, end_block_timestamp, all_processed

    def estimate_next_chunk_size(self, current_chuck_size: int, event_found_count: int):
        if event_found_count > 0:
            # When we encounter first events, reset the chunk size window
            current_chuck_size = self.min_watch_chunk_size
        else:
            current_chuck_size *= self.chunk_size_increase

        current_chuck_size = max(self.min_watch_chunk_size, current_chuck_size)
        current_chuck_size = min(self.max_watch_chunk_size, current_chuck_size)
        return int(current_chuck_size)

    def watch(self, start_block, end_block, start_chunk_size=20, progress_callback=Optional[Callable]) -> Tuple[
        list, int]:

        assert start_block <= end_block

        current_block = start_block

        # watch in chunks, commit between
        chunk_size = start_chunk_size
        last_watch_duration = last_logs_found = 0
        total_chunks_watched = 0

        # All processed entries we got on this watch cycle
        all_processed = []

        while current_block <= end_block:

            self.state.start_chunk(current_block, chunk_size)

            # Print some diagnostics to logs to try to fiddle with real world JSON-RPC API performance
            estimated_end_block = current_block + chunk_size

            if(estimated_end_block >= end_block):
                estimated_end_block = end_block

            logger.debug(
                f"Watching token transfers for blocks: {current_block} - {estimated_end_block}, chunk size {chunk_size}, last chunk watch took {last_watch_duration}, last logs found {last_logs_found}"
            )

            start = time.time()

            actual_end_block, end_block_timestamp, new_entries = self.watch_chunk(current_block, estimated_end_block)

            # Where does our current chunk watch ends - are we out of chain yet?
            current_end = actual_end_block

            last_watch_duration = time.time() - start
            all_processed += new_entries

            # Print progress bar
            if progress_callback:
                progress_callback(start_block, end_block, current_block, end_block_timestamp, chunk_size, len(new_entries))

            # Try to guess how many blocks to fetch over `eth_getLogs` API next time
            chunk_size = self.estimate_next_chunk_size(chunk_size, len(new_entries))

            # Set where the next chunk starts
            current_block = current_end + 1
            total_chunks_watched += 1

            self.state.end_chunk(current_end)

        return all_processed, total_chunks_watched


def _retry_web3_call(func, start_block, end_block, retries, delay) -> Tuple[int, list]:
    for i in range(retries):
        try:
            return end_block, func(start_block, end_block)
        except Exception as e:
            # Assume this is HTTPConnectionPool(host='localhost', port=8545): Read timed out. (read timeout=10)
            # from Go Ethereum. This translates to the error "context was cancelled" on the server side:
            # https://github.com/ethereum/go-ethereum/issues/20426
            if i < retries - 1:
                # Give some more verbose info than the default middleware
                logger.warning(
                    f"Retrying events for block range {start_block} - {end_block} ({end_block-start_block}) failed with {e} , retrying in {delay} seconds")
                # Decrease the `eth_getBlocks` range
                end_block = start_block + ((end_block - start_block) // 2)
                # Let the JSON-RPC to recover e.g. from restart
                time.sleep(delay)
                continue
            else:
                logger.warning("Out of retries")
                raise


def _fetch_events_for_all_contracts(
        w3,
        event,
        argument_filters: dict,
        from_block: int,
        to_block: int) -> Iterable:

    if from_block is None:
        raise TypeError("Missing mandatory keyword argument to getLogs: fromBlock")

    # Currently no way to poke this using a public Web3.py API.
    # This will return raw underlying ABI JSON object for the event
    abi = event._get_event_abi()

    # Depending on the Solidity version used to compile
    # the contract that uses the ABI,
    # it might have Solidity ABI encoding v1 or v2.
    # We just assume the default that you set on Web3 object here.
    # More information here https://eth-abi.readthedocs.io/en/latest/index.html
    codec: ABICodec = w3.codec

    # Here we need to poke a bit into Web3 internals, as this
    # functionality is not exposed by default.
    # Construct JSON-RPC raw filter presentation based on human readable Python descriptions
    # Namely, convert event names to their keccak signatures
    # More information here:
    # https://github.com/ethereum/web3.py/blob/e176ce0793dafdd0573acc8d4b76425b6eb604ca/web3/_utils/filters.py#L71
    data_filter_set, event_filter_params = construct_event_filter_params(
        abi,
        codec,
        address=argument_filters.get("address"),
        argument_filters=argument_filters,
        fromBlock=from_block,
        toBlock=to_block
    )

    logger.debug(f"Querying eth_getLogs with the following parameters: {event_filter_params}")

    # Call JSON-RPC API on your Ethereum node.
    # get_logs() returns raw AttributedDict entries
    logs = w3.eth.get_logs(event_filter_params)

    # Convert raw binary data to Python proxy objects as described by ABI
    all_events = []
    for log in logs:
        # Convert raw JSON-RPC log result to human readable event by using ABI data
        # More information how processLog works here
        # https://github.com/ethereum/web3.py/blob/fbaf1ad11b0c7fac09ba34baff2c256cffe0a148/web3/_utils/events.py#L200
        evt = get_event_data(codec, abi, log)
        # Note: This was originally yield,
        # but deferring the timeout exception caused the throttle logic not to work
        all_events.append(evt)
    return all_events
