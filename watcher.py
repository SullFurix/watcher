import datetime
import time
import logging
import asyncio
from price_watcher import PriceWatcher
from transfer_watcher import TransferWatcher, TransferWatcherState
from web3 import Web3
from web3.datastructures import AttributeDict
from web3.middleware import geth_poa_middleware

if __name__ == "__main__":
    import json
    from web3.providers.rpc import HTTPProvider

    INFURA_KEY = " :) " # node key
    API_URL = "https://polygon-mainnet.infura.io/v3/%s" % (INFURA_KEY) # node url
    BLOCKCHAIN_NAME = "polygon" # blockchain name for file system
    SC_ADDRESS = "0x74ba6A10978F643A84C0b37fCB599081079811cB" # token smart contract address
    DEPOSIT_ADDRESS = "0x19CC89bF8f4F67E9255B574E65fADAa8e34a7667" # address received the token

    MULTICALL_DEX_ADDRESS = "0x198B38312A9667db4eC4f11EE6Fc9b59132B3956" # contract to return multiple order on order book
    DEX_ADDRESS = "0x658aF64B59974968012EE2f9B529eF39c6F91787" # dex contract
    QUOTE_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174" # quote token address for dex e.g : USDC
    QUOTE_DECIMALS = 6 # decimals of quote token
    BASE_ADDRESS = "0x74ba6A10978F643A84C0b37fCB599081079811cB" # base token address for dex e.g : DCASH
    BASE_DECIMALS = 10 # decimals of base token

    MAX_LOG_LIMIT = 3499 # maximum log limit in one api call to node
    MIN_CONFIRMATION = 126 # minimum 12 confirmations
    REFRESH_INTERVAL = 60 # 60 seconds
    SAVE_INTERVAL = 60 # Save the database file for every minute


    # Reduced ERC-20 ABI, only Transfer event
    ABI = """[
        {
            "anonymous": false,
            "inputs": [
                {
                    "indexed": true,
                    "name": "from",
                    "type": "address"
                },
                {
                    "indexed": true,
                    "name": "to",
                    "type": "address"
                },
                {
                    "indexed": false,
                    "name": "value",
                    "type": "uint256"
                }
            ],
            "name": "Transfer",
            "type": "event"
        }
    ]
    """

    MULTICALL_DEX_ABI = """[
        {
            "constant": true,
            "inputs": [
                {
                    "name": "dex",
                    "type": "address"
                },
                {
                    "name": "payToken",
                    "type": "address"
                },
                {
                    "name": "buyToken",
                    "type": "address"
                }
            ],
            "name": "getOffers",
            "outputs": [
                {
                    "name": "ids",
                    "type": "uint256[100]"
                },
                {
                    "name": "payAmts",
                    "type": "uint256[100]"
                },
                {
                    "name": "buyAmts",
                    "type": "uint256[100]"
                },
                {
                    "name": "owners",
                    "type": "address[100]"
                },
                {
                    "name": "timestamps",
                    "type": "uint256[100]"
                }
            ],
            "payable": false,
            "stateMutability": "view",
            "type": "function"
        }
    ]
    """

    class JSONifiedState(TransferWatcherState):
        def __init__(self):
            self.state = None
            self.fname = BLOCKCHAIN_NAME+"_state.json"
            self.last_save = 0

        def reset(self):
            self.state = {
                "last_watched_block": 0,
                "blocks": {},
            }

        def restore(self):
            try:
                self.state = json.load(open(self.fname, "rt"))
                print(f"Restored the state, previously {self.state['last_watched_block']} blocks have been watched")
            except (IOError, json.decoder.JSONDecodeError):
                print("State starting from scratch")
                self.reset()

        def save(self):
            with open(self.fname, "wt") as f:
                json.dump(self.state, f)
            self.last_save = time.time()

        #
        # TransferWatcherState methods implemented below
        #

        def get_last_watched_block(self):
            return self.state["last_watched_block"]

        def delete_data(self, since_block):
            for block_num in range(since_block, self.get_last_watched_block()):
                if block_num in self.state["blocks"]:
                    del self.state["blocks"][block_num]

        def start_chunk(self, block_number, chunk_size):
            pass

        def end_chunk(self, block_number):
            # Next time the transfer watcher is started we will resume from this block
            self.state["last_watched_block"] = block_number

            # Save the database file for every minute
            if time.time() - self.last_save > SAVE_INTERVAL:
                self.save()

        def process_event(self, block_when: datetime.datetime, event: AttributeDict) -> str:
            # Events are keyed by their transaction hash and log index
            # One transaction may contain multiple events
            # and each one of those gets their own log index

            # event_name = event.event # "Transfer"
            log_index = event.logIndex  # Log index within the block
            # transaction_index = event.transactionIndex  # Transaction index within the block
            txhash = event.transactionHash.hex()  # Transaction hash
            block_number = event.blockNumber

            # Convert ERC-20 Transfer event to our internal format
            args = event["args"]
            transfer = {
                "from": args["from"],
                "to": args.to,
                "value": args.value,
                "timestamp": block_when.isoformat(),
            }

            if(transfer["to"] == DEPOSIT_ADDRESS):

                print(txhash)
                print(block_number)
                print(transfer["from"])

                # Create empty dict as the block that contains all transactions by txhash
                if block_number not in self.state["blocks"]:
                    self.state["blocks"][block_number] = {}

                block = self.state["blocks"][block_number]
                if txhash not in block:
                    # We have not yet recorded any transfers in this transaction
                    # (One transaction may contain multiple events if executed by a smart contract).
                    # Create a tx entry that contains all events by a log index
                    self.state["blocks"][block_number][txhash] = {}

                # Record ERC-20 transfer in our database
                self.state["blocks"][block_number][txhash][log_index] = transfer

                # Return a pointer that allows us to look up this event later if needed
                return f"{block_number}-{txhash}-{log_index}"

    def new_price(price):
        print(price) # ?? toi de jouer !!

    def run():
        # Enable logs to the stdout.
        # DEBUG is very verbose level
        logging.basicConfig(level=logging.INFO)

        provider = HTTPProvider(API_URL)

        # Remove the default JSON-RPC retry middleware
        # as it correctly cannot handle eth_getLogs block range
        # throttle down.
        provider.middlewares.clear()

        w3 = Web3(provider)

        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        multicall_dex_abi = json.loads(MULTICALL_DEX_ABI)
        MULTICALL_DEX = w3.eth.contract(MULTICALL_DEX_ADDRESS, abi=multicall_dex_abi)

        # Prepare stub ERC-20 contract object
        abi = json.loads(ABI)
        ERC20 = w3.eth.contract(abi=abi)

        price_watcher = PriceWatcher(
            w3=w3,
            contract=MULTICALL_DEX
        )

        price = price_watcher.watch(DEX_ADDRESS, QUOTE_ADDRESS, QUOTE_DECIMALS, BASE_ADDRESS, BASE_DECIMALS)

        new_price(price)

        # Restore/create our persistent state
        state = JSONifiedState()
        state.restore()

        # chain_id: int, w3: Web3, abi: dict, state: TransferWatcherState, events: List, filters: {}, max_chunk_watch_size: int=10000
        transfer_watcher = TransferWatcher(
            w3=w3,
            contract=ERC20,
            state=state,
            events=[ERC20.events.Transfer],
            filters={"address": SC_ADDRESS},
            # How many maximum blocks at the time we request from JSON-RPC
            # and we are unlikely to exceed the response size limit of the JSON-RPC server
            max_chunk_watch_size=MAX_LOG_LIMIT
        )

        # Assume we might have watched the blocks all the way to the last Ethereum block
        # that mined a few seconds before the previous watch run ended.
        # Because there might have been a minor Etherueum chain reorganisations
        # since the last watch ended, we need to discard
        # the last few blocks from the previous watch results.
        chain_reorg_safety_blocks = MIN_CONFIRMATION
        transfer_watcher.delete_potentially_forked_block_data(state.get_last_watched_block() - chain_reorg_safety_blocks)

        # watch from [last block watched] - [latest ethereum block]
        # Note that our chain reorg safety blocks cannot go negative

        start_block = max(state.get_last_watched_block() - chain_reorg_safety_blocks, 0)

        end_block = transfer_watcher.get_suggested_watch_end_block()
        blocks_to_watch = end_block - start_block

        print(f"Watching events from blocks {start_block} - {end_block}")

        # Render a progress bar in the console
        start = time.time()

        # Run the watch
        result, total_chunks_watched = transfer_watcher.watch(start_block, end_block)

        state.save()
        duration = time.time() - start
        print(f"Watched total {len(result)} Transfer events, in {duration} seconds, total {total_chunks_watched} chunk watchs performed")


    async def log_loop(poll_interval):
        while True:
            run()
            await asyncio.sleep(poll_interval)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(REFRESH_INTERVAL)))
    finally:
        # close loop to free up system resources
        loop.close()
