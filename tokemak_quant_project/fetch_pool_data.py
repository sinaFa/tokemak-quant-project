import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

from config import *
from utilities import load_abi

curve_filenames = [
    CURVE_TOKEN_TRANSFERS_FILENAME,
    CURVE_POOL_ADDLIQUIDITY_FILENAME,
    CURVE_POOL_REMOVELIQUIDITY_FILENAME,
    CURVE_POOL_REMOVELIQUIDITYONE_FILENAME,
    CURVE_POOL_REMOVELIQUIDITYIMBALANCE_FILENAME,
    CURVE_POOL_TOKENSWAPS_FILENAME,
]

maverick_filenames = [
    MAVERICK_TOKEN_DEPOSITS_FILENAME,
    MAVERICK_TOKEN_WITHDRAWALS_FILENAME,
    MAVERICK_TOKEN_REPRICE_FILENAME,
    MAVERICK_TOKEN_TRANSFER_FILENAME,
]


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class PoolDataFetcher:
    """
    This class is responsible for fetching and processing data from a given DeFi pool contract.
    """

    def __init__(self, web3, pool_address, abi):
        """
        Initializes the PoolDataFetcher with a Web3 instance, pool address, and ABI.

        :param web3: A Web3 instance connected to an Ethereum node.
        :param pool_address: The Ethereum address of the DeFi pool contract.
        :param abi: The ABI of the DeFi pool contract.
        """
        self.w3 = web3
        self.pool_address = web3.to_checksum_address(pool_address)
        self.abi = abi
        self.contract = web3.eth.contract(address=self.pool_address, abi=self.abi)

    def fetch_curve_token_data(self, start_block, end_block):
        """
        Fetches all Transfer events for the contract between the specified block range.

        :param start_block: The starting block number for the query.
        :param end_block: The ending block number for the query.
        :return: A list of Transfer event data.
        """
        try:
            logging.info(f"\t\tFetching token Transfers events from Curve pool")
            transfers = self.contract.events.Transfer.getLogs(fromBlock=start_block)
            return transfers

        except Exception as e:
            logging.error(f"Error fetching Curve token data: {e}")

    def fetch_curve_pool_data(self, start_block, end_block):
        """
        Fetches various metrics from the pool contract over a specified range of blocks.

        :param start_block: The starting block number for the query.
        :param end_block: The ending block number for the query.
        :return: A dictionary of pool metrics such as TVL, volume, etc.
        """
        try:
            logging.info(f"\t\tFetching Add liquidity events from Curve pool")
            add_liquidity = self.contract.events.AddLiquidity.getLogs(
                fromBlock=start_block
            )

            logging.info(f"\t\tFetching Remove liquidity events from Curve pool")
            remove_liquidity = self.contract.events.RemoveLiquidity.getLogs(
                fromBlock=start_block
            )

            logging.info(f"\t\tFetching Remove liquidity One events from Curve pool")
            remove_liquidity_one = self.contract.events.RemoveLiquidityOne.getLogs(
                fromBlock=start_block
            )

            logging.info(
                f"\t\tFetching Remove liquidity Imbalance events from Curve pool"
            )
            remove_liquidity_imbalance = (
                self.contract.events.RemoveLiquidityImbalance.getLogs(
                    fromBlock=start_block
                )
            )

            logging.info(f"\t\tFetching Token Exchange swap events from Curve pool")
            token_exchange = self.contract.events.TokenExchange.getLogs(
                fromBlock=start_block
            )
            # print(f"virtual prices: {self.contract.functions.get_virtual_price().call()}")
            curve_contract_data = [
                add_liquidity,
                remove_liquidity,
                remove_liquidity_one,
                remove_liquidity_imbalance,
                token_exchange,
            ]
            return curve_contract_data

        except Exception as e:
            logging.error(f"Error fetching Curve pool data: {e}")

    def fetch_maverick_token_data(self, start_block, end_block):
        """
        Fetches all Transfer events for the contract between the specified block range.

        :param start_block: The starting block number for the query.
        :param end_block: The ending block number for the query.
        :return: A list of Transfer event data.
        """
        try:
            deposits = self.contract.events.ETHDepositReceived.getLogs(
                fromBlock=start_block
            )
            withdrawals = self.contract.events.ETHWithdrawn.getLogs(
                fromBlock=start_block
            )
            reprice = self.contract.events.Reprice.getLogs(fromBlock=start_block)
            transfers = self.contract.events.Transfer.getLogs(fromBlock=start_block)
            maverick_token_data = [deposits, withdrawals, reprice, transfers]
            return maverick_token_data

        except Exception as e:
            logging.error(f"Error fetching Maverick pool data: {e}")

    def fetch_maverick_pool_data(self, start_block, end_block):
        """
        Fetches various metrics from the pool contract over a specified range of blocks.

        :param start_block: The starting block number for the query.
        :param end_block: The ending block number for the query.
        :return: A dictionary of pool metrics such as TVL, volume, etc.
        """
        try:
            pass
        except Exception as e:
            logging.error(f"Error fetching Maverick pool data: {e}")


def fetch_and_store_curve_data(w3, start_block, current_block, query_round):
    """
    Fetches and stores data for the Curve stETH/ETH pool.

    :param w3: A Web3 instance connected to an Ethereum node.
    :param start_block: The starting block number.
    :param current_block: The current block number.
    :param Sending queries to Alchemy in batches to not get rejected
    """

    curve_token_fetcher = PoolDataFetcher(
        w3,
        CURVE_TOKEN_ADDRESS,
        load_abi(CURVE_TOKEN_ABI_PATH),
    )

    curve_pool_fetcher = PoolDataFetcher(
        w3,
        CURVE_POOL_ADDRESS,
        load_abi(CURVE_POOL_ABI_PATH),
    )

    logging.info("\tGetting Curve steCRV token data")
    curve_token_data = curve_token_fetcher.fetch_curve_token_data(
        start_block, current_block
    )

    logging.info("\tGetting Curve stETH/ETH pool data")
    curve_contract_data = curve_pool_fetcher.fetch_curve_pool_data(
        start_block, current_block
    )

    logging.info(f"\tStoring data for Curve stETH/ETH pool for batch {query_round}")
    store_data_curve(curve_token_data, curve_contract_data, query_round)


def fetch_and_store_maverick_data(w3, start_block, current_block, query_round):
    """
    Fetches and stores data for the Maverick swETH/ETH Pool.

    :param w3: A Web3 instance connected to an Ethereum node.
    :param start_block: The starting block number.
    :param current_block: The current block number.
    """

    maverick_token_fetcher = PoolDataFetcher(
        w3,
        MAVERICK_TOKEN_ADDRESS,  # Proxy contract
        load_abi(MAVERICK_TOKEN_ABI_PATH),  # Implementation contract ABI
    )
    maverick_pool_fetcher = PoolDataFetcher(
        w3,
        MAVERICK_POOL_ADDRESS,
        load_abi(MAVERICK_CONTRACT_ABI_PATH),
    )

    # Fetch data for Maverick pools
    logging.info("\tGetting Maverick swETH token data")
    maverick_token_data = maverick_token_fetcher.fetch_maverick_token_data(
        start_block, current_block
    )
    logging.info("\tGetting Maverick swETH-ETH pool data")
    maverick_contract_data = maverick_pool_fetcher.fetch_maverick_pool_data(
        start_block, current_block
    )

    # Store the fetched data for later analysis
    logging.info(f"\tStoring data for Maverick swETH/ETH pool for batch {query_round}")
    store_data_maverick(maverick_token_data, maverick_contract_data, query_round)


def store_data_curve(token_data, contract_data, batch_n):
    """
    Stores the fetched data into a JSON file for later analysis.

    :param token_data: Data related to the pools.
    :param contract_data: Data related to the Curve pool contract.
    :param identifier number of the Alchemy batch
    """
    if token_data is not None:
        processed_data = []
        # Add tokken transfers
        for data in token_data:
            event_data = {
                "from": data.args._from,
                "to": data.args._to,
                "vaue": data.args._value,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{CURVE_TOKEN_TRANSFERS_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tData for Curve pool tokens stored in {CURVE_TOKEN_TRANSFERS_FILENAME}_{batch_n}."
        )
    else:
        logging.warning(f"No data to store on {CURVE_TOKEN_TRANSFERS_FILENAME}")

    if contract_data is not None:
        ## Add Liquidity Data
        processed_data = []
        for data in contract_data[0]:
            event_data = {
                "provider": data.args.provider,
                "token_amounts_a": data.args.token_amounts[0],
                "token_amounts_b": data.args.token_amounts[1],
                "fees_a": data.args.fees[0],
                "fees_b": data.args.fees[1],
                "invariant": data.args.invariant,
                "token_supply": data.args.token_supply,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{CURVE_POOL_ADDLIQUIDITY_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tAdd Liquidity Data for Curve pool contract stored in {CURVE_POOL_ADDLIQUIDITY_FILENAME}_{batch_n}."
        )

        ## Remove Liquidity Data
        processed_data = []
        for data in contract_data[1]:
            event_data = {
                "provider": data.args.provider,
                "token_amounts_a": data.args.token_amounts[0],
                "token_amounts_b": data.args.token_amounts[1],
                "fees_a": data.args.fees[0],
                "fees_b": data.args.fees[1],
                "token_supply": data.args.token_supply,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{CURVE_POOL_REMOVELIQUIDITY_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tRemove Liquidity Data for Curve pool contract stored in {CURVE_POOL_REMOVELIQUIDITY_FILENAME}."
        )

        ## Remove Liquidity One
        processed_data = []
        for data in contract_data[2]:
            event_data = {
                "provider": data.args.provider,
                "token_amount": data.args.token_amount,
                "coin_amount": data.args.coin_amount,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{CURVE_POOL_REMOVELIQUIDITYONE_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tRemove Liquidity One Data for Curve pool contract stored in {CURVE_POOL_REMOVELIQUIDITYONE_FILENAME}_{batch_n}."
        )

        ## Remove Liquidity Imbalance
        processed_data = []
        for data in contract_data[3]:
            event_data = {
                "provider": data.args.provider,
                "token_amounts_a": data.args.token_amounts[0],
                "token_amounts_b": data.args.token_amounts[1],
                "invariant": data.args.invariant,
                "token_supply": data.args.token_supply,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{CURVE_POOL_REMOVELIQUIDITYIMBALANCE_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tRemove Liquidity Imbalance Data for Curve pool contract stored in {CURVE_POOL_REMOVELIQUIDITYIMBALANCE_FILENAME}_{batch_n}."
        )

        ## Token Exchange swaps
        processed_data = []
        for data in contract_data[4]:
            event_data = {
                "buyer": data.args.buyer,
                "sold_id": data.args.sold_id,
                "tokens_sold": data.args.tokens_sold,
                "bought_id": data.args.bought_id,
                "tokens_bought": data.args.tokens_bought,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(CURVE_POOL_TOKENSWAPS_FILENAME, index=False)
        logging.info(
            f"\t\tToken Exchange Swaps Data for Curve pool contract stored in {CURVE_POOL_TOKENSWAPS_FILENAME}_{batch_n}."
        )
    else:
        logging.warning(f"No data to store for Curve")


def store_data_maverick(token_data, contract_data, batch_n):
    """
    Stores the fetched data into a JSON file for later analysis.

    :param token_data: Data related to the pools.
    :param contract_data: Data related to the Curve pool contract.
    """
    if token_data is not None:
        # Add ETHDepositReceived events
        processed_data = []
        for data in token_data[0]:
            event_data = {
                "from": data.args["from"],
                "referral": data.args.referral,
                "amount": data.args.amount,
                "swETHMinted": data.args.swETHMinted,
                "newTotalETHDeposited": data.args.newTotalETHDeposited,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{MAVERICK_TOKEN_DEPOSITS_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tDeposit Data for Maverick tokens stored in {MAVERICK_TOKEN_DEPOSITS_FILENAME}_{batch_n}."
        )
        # Add ETHWithdrawn events
        processed_data = []
        for data in token_data[1]:
            event_data = {
                "to": data.args.to,
                "swETHBurned": data.args.swETHBurned,
                "ethReturned": data.args.ethReturned,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{MAVERICK_TOKEN_WITHDRAWALS_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tWithdrawal Data for Maverick tokens stored in {MAVERICK_TOKEN_WITHDRAWALS_FILENAME}_{batch_n}."
        )
        # Add Reprice events
        processed_data = []
        for data in token_data[2]:
            event_data = {
                "newEthReserves": data.args.newEthReserves,
                "newSwETHToETHRate": data.args.newSwETHToETHRate,
                "nodeOperatorRewards": data.args.nodeOperatorRewards,
                "swellTreasuryRewards": data.args.swellTreasuryRewards,
                "totalETHDeposited": data.args.totalETHDeposited,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{MAVERICK_TOKEN_REPRICE_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tWithdrawal Data for Maverick tokens stored in {MAVERICK_TOKEN_REPRICE_FILENAME}_{batch_n}."
        )
        # Add Transfer events
        processed_data = []
        for data in token_data[3]:
            event_data = {
                "from": data.args["from"],
                "to": data.args["to"],
                "value": data.args.value,
                "event": data.event,
                "logIndex": data.logIndex,
                "transactionIndex": data.transactionIndex,
                "transactionHash": data.transactionHash.hex(),
                "address": data.address,
                "blockHash": data.blockHash.hex(),
                "blockNumber": data.blockNumber,
            }
            processed_data.append(event_data)

        pd.DataFrame(processed_data).to_csv(
            f"{MAVERICK_TOKEN_TRANSFER_FILENAME}_{batch_n}", index=False
        )
        logging.info(
            f"\t\tWithdrawal Data for Maverick tokens stored in {MAVERICK_TOKEN_TRANSFER_FILENAME}_{batch_n}."
        )
    else:
        logging.warning(f"No data to store for Maverick tokens")

    if contract_data is not None:
        processed_data = []
        #### TBD ....
    else:
        logging.warning(f"No data to store for Maverick pool ")


def getBlockDate(filenames, w3, project, merge=False, max_files=MAX_BATCH):
    """
     Parameters:
    :param filenames (list of str): A list of file paths to CSV files. Each CSV file must have a 'blockNumber' column.
    :param w3(Web3): An instance of Web3 connected to an Ethereum node. This is used to fetch block timestamps.
    :param merge: A boolean indicating if Alchemy requests have been split in 2k blocks batches
    :param max_files: The number of files to process

    Returns:
    None: The function does not return any value. Instead, it updates the original CSV files and creates a new file.

    Note:
    - This function can be time-consuming for a large number of unique block numbers, as it makes a network request for each block.
    - Ensure that the Web3 instance is correctly configured and connected to an Ethereum node.
    - The new 'block_date' column in the updated CSV files will contain the date and time of the block in 'YYYY-MM-DD HH:MM:SS' format.

    """
    all_block_numbers = pd.Series(dtype=int)
    for filename in filenames:
        if merge:
            for i in range(maverick):
                try:
                    df = pd.read_csv(f"{filename}_{i}")
                    all_block_numbers = pd.concat(
                        [all_block_numbers, df["blockNumber"]], ignore_index=True
                    )
                except Exception as e:
                    logging.error(f"{filename}_{i} : {e}")
        else:
            df = pd.read_csv(filename)
            all_block_numbers = pd.concat(
                [all_block_numbers, df["blockNumber"]], ignore_index=True
            )

    unique_blocks = all_block_numbers.unique()

    logging.info("Fetch timestamps for each unique block number")
    block_dates = {}
    for block_number in unique_blocks:
        try:
            timestamp = w3.eth.get_block(int(block_number))["timestamp"]
            block_dates[block_number] = datetime.fromtimestamp(timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except Exception as e:
            logging.error(
                f"Error fetching block data for block number {block_number}: {e}"
            )

    block_date_df = pd.DataFrame(
        list(block_dates.items()), columns=["blockNumber", "block_date"]
    )
    block_date_df.to_csv(f"blockNumberDates_{project}.csv", index=False)

    logging.info("Storing block and date results")
    for filename in filenames:
        if merge:
            for i in range(MAX_BATCH):
                try:
                    df = pd.read_csv(f"{filename}_{i}")
                    df["block_date"] = df["blockNumber"].map(block_dates)
                    df.to_csv(f"{filename}_{i}", index=False)
                except Exception as e:
                    logging.error(f"{filename}_{i} : {e}")
        else:
            df = pd.read_csv(f"{filename}_{i}")
            df["block_date"] = df["blockNumber"].map(block_dates)
            df.to_csv(filename, index=False)

    if project == "curve":
        filenames = curve_filenames
    elif project == "maverick":
        filenames = maverick_filenames

    dataframes = []
    for filename in filenames:
        if merge:
            for i in range(max_files):
                try:
                    df = pd.read_csv(f"{filename}_{i}")
                    dataframes.append(df)
                except Exception as e:
                    print(f"File not found: {e}")
            concatenated_df = pd.concat(dataframes, ignore_index=True)
            concatenated_df.to_csv(filename, index=False)


def main():
    load_dotenv()

    w3 = Web3(Web3.HTTPProvider(os.getenv("PROVIDER_URL")))

    current_block = w3.eth.block_number

    for i in range(MAX_BATCH):
        # Alchemy accepts up to 2000 blocks per request
        start_block = current_block - 2000

        logging.info(
            f"Getting data for Curve stETH/ETH pool - Block {start_block} to {current_block} - Run {i}"
        )
        fetch_and_store_curve_data(w3, start_block, current_block, i)

        logging.info(
            f"Getting data for Maverick swETH/ETH pool - Block {start_block} to {current_block} - Run {i}"
        )
        fetch_and_store_maverick_data(w3, start_block, current_block, i)

        current_block = start_block
        time.sleep(5)

    logging.info("Getting corresponding dates for all blocks stored")
    getBlockDate(curve_filenames, w3, "Curve", merge=True)
    getBlockDate(maverick_filenames, w3, "Maverick", merge=True)

    logging.info("DONE fetching data.")


if __name__ == "__main__":
    main()


# Run using: poetry run python tokemak_quant_project/fetch_pool_data
