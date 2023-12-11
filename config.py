####################
# Pools 
####################
CURVE_TOKEN_ADDRESS = "0x06325440D014e39736583c165C2963BA99fAf14E"
CURVE_POOL_ADDRESS = "0xdc24316b9ae028f1497c275eb9192a3ea0f67022"

# Maverick uses a proxy contract
MAVERICK_TOKEN_ADDRESS = "0xf951e335afb289353dc249e82926178eac7ded78" # implementation contract: 0xdda46bf18eeb3e06e2f12975a3a184e40581a72f
MAVERICK_POOL_ADDRESS = "0x0ce176e1b11a8f88a4ba2535de80e81f88592bad"

####################
# ABIs
####################

CURVE_TOKEN_ABI_PATH = "abis/curve_pool_token_abi.json"
CURVE_POOL_ABI_PATH = "abis/curve_pool_contract_abi.json"

MAVERICK_TOKEN_ABI_PATH = "abis/maverick_pool_token_abi.json"
MAVERICK_CONTRACT_ABI_PATH = "abis/maverick_pool_contract_abi.json"

####################
# Files to store
####################

CURVE_TOKEN_TRANSFERS_FILENAME = "data/curve/curve_token_Transfers.csv"
CURVE_POOL_ADDLIQUIDITY_FILENAME = "data/curve/curve_pool_AddLiquidity.csv"
CURVE_POOL_REMOVELIQUIDITY_FILENAME = "data/curve/curve_pool_RemoveLiquidity.csv"
CURVE_POOL_REMOVELIQUIDITYONE_FILENAME = "data/curve/curve_pool_RemoveLiquidityOne.csv"
CURVE_POOL_REMOVELIQUIDITYIMBALANCE_FILENAME = "data/curve/curve_pool_RemoveLiquidityImbalance.csv"
CURVE_POOL_TOKENSWAPS_FILENAME = "data/curve/curve_pool_TokenExchangeSwaps.csv"

MAVERICK_TOKEN_DEPOSITS_FILENAME = "data/maverick/maverick_token_ETHDepositReceived.csv"
MAVERICK_TOKEN_WITHDRAWALS_FILENAME = "data/maverick/maverick_token_ETHWithdrawn.csv"
MAVERICK_TOKEN_REPRICE_FILENAME = "data/maverick/maverick_token_Reprice.csv"
MAVERICK_TOKEN_TRANSFER_FILENAME = "data/maverick/maverick_token_Transfer.csv"

MAX_BATCH=1000
