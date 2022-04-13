# Tx Watcher

## Setup

Config on tx_watcher.py

#### Address received the token(e.g Diabolo Cold Wallet Address):
- DEPOSIT_ADDRESS = "0x19CC89bF8f4F67E9255B574E65fADAa8e34a7667"

#### Node API url(e.g infura url):
- API_URL = "https://polygon-mainnet.infura.io/v3/APIKEY"

#### Address of Smart Contract Token(e.g Diabolo Token Address on polygon):
- SC_ADDRESS = "0x74ba6A10978F643A84C0b37fCB599081079811cB"

#### Maximum log limit in one API call to node(e.g 3500 limit to infura polygon call):
- MAX_LOG_LIMIT = 3499

#### Minimum confirmation(e.g 12 confirmation needed to save receipt):
- MIN_CONFIRMATION = 12

#### Refresh interval to run watcher of new blocks(e.g 60 seconds):
- REFRESH_INTERVAL = 60

#### Blockchain name(e.g Polygon):
- BLOCKCHAIN_NAME = "polygon"

### Run

`python3 tx_watcher.py`
