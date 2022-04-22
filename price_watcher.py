import math

from web3 import Web3
from web3.contract import Contract

class PriceWatcher:
    def __init__(self, w3: Web3, contract: Contract):
        self.contract = contract
        self.w3 = w3

    @staticmethod
    def from_wei_with_decimals(amount, decimals):
        return amount / math.pow(10, decimals)

    def watch(self, dex_address, quote_token_address, quote_decimals, base_token_address, base_decimals):

        ask = self.contract.functions.getOffers(dex_address,quote_token_address,base_token_address).call()

        '''id = ask[0][0] # id'''
        quote = self.from_wei_with_decimals(ask[1][0], quote_decimals) # payAmts
        base = self.from_wei_with_decimals(ask[2][0], base_decimals) # buyAmts
        '''owner = ask[3][0] # owners'''
        '''timestamp = ask[4][0] # timestamps'''
        price = quote / base

        return price
