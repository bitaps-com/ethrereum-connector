from collections import OrderedDict
import functools
from binascii import hexlify, unhexlify

DEFAULT_CONFIRMED_TX_CACHE_SIZE = 10000
DEFAULT_BLOCK_CACHE_SIZE = 200
DEFAULT_BLOCK_PRELOAD_CACHE_SIZE = 50000

DEFAULT_EXPIRED_PENDING_TX_TIME = 43200 #12 hours

DEFAULT_RPC_TIMEOUT = 60
DEFAULT_BLOCK_HANDLER_TIMEOUT = 120

CLIENTS={"geth":{"trace":False, "getBlockReceipts":{"method":"eth_getBlockReceipts", "params":"height"}},
         "nethermind":{"trace":True, "getBlockReceipts":{"method":"eth_getBlockReceipts", "params":"height"}},
         "erigon":{"trace":True, "getBlockReceipts":{"method":"erigon_getBlockReceiptsByBlockHash", "params":"hash"}}, #>=v2.32.0
         "tron":{"trace":False, "getBlockReceipts":{"method":None, "params":"height"}}}

class Cache():
    def __init__(self, max_size=0):
        self._store = OrderedDict()
        self._max_size = max_size
        self.clear_tail = False
        self._requests = 0
        self._hit = 0

    def set(self, key, value):
        if self._max_size>0: self._check_limit()
        self._store[key] = value

    def _check_limit(self):
        if len(self._store) >= self._max_size * 1.1:
            self.clear_tail = True
        if self.clear_tail:
            if len(self._store) >= int(self._max_size):
                [self._store.popitem(last=False) for i in range(20)]
            else:
                self.clear_tail = False

    def get(self, key):
        self._requests += 1
        try:
            i = self._store[key]
            self._hit += 1
            return i
        except:
            return None

    def pop(self, key):
        self._requests += 1
        try:
            data = self._store[key]
            del self._store[key]
            self._hit += 1
            return data
        except:
            return None

    def len(self):
        return len(self._store)

    def hitrate(self):
        if self._requests:
            return self._hit / self._requests
        else:
            return 0


# Cache structures
# ------------------
# tx_cache_data=(height,last_timestamp)
# confirmed_tx_cache.set(hash,tx_cache_data)
# pending_tx_cache.set(hash,tx_cache_data)


# block_cache.set(hash, height)
# ------------------


def transaction(func):
    @functools.wraps(func)
    async def wrapper(*args, db_pool, **kwargs):
        if db_pool:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    await func(*args, conn=conn, **kwargs)
        else:
            await func(*args, conn=None, **kwargs)
    return wrapper

def hex_to_bytes(hex_type):
        return unhexlify(hex_type[2:]) if hex_type.startswith("0x") else unhexlify(hex_type)