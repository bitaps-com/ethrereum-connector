from collections import OrderedDict
import functools

DEFAULT_CONFIRMED_TX_CACHE_SIZE = 50000
DEFAULT_BLOCK_CACHE_SIZE = 200
DEFAULT_BLOCK_PRELOAD_CACHE_SIZE = 50000

DEFAULT_EXPIRED_PENDING_TX_TIME = 43200 #12 hours

DEFAULT_RPC_TIMEOUT = 60
DEFAULT_BLOCK_HANDLER_TIMEOUT = 120

CLIENTS={"geth":{"trace":False, "getBlockReceipts_method":None},
         "nethermind":{"trace":True, "getBlockReceipts_method":"parity_getBlockReceipts"},
         "erigon":{"trace":True, "getBlockReceipts_method":None}}

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