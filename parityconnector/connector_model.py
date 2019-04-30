import asyncio
import time
from collections import OrderedDict
from binascii import hexlify, unhexlify

class Cache():
    def __init__(self, max_size=1000):
        self._store = OrderedDict()
        self._max_size = max_size
        self.clear_tail = False
        self._requests = 0
        self._hit = 0

    def set(self, key, value):
        self._check_limit()
        self._store[key] = value

    def _check_limit(self):
        if len(self._store) >= self._max_size:
            self.clear_tail = True
        if self.clear_tail:
            if len(self._store) >= int(self._max_size * 0.75):
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
# tx_cache.set(hash,tx_cache_data)
# pending_cache.set(hash,tx_cache_data)


# block_cache.set(hash, height)
# ------------------






def get_last_block_height(app):
    block_height=-1
    for i in app.block_cache._store:
        if app.block_cache._store[i] >= block_height:
            block_height=app.block_cache._store[i]
    return block_height if block_height!=-1 else None

def get_last_block_hash(app):
    block_height=0
    binary_block_hash=None
    for i in app.block_cache._store:
        if app.block_cache._store[i] >= block_height:
            block_height=app.block_cache._store[i]
            binary_block_hash =i
    return binary_block_hash









def block_hash_by_height(app,block_height):
    binary_block_hash=None
    if block_height == app.last_inserted_block[1]:
        binary_block_hash=app.last_inserted_block[0]
    else:
        for i in app.block_cache._store:
            if app.block_cache._store[i] == block_height:
                binary_block_hash = i
    return binary_block_hash





def clear_expired_tx(app,unconfirmed_exp = 12):
        expired_hash_list=[]
        expired_timestamp=int(time.time()) - 60*60*unconfirmed_exp
        for i in app.pending_cache._store:
            if app.pending_cache._store[i][1] <= expired_timestamp:
                expired_hash_list.append(i)
        if expired_hash_list:
                [app.pending_cache.pop(tx_hash) for tx_hash in expired_hash_list]
        return expired_hash_list




def remove_orphan(app,block_height,binary_block_hash):
    tx_hash_list=[]
    for i in app.tx_cache._store:
        if app.tx_cache._store[i][0]==block_height:
            tx_cache=(-1,app.tx_cache._store[i][1])
            app.pending_cache.set(i,tx_cache)
            tx_hash_list.append(i)
            app.log.warning('orphan remove tx hash 0x%s tx cache %s' %(hexlify(i),tx_cache))
    for i in tx_hash_list:
        app.tx_cache.pop(i)
    app.block_cache.pop(binary_block_hash)






