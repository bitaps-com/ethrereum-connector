import time

def get_last_block_height(app):
    return list(app.block_cache._store.values())[-1] if app.block_cache._store else -1

def get_last_block_hash(app):
    return list(app.block_cache._store.keys())[-1] if app.block_cache._store else None

def get_block_hash_by_height(app, block_height):
    try:
        index = list(app.block_cache._store.values()).index(block_height)
    except:
        index = -1
    return list(app.block_cache._store.keys())[index] if index !=-1 else None

def get_expired_tx(pending_tx_cache, expired_time):
    pendings_expired_hash_list = []
    expired_timestamp = int(time.time()) - expired_time
    for key, value in pending_tx_cache._store.items():
        if value[1] <= expired_timestamp:
            pendings_expired_hash_list.append(key)
    return pendings_expired_hash_list

def remove_orphan(app,block_height,bin_block_hash):
    tx_hash_list = []
    for key, value in app.confirmed_tx_cache._store.items():
        if value[0]==block_height:
            tx_cache=(-1,value[1])
            tx_hash_list.append(key)
            app.pending_tx_cache.set(key, tx_cache)
    for key in tx_hash_list:
        app.confirmed_tx_cache.pop(key)
    app.block_cache.pop(bin_block_hash)

