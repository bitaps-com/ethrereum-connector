import asyncio
from binascii import hexlify, unhexlify
import time
from collections import OrderedDict
import traceback

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




async def get_last_tx_id(conn):
    stmt = await conn.prepare("SELECT id "
                              "FROM connector_transaction "
                              "ORDER BY id DESC LIMIT 1;")
    tx_id = await stmt.fetchval()
    return tx_id if tx_id else 0


async def get_last_block_id(conn):
    stmt = await conn.prepare("SELECT id "
                              "FROM connector_block "
                              "ORDER BY id DESC LIMIT 1;")
    block_id = await stmt.fetchval()
    return block_id if block_id else 0

async def get_last_block_height(conn):
    stmt = await conn.prepare("SELECT height FROM connector_block ORDER BY id DESC LIMIT 1;")
    height = await stmt.fetchval()
    return height


async def get_last_block_hash(conn):
    stmt = await conn.prepare("SELECT hash FROM connector_block ORDER BY id DESC LIMIT 1;")
    binary_block_hash = await stmt.fetchval()
    return binary_block_hash


async def get_tx_from_db(binary_tx_hash,conn):
    stmt = await conn.prepare("SELECT id,height FROM connector_transaction  "
                              "WHERE hash=$1;")
    result = await stmt.fetch(binary_tx_hash)
    return result


async def load_tx_cache(app, conn):
    stmt = await conn.prepare("SELECT hash, id, height FROM connector_transaction WHERE height IS NOT NULL  "
                              "ORDER BY height DESC LIMIT 50000;")
    rows = await stmt.fetch()
    [app.tx_cache.set(row["hash"], [row["id"],row["height"]]) for row in rows]


async def load_pending_cache(app, conn):
    stmt = await conn.prepare("SELECT hash, id FROM connector_transaction WHERE height IS NULL  "
                              "ORDER BY last_timestamp DESC LIMIT 100000;")
    rows = await stmt.fetch()
    [app.pending_cache.set(row["hash"], row["id"]) for row in rows]




async def load_block_cache(app, conn):
    stmt = await conn.prepare("SELECT hash, height FROM connector_block  "
                              "ORDER BY id DESC LIMIT 10000;")
    rows = await stmt.fetch()
    [app.block_cache.set(row["hash"], row["height"]) for row in rows]





async def block_height_by_hash(app,binary_block_hash, conn):
    if binary_block_hash == app.last_inserted_block[0]:
        block_height=app.last_inserted_block[1]
    else:
        stmt = await conn.prepare("SELECT height FROM connector_block "
                              "WHERE hash = $1 LIMIT 1;")
        block_height = await stmt.fetchval(binary_block_hash)
    return block_height

async def block_hash_by_height(app,block_height, conn):
    if block_height == app.last_inserted_block[1]:
        binary_block_hash=app.last_inserted_block[0]
    else:
        stmt = await conn.prepare("SELECT hash FROM connector_block WHERE height= $1 LIMIT 1;")
        binary_block_hash = await stmt.fetchval(block_height)
    return binary_block_hash

async def insert_new_tx_batch(batch, conn):
    await conn.copy_records_to_table('connector_transaction',
                                     columns=["id", "hash", "timestamp","last_timestamp","affected"],
                                     records=batch)


async def update_pending_tx(binary_tx_hash,last_seen_timestamp,conn):
    stmt = await conn.prepare("UPDATE connector_transaction SET last_timestamp = $1  WHERE hash=$2;")
    await stmt.fetch(last_seen_timestamp,binary_tx_hash)

async def unconfirmed_count(conn):
    stmt = await conn.prepare("SELECT count(id) FROM connector_transaction "
                              "WHERE height is NULL;")
    count = await stmt.fetchval()
    return count



async def clear_expired_tx(app,conn,unconfirmed_exp = 12):
    try:
        expired_list=[]
        stmt = await conn.prepare("SELECT id FROM connector_transaction "
                                      "WHERE "
                                     "height IS NULL "
                                     "AND last_timestamp < $1;")
        rows = await stmt.fetch(int(time.time()) - 60*60*unconfirmed_exp)
        id_list = [row for row in rows]
        if id_list:
            app.log.debug("clear %s expired transactions" % len(id_list))
            stmt = await conn.prepare("DELETE FROM connector_transaction "
                                      "WHERE id = ANY ($1) "
                                      "AND affected = 'B0' RETURNING hash;")
            expired_not_affected_list=await stmt.fetch(id_list)
            stmt = await conn.prepare("DELETE FROM connector_transaction "
                                      "WHERE id = ANY ($1) "
                                      "AND affected = 'B1' RETURNING hash;")
            expired_list=await stmt.fetch(id_list)
            if expired_not_affected_list:
                [app.pending_cache.pop(tx['hash']) for tx in expired_not_affected_list]
            if expired_list:
                [app.pending_cache.pop(tx['hash']) for tx in expired_list]
        return expired_list
    except:
        app.log.error(str(traceback.format_exc()))




async def clear_old_tx(app, conn, block_exp = 5170):
    height = app.last_block_height
    if height is not None:
        stmt = await conn.prepare("SELECT id FROM connector_transaction "
                                  "WHERE "
                                  "height < ($1);")
        rows = await stmt.fetch(height - block_exp)
        id_list = [row[0] for row in rows]
        if id_list:
            app.log.debug("clear %s old transactions" % len(id_list))
            stmt = await conn.prepare("DELETE FROM connector_transaction "
                                      "WHERE id = ANY ($1) RETURNING hash;")
            old_tx_list=await stmt.fetch(id_list)
            [app.tx_cache.pop(tx['hash']) for tx in old_tx_list]


async def remove_orphan(app,block_height,binary_block_hash, conn):
    i = app.tx_cache.len()
    for tx in app.tx_cache._store:
        if i < 1000:
            app.log.info("0x%s: %s" % (hexlify(tx).decode(), app.tx_cache.get(tx)))
        i -= 1
    tx_id_list=[]
    tx_hash_list=[]
    for i in app.tx_cache._store:
        if app.tx_cache._store[i][1]==block_height:
            app.pending_cache.set(i,app.tx_cache._store[i][0])
            tx_id_list.append(app.tx_cache._store[i][0])
            tx_hash_list.append(i)
    for i in tx_hash_list:
        app.tx_cache.pop(i)
    app.log.info(tx_id_list)
    app.block_cache.pop(binary_block_hash)
    stmt = await conn.prepare("DELETE FROM connector_block "
                              "WHERE hash = $1;")
    await stmt.fetch(binary_block_hash)
    return tx_id_list



async def insert_new_block(app, binary_block_hash, height,
                           binary_previousblock_hash, timestamp, conn):
    stmt = await conn.prepare("INSERT INTO connector_block (hash, height, previous_hash, timestamp) "
                          "VALUES ($1, $2, $3, $4);")
    await stmt.fetch(binary_block_hash, height, binary_previousblock_hash, int(timestamp, 16))
    app.last_inserted_block = [binary_block_hash, height]


async def update_block_height(app,height, tx_id_list,orphan_block_height,conn):
    stmt = await conn.prepare("""UPDATE connector_transaction 
                                          SET height = $1 
                                          WHERE  id  in (select(unnest($2::BIGINT[])));
                                          """)
    await stmt.fetch(height, tx_id_list)
    if orphan_block_height:
        stmt = await conn.prepare("""SELECT COUNT(id) FROM connector_transaction 
                                          WHERE height = $1;
                                          """)
        cnt=await stmt.fetchval(orphan_block_height)
        if cnt!=0:
            stmt = await conn.prepare("""SELECT hash FROM connector_transaction 
                                                      WHERE height = $1;
                                                      """)
            txs = await stmt.fetch(orphan_block_height)
            for tx in txs:
                app.log.error(app.tx_cache.get(tx['hash']))
                app.log.error(app.pending_cache.get(tx['hash']))
            raise Exception('not delete orphan height')




async def init_db(conn):
    await conn.execute("""
                       CREATE TABLE IF NOT EXISTS connector_block (
                           id BIGSERIAL PRIMARY KEY,
                           height BIGINT DEFAULT NULL,
                           hash bytea NOT NULL,
                           previous_hash bytea,
                           timestamp INT4   DEFAULT 0
                        );
                        """)
    await conn.execute("""
                       CREATE INDEX IF NOT EXISTS connector_block_hash_block ON connector_block USING hash (hash);
                       """)

    await conn.execute("""
                       CREATE TABLE IF NOT EXISTS connector_transaction (
                           id BIGSERIAL PRIMARY KEY,
                           height INT4  DEFAULT NULL,
                           hash bytea NOT NULL,
                           timestamp INT4   DEFAULT 0,
                           last_timestamp INT4   DEFAULT 0,
                           affected BIT(1) DEFAULT 'B0'
                        );
                       """)

    await conn.execute("""
                       CREATE UNIQUE INDEX IF NOT EXISTS connector_transaction_hash_transaction ON connector_transaction(hash);
                       """)
