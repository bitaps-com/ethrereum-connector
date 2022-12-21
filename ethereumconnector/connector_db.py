import asyncio
from binascii import hexlify, unhexlify
import asyncpg
from .utils import *
import traceback

async def init_db(app,conn):
    level = await conn.fetchval("SHOW TRANSACTION ISOLATION LEVEL;")
    if level != "repeatable read":
        app.log.warning("RECOMMENDED Postgres isolation level is REPEATABLE READ! Current isolation level is %s" % level.upper())
    await conn.execute("""
                       CREATE TABLE IF NOT EXISTS connector_block (
                           height BIGINT DEFAULT NULL,
                           hash bytea NOT NULL PRIMARY KEY,
                           previous_hash bytea,
                           timestamp INT4   DEFAULT 0
                        );
                        """)

    await conn.execute("""
                       CREATE INDEX IF NOT EXISTS connector_block_height ON connector_block USING BTREE (height desc);
                       """)

    await conn.execute("""
                       CREATE TABLE IF NOT EXISTS connector_transaction (
                           height INT4  DEFAULT NULL,
                           hash bytea NOT NULL  PRIMARY KEY ,
                           timestamp INT4   DEFAULT 0,
                           last_timestamp INT4   DEFAULT 0,
                           affected BIT(1) DEFAULT 'B0'
                        );
                       """)

    await conn.execute("""
                       CREATE INDEX IF NOT EXISTS connector_transaction_height ON connector_transaction USING BTREE (height desc);
                       """)

async def create_pool(app):
    app.db_pool = await asyncpg.create_pool(dsn=app.postgresql_dsn, loop=app.loop,
                                             min_size=1, max_size=app.postgresql_pool_max_size)

async def ping(app):
    async with app.db_pool.acquire() as conn:
        await conn.fetchval("SELECT 1")

async def cache_load_handler(app, conn):
    stmt = await conn.prepare("SELECT hash, height, last_timestamp FROM connector_transaction WHERE height IS NOT NULL ORDER BY height DESC, last_timestamp DESC LIMIT $1;")
    rows = await stmt.fetch(DEFAULT_CONFIRMED_TX_CACHE_SIZE)
    rows = sorted(rows, key=lambda i: i["height"])
    [app.confirmed_tx_cache.set(row["hash"], (row["height"], row["last_timestamp"])) for row in rows]
    stmt = await conn.prepare("SELECT hash, height, last_timestamp FROM connector_transaction WHERE height IS NULL ORDER BY last_timestamp DESC;")
    rows = await stmt.fetch()
    rows = sorted(rows, key=lambda i: i["last_timestamp"])
    [app.pending_tx_cache.set(row["hash"],  (-1, row["last_timestamp"])) for row in rows]
    stmt = await conn.prepare("SELECT hash, height FROM connector_block ORDER BY height DESC LIMIT $1;")
    rows = await stmt.fetch(DEFAULT_BLOCK_CACHE_SIZE)
    rows = sorted(rows, key=lambda i: i["height"])
    [app.block_cache.set(row["hash"], row["height"]) for row in rows]

async def block_handler(block, conn):
    stmt = await conn.prepare("INSERT INTO connector_block (hash, height, previous_hash, timestamp) VALUES ($1, $2, $3, $4);")
    await stmt.fetch(unhexlify(block["hash"][2:]), int(block["number"], 16), unhexlify(block["parentHash"][2:]) if "parentHash" in block else None, int(block['timestamp'], 16))
    tx_hash_list = [unhexlify(tx["hash"][2:]) for tx in block["transactions"]]
    stmt = await conn.prepare("UPDATE connector_transaction SET height = $1, timestamp=$2  WHERE hash = ANY ($3);")
    await stmt.fetch(int(block["number"], 16), int(block['timestamp'], 16), tx_hash_list)

async def tx_handler(tx, conn):
    stmt = await conn.prepare("INSERT INTO connector_transaction (hash, timestamp, last_timestamp, affected) VALUES ($1, $2, $3, $4);")
    await stmt.fetch(unhexlify(tx["hash"][2:]), tx["timestamp"], tx["timestamp"], asyncpg.BitString('1') if tx["handler_result"] else asyncpg.BitString('0'))

async def pending_tx_update_handler(bin_tx_hash,last_seen_timestamp,conn):
    stmt = await conn.prepare("UPDATE connector_transaction SET last_timestamp = $1  WHERE hash=$2;")
    await stmt.fetch(last_seen_timestamp,bin_tx_hash)

async def pending_tx_expire_handler(expired_hash_list, conn):
    stmt = await conn.prepare("DELETE FROM connector_transaction WHERE hash = ANY ($1)")
    await stmt.fetch(expired_hash_list)

async def orphan_handler(orphan_block_height,orphan_bin_block_hash, conn):
    stmt = await conn.prepare("DELETE FROM connector_block WHERE hash = $1;")
    await stmt.fetch(orphan_bin_block_hash)
    stmt = await conn.prepare("UPDATE connector_transaction SET height = NULL WHERE height=$1;")
    await stmt.fetch(orphan_block_height)


