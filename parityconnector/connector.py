import asyncio
import asyncpg
import traceback
import aiohttp
import json
import aiojsonrpc
import time

from .connector_model import *


class Connector:

    def __init__(self,
                 loop,
                 logger,
                 rpc_url,
                 trace_rpc_url,
                 socket_url,
                 postgresql_dsn,
                 postgresql_pool_max_size=50,
                 start_block=None,
                 tx_handler=None,
                 block_handler=None,
                 block_received_handler=None,
                 orphan_handler=None,
                 expired_tx_handler = None,
                 pending_tx_update_handler=None,
                 rpc_batch_limit=20,
                 rpc_threads_limit=100,
                 rpc_timeout=60,
                 block_timeout=120,
                 clear_history=False,
                 preload=False):
        self.loop = loop
        self.log = logger
        self.rpc_timeout = rpc_timeout
        self.block_timeout = block_timeout
        self.active = True
        self.active_block = asyncio.Future()
        self.active_block.set_result(True)
        self.session = aiohttp.ClientSession()
        self.start_block = start_block
        self.start_tr = True
        self.clear_history=clear_history

        def empty(a=None):
            return None

        self.ws = None
        self.trace_rpc_url = trace_rpc_url
        self.rpc_url = rpc_url
        self.socket_url = socket_url
        self.postgresql_pool_max_size = postgresql_pool_max_size
        self.postgresql_dsn = postgresql_dsn

        self.orphan_handler = orphan_handler
        self.tx_handler = tx_handler
        self.pending_tx_update_handler= pending_tx_update_handler
        self.block_handler = block_handler
        self.block_received_handler = block_received_handler
        self.expired_tx_handler = expired_tx_handler


        self.tx_in_process = set()
        self.block_txs_request = None
        self.sync = 0
        self.sync_requested = False
        self.tx_sub = False
        self.block_sub = False
        self.connected = asyncio.Future()

        self.preload = preload
        self.block_preload = Cache(max_size=250)
        self.tx_cache = Cache(max_size=50000)
        self.pending_cache = Cache(max_size=100000)
        self.block_cache = Cache(max_size=10000)
        self.last_tx_id = 0
        self.last_block_id = 0
        self.last_inserted_block = [0, 0]
        self.last_block_height = None
        self.add_tx_future = dict()
        self.tx_batch_active = False
        self.rpc_batch_limit = rpc_batch_limit
        self.rpc_threads_limit = rpc_threads_limit
        self.await_tx_list = list()
        self.await_tx_id_list=list()
        self._watchdog = False
        self.websocket = False
        self._db_pool = False

        self.log.info("Ethereum node connector started")

        asyncio.ensure_future(self.start(), loop=self.loop)



    async def start(self):
        try:
            conn = await asyncpg.connect(dsn=self.postgresql_dsn)
            await init_db(conn)
            self.last_tx_id = await get_last_tx_id(conn)
            self.last_block_id = await get_last_block_id(conn)
            self.last_block_height = await get_last_block_height(conn)
            await load_tx_cache(self, conn)
            self.log.info('tx cache len %s' %(self.tx_cache.len()))
            await load_pending_cache(self, conn)
            self.log.info('pending cache len %s' %(self.pending_cache.len()))

            await load_block_cache(self, conn)
            await conn.close()
            self._db_pool = await \
                asyncpg.create_pool(dsn=self.postgresql_dsn, loop=self.loop, min_size=1, max_size=self.postgresql_pool_max_size)
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))
            await self.stop()
            return
        self.rpc = aiojsonrpc.rpc(self.rpc_url, self.loop, timeout=self.rpc_timeout)
        if self.trace_rpc_url:
            self.trace_rpc = aiojsonrpc.rpc(self.trace_rpc_url, self.loop, timeout=self.rpc_timeout)
        else:
            self.trace_rpc=None
        self.websocket = self.loop.create_task(self.websocket_client())
        self._watchdog = self.loop.create_task(self.watchdog())
        if self.preload:
            self.loop.create_task(self.preload_block())
        self.loop.create_task(self.get_last_block())


    async def websocket_client(self):
        while True:
            try:
                connector = aiohttp.TCPConnector(verify_ssl=False)
                session = aiohttp.ClientSession(connector=connector)
                self.ws = await session.ws_connect(self.socket_url,
                                                   autoclose=True,
                                                   autoping=True)
                self.connected.set_result(True)
                self.tx_sub = False
                self.block_sub = False
                self.log.info('websocket connected')
                if self.tx_sub: self.unsubscribe_blocks()
                if self.block_sub: self.unsubscribe_transactions()
                self.subscribe_blocks()
                self.subscribe_transactions()
                while True:
                    msg = await self.ws.receive()
                    if msg.tp == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except ValueError:
                            self.log.warning("Can't parse data")
                            continue
                        try:
                            if "id" in data:
                                if data["id"] == 1:
                                    self.block_sub = data["result"]
                                    self.log.info("Blocks subscription %s" % data["result"])
                                elif data["id"] == 2:
                                    self.tx_sub = data["result"]
                                    self.log.info("Transactions subscription %s" % data["result"])
                            if "method" in data:
                                if data["params"]["subscription"] == self.tx_sub:
                                    tx_task = self._new_transaction(data["params"]["result"])
                                    self.loop.create_task(tx_task)
                                if data["params"]["subscription"] == self.block_sub:
                                    block_task = self._new_block(data["params"]["result"])
                                    self.loop.create_task(block_task)
                        except Exception as err:
                            self.log.error("error>: %s" % str(err))
                            self.log.error(traceback.format_exc())
                    elif msg.tp in (aiohttp.WSMsgType.ERROR,
                                    aiohttp.WSMsgType.CLOSE,
                                    aiohttp.WSMsgType.CLOSED):
                        self.log.error("websocket error 1 %s" % msg.tp.name)
                        break
            except asyncio.CancelledError:
                await self.ws.close()
                self.log.info("websocket disconnected")
                self.connected = asyncio.Future()
                break
            except Exception as err:
                self.log.error("websocket error 2 %s" % err)
                self.connected.cancel()
                self.connected = asyncio.Future()
            finally:
                session.close()
            await asyncio.sleep(1)

    async def subscribe_blocks(self):
        await self.ws.send_str('{"jsonrpc":"2.0", "id": 1, "method":"eth_subscribe", "params": ["newHeads"]}')

    async def unsubscribe_blocks(self):
        await self.ws.send_str('{"jsonrpc":"2.0","id": 3, "method":"eth_unsubscribe", "params": ["%s"]}' % self.block_sub)
        self.log.info("Blocks subscription canceled")

    async def subscribe_transactions(self):
        await self.ws.send_str('{"jsonrpc":"2.0","id": 2, "method": "eth_subscribe", "params": ["newPendingTransactions"]}')

    async def unsubscribe_transactions(self):
        await self.ws.send_str('{"jsonrpc":"2.0","id": 4, "method": "eth_unsubscribe", "params": ["%s"]}' % self.tx_sub)
        self.log.info("Transactions subscription canceled")



    async def _new_transaction(self, tx_hash, tx = None,block_height = None):
            binary_tx_hash=unhexlify(tx_hash[2:])
            if tx_hash in self.tx_in_process:
                return
            self.tx_in_process.add(tx_hash)
            try:
                tx_cache = self.tx_cache.get(binary_tx_hash)
                if tx_cache:
                    tx_id,tx_height =tx_cache
                    if tx_height==block_height:
                        # if transaction in block
                        if tx_hash in self.await_tx_list:
                            self.await_tx_list.remove(tx_hash)
                            self.await_tx_id_list.append(tx_id)
                            if not self.await_tx_list:
                                self.block_txs_request.set_result(True)
                    return
                else:
                    tx_id = self.pending_cache.get(binary_tx_hash)
                    if tx_id and not block_height:
                        async with self._db_pool.acquire() as conn:
                            last_seen_timestamp = int(time.time())
                            await update_pending_tx(binary_tx_hash, last_seen_timestamp, conn)
                            if self.pending_tx_update_handler:
                                await self.pending_tx_update_handler(binary_tx_hash, last_seen_timestamp, conn)
                        return
                    if not tx_id:
                        if tx is None:
                            try:
                                tx = await self.rpc.eth_getTransactionByHash(tx_hash)
                                if not(tx_hash == tx["hash"]): raise Exception
                                tx['status'] = 0
                            except:
                                if tx:
                                    raise
                                else:
                                    self.log.debug("None tx data %s" % (tx_hash))
                                    return

                        async with self._db_pool.acquire() as conn:
                            tr = conn.transaction()
                            try:
                                await tr.start()
                                # call external handler
                                handler_result= 0
                                if self.tx_handler:
                                    handler_result = await self.tx_handler(tx, block_height, conn)
                                    if handler_result != 0 and handler_result != 1:
                                        raise Exception('tx handler error')
                                await tr.commit()
                            except:
                                await tr.rollback()
                                raise
                        tx_id = await self.insert_new_tx(binary_tx_hash, handler_result)
                    # check if this tx requested by new_block
                    if tx_hash in self.await_tx_list:
                        self.await_tx_list.remove(tx_hash)
                        self.await_tx_id_list.append(tx_id)
                        if not self.await_tx_list:
                            self.block_txs_request.set_result(True)
                    if block_height:
                        self.tx_cache.set(binary_tx_hash, [tx_id, block_height])
                        self.pending_cache.pop(binary_tx_hash)
                    else:
                        self.pending_cache.set(binary_tx_hash, tx_id)
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("new transaction error %s " % err)
                if tx_hash in self.await_tx_list:
                    self.await_tx_list = []
                    self.await_tx_id_list = []
                    self.block_txs_request.cancel()
            finally:
                self.tx_in_process.remove(tx_hash)

    async def insert_new_tx(self, binary_tx_hash, affected=0):
        self.last_tx_id += 1
        tx_id = self.last_tx_id
        af = BitString('1') if affected else BitString('0')
        self.add_tx_future[binary_tx_hash] = {"insert": asyncio.Future(),
                                      "row": (tx_id, binary_tx_hash, int(time.time()),int(time.time()), af)}
        if not self.tx_batch_active:
            self.loop.create_task(self.tx_batch())
        await  self.add_tx_future[binary_tx_hash]["insert"]
        self.add_tx_future.pop(binary_tx_hash)
        return tx_id

    async def tx_batch(self):
        if self.tx_batch_active:
            return
        self.tx_batch_active = True
        batch, hash_list = [], set()
        try:
            for f in self.add_tx_future:
                if not self.add_tx_future[f]["insert"].done():
                    batch.append(self.add_tx_future[f]["row"])
                    hash_list.add(f)
            if not batch:
                return
            async with self._db_pool.acquire() as conn:
                await insert_new_tx_batch(batch, conn)
            for f in hash_list:
                self.add_tx_future[f]["insert"].set_result(True)
            self.loop.create_task(self.tx_batch())
        except:
            self.log.error(str(traceback.format_exc()))
        finally:
            self.tx_batch_active = False

    async def get_last_block(self):
        try:
            if not self.active:
                return
            data = await self.rpc.eth_blockNumber()
            last_block_node = int(data,16)
            if not self.last_block_height or last_block_node>self.last_block_height:
                block=await self.get_block_by_height(last_block_node)
                if block:
                    self.loop.create_task(self._new_block(block))
        except Exception as err:
            self.log.error(str(traceback.format_exc()))
            self.log.error("Get last block by height")

    async def preload_block(self):
        while True:
            try:
                if self.last_block_height:
                    start_height = self.last_block_height
                    height = start_height + 50
                    data = await self.rpc.eth_blockNumber()
                    last_block_node = int(data, 16)
                    if last_block_node > start_height + 1000:
                        while True:
                            if not self.active:
                                 raise asyncio.CancelledError
                            if start_height + 200 < height:
                                break
                            ex = self.block_preload.get(height)
                            if not ex:
                                self.log.debug('preload block height %s' % height)
                                block=await self.get_block_by_height(height)
                                if block:
                                    self.block_preload.set(height, block)
                                    height += 1
                            else:
                                height += 1
            except asyncio.CancelledError:
                self.log.info("connector preload_block terminated")
                break
            except:
                self.log.error(str(traceback.format_exc()))
            await asyncio.sleep(5)


    async def _new_block(self, block):
        self.log.warning("New block %s hash %s" % (int(block["number"], 16),block["hash"]))
        if not self.active:
            self.log.warning("connector is terminated")
            return
        if not self.active_block.done():
            return
        if block is None:
            self.sync_requested = False
            self.log.info('Block synchronization completed')
            return
        self.active_block = asyncio.Future()
        binary_block_hash = unhexlify(block["hash"][2:])
        binary_previousblock_hash = \
            unhexlify(block["parentHash"][2:]) \
                if "parentHash" in block else None
        bt = time.time()
        block_height = int(block["number"], 16)
        self.log.info('parent hash %s' %block["parentHash"])
        next_block_height=block_height+1
        try:
            block_exist = self.block_cache.get(binary_block_hash)
            if block_exist is not None:
                self.log.info("block already exist in db %s" % block["hash"])
                return
            if binary_previousblock_hash is not None:
                parent_height = self.block_cache.get(binary_previousblock_hash)
            else:
                self.log.debug("parent block hash doesnt exist in db %s" % block["parentHash"])
                parent_height = None
            if self.last_block_height is None:
                if self.start_block is not None and block_height != self.start_block:
                    self.log.info("Start from block %s" % self.start_block)
                    self.sync_requested = True
                    self.sync = block_height
                    next_block_height = self.start_block
                    return
            else:
                if self.last_block_height + 1 > block_height:
                    self.log.info("not mainchain block %s %s" % (block["hash"],block_height))
                    return
                if (self.last_block_height + 1) < block_height:
                     if block_height > self.sync:self.sync=block_height
                     self.sync_requested = True
                     next_block_height = self.last_block_height + 1
                     return
                if self.last_block_height + 1 == block_height:
                    async with self._db_pool.acquire() as conn:
                        if parent_height is None:
                            q = time.time()
                            orphan_block_height=self.last_block_height
                            orphan_binary_block_hash = await block_hash_by_height(self,orphan_block_height, conn)
                            if self.orphan_handler:
                                await self.orphan_handler(orphan_block_height, orphan_binary_block_hash, conn)
                            self.log.info("remove orphan %s" % self.last_block_height)
                            orphan_tx_id_list= await remove_orphan(self,orphan_block_height,orphan_binary_block_hash, conn)
                            await update_block_height(self,None, orphan_tx_id_list,orphan_block_height, conn)
                            if block_height > self.sync: self.sync = block_height
                            self.sync_requested = True
                            self.last_block_height-=1
                            next_block_height = self.last_block_height + 1
                            self.log.info('Orphan handler time [%s]' % round(time.time() - q, 2))
                            return

            if block_height > self.sync:
                self.sync = block_height
                self.sync_requested = False
            async with self._db_pool.acquire() as conn:
                async with conn.transaction():
                    self.await_tx_id_list = []
                    if 'transactions' in block:
                        transactions = block['transactions']
                    else:
                        q = time.time()
                        block = await self.get_block_by_height(block_height)
                        if not block:
                            raise Exception('cant get block by height')
                        transactions = block['transactions']
                        self.log.debug('Get block txs  [%s]' % round(time.time() - q, 2))
                    q = time.time()
                    tx_list, transactions = await self.handle_block_txs(block_height, transactions)
                    block['transactions']=transactions
                    if round(time.time() - q, 1)>2:
                        self.log.warning('Long handle time for block txs [%s]' % round(time.time() - q, 2))
                    if len(tx_list) != len(self.await_tx_id_list):
                        self.log.error('tx list [%s] await tx list [%s] blockheight [%s]' %(len(tx_list),len(self.await_tx_id_list),block_height))
                        raise Exception('missed block transactions')
                    if self.block_received_handler:
                        q = time.time()
                        await self.block_received_handler(block, conn)
                        self.log.info('block_received_handler time [%s]' % round(time.time() - q, 2))
                    # insert new block
                    q = time.time()
                    await insert_new_block(self,binary_block_hash, block_height,
                                           binary_previousblock_hash,block["timestamp"],conn)
                    await update_block_height(self,block_height,self.await_tx_id_list,None, conn)
                    self.block_cache.set(binary_block_hash, block_height)
                    self.log.debug('Insert in db time [%s]' % round(time.time() - q, 2))
                    self.last_block_height=block_height
                # after block added handler
                if self.block_handler:
                    q = time.time()
                    await self.block_handler(block, conn)
                    self.log.info('block_handler time [%s]' % round(time.time() - q, 2))
        except Exception as err:
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            self.log.error("await_tx_list %s" %self.await_tx_list)
            self.sync_requested = False
        finally:
            self.active_block.set_result(True)
            if self.sync_requested:
                self.log.debug("requested %s" % next_block_height)
                self.loop.create_task(self.request_block_by_height(next_block_height))
            self.log.warning("%s block processing time [%s]" % (block_height, round(time.time() - bt, 2)))


    async def handle_block_txs(self, block_height,transactions):
        self.log.info("%s transactions received" % len(transactions))
        if transactions:
            self.block_txs_request = asyncio.Future()
            tx_list = [tx["hash"] for tx in transactions]
            self.await_tx_list = list(tx_list)
            for tx in transactions:
                self.loop.create_task(self._new_transaction(tx["hash"], tx=tx, block_height=block_height))
            await asyncio.wait_for(self.block_txs_request, timeout=self.block_timeout)
            return tx_list, transactions
        else:
            return [], []


    async def request_block_by_height(self,next_block_height):
        if not self.active:
            return
        block_preload_cache = self.block_preload.get(next_block_height)
        if block_preload_cache:
            self.block_preload.pop(next_block_height)
            self.log.debug('get block %s from cashe' %next_block_height)
            self.loop.create_task(self._new_block(block_preload_cache))
        else:
            block=await self.get_block_by_height(next_block_height)
            if block:
                self.loop.create_task(self._new_block(block))
                self.log.debug('get block %s from node' % next_block_height)


    async def get_block_by_height(self, block_height):
        try:
            block = await self.rpc.eth_getBlockByNumber(hex(block_height), True)
            if block is None:
                await asyncio.sleep(1)
            else:
                if not(block["number"] == hex(block_height)): raise Exception
                transactions=await self.get_block_trace_and_receipt(block_height,block["hash"],block["transactions"])
                uncles_data = []
                if block["uncles"]:
                    for index in range(len(block["uncles"])):
                        u_data = await self.get_block_uncles(block["hash"],index)
                        uncles_data.append(u_data)
                block['transactions']=transactions
                block['uncles_data']=uncles_data
                return block
        except Exception:
            self.log.error(str(traceback.format_exc()))
            self.log.error("Get block by height %s failed" % block_height)
            return None

    async def get_block_uncles(self,block_hash,index):
        try:
            uncle = await self.rpc.eth_getUncleByBlockHashAndIndex(block_hash,hex(index))
            if uncle is None:
                await asyncio.sleep(1)
            else:
                return uncle
        except Exception:
            self.log.error(str(traceback.format_exc()))
            self.log.error("Get uncle %s failed" % block_hash)
            return None


    async def get_block_trace_and_receipt(self, block_height,block_hash,transactions):
        if transactions:
            internal_tx = {}
            if self.trace_rpc:
                q = time.time()
                block_trace = await self.trace_rpc.trace_block(hex(block_height))
                if not (block_trace[0]['blockHash'] == block_hash):
                    raise Exception ('block trace hash %s block hash %s' %(block_trace[0]['blockHash'],block_hash))
                trace = {}

                for tx in block_trace:
                    if not tx['transactionHash'] in trace:
                        trace[tx['transactionHash']] = list()
                    trace[tx['transactionHash']].append(tx)
                for key in trace:
                    if len(trace[key]) > 1:
                        internal_tx[key] = trace[key][1:]
            receipt = {}
            q = time.time()
            block_receipt = await self.rpc.parity_getBlockReceipts(hex(block_height))
            #self.log.debug('Get block receipts  [%s]' % round(time.time() - q, 2))
            if not (block_receipt[0]['blockHash'] == block_hash):
                raise Exception('block receipt hash %s block hash %s' % (block_receipt[0]['blockHash'], block_hash))
            for tx in block_receipt:
                if not tx['transactionHash'] in receipt:
                    receipt[tx['transactionHash']] = {}
                receipt[tx['transactionHash']]['status'] = tx['status']
                receipt[tx['transactionHash']]['logs'] = tx['logs']
                receipt[tx['transactionHash']]['gasUsed']=tx['gasUsed']
            for tx in transactions:
                if tx['hash'] in internal_tx:
                    tx['internal'] = internal_tx[tx['hash']]
                if receipt[tx['hash']]['status'] == '0x0':
                    tx['status'] = 2
                else:
                    tx['status'] = 1
                tx['logs'] = receipt[tx['hash']]['logs']
                tx['gasUsed'] = receipt[tx['hash']]['gasUsed']
        return transactions


    async def stop(self):
        self.active = False
        self.log.warning("Stopping connector")
        self.log.warning("Kill watchdog")
        if self._watchdog:
            self._watchdog.cancel()
        self.log.warning("Unsubscribe blocks")
        if self.block_sub:
           self.unsubscribe_blocks()
           self.log.warning("Unsubscribe transactions")
        if self.tx_sub:
            self.unsubscribe_transactions()
        self.log.warning("New block processing restricted")
        if not self.active_block.done():
            self.log.warning("Waiting active block task")
            await self.active_block
        self.session.close()
        self.log.warning("Close websocket")
        if self.websocket:
            self.websocket.cancel()
            await asyncio.wait_for(self.websocket, None, loop=self.loop)
        self.log.warning("Close db pool")
        if self._db_pool:
            await self._db_pool.close()
        self.log.debug('Connector ready to shutdown')

    async def watchdog(self):
        """
        Вывод статистики
        Проверка обновлений блоков
        Очистка не нужных транзакций
        очистка транзакций в блоках
        очистка пула
        """
        while True:
            conn = False
            try:
                self.log.info("watchdog started")
                while True:
                    await asyncio.sleep(60)
                    try:
                        conn = await asyncpg.connect(dsn=self.postgresql_dsn)
                        count = await unconfirmed_count(conn)
                        binary_last_block_hash = await get_last_block_hash(conn)
                        height = await block_height_by_hash(self,binary_last_block_hash, conn)
                        self.log.info("unconfirmed tx %s last block %s" %(count, height))
                        self.log.info('tx cache len %s' % (self.tx_cache.len()))
                        self.log.info('pending cache len %s' % (self.pending_cache.len()))
                        async with conn.transaction():
                            expired_list=await clear_expired_tx(self,conn)
                            if expired_list:
                                if self.expired_tx_handler:
                                    await self.expired_tx_handler(expired_list, conn)
                        if self.clear_history:
                            await clear_old_tx(self, conn, block_exp=5170)

                    finally:
                        if conn:await conn.close()
                    await self.get_last_block()
            except asyncio.CancelledError:
                self.log.debug("watchdog terminated")
                break
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("watchdog error %s " % err)
