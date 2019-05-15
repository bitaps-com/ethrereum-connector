import asyncio
import traceback
import aiohttp
import json
import aiojsonrpc
import time
from binascii import hexlify, unhexlify

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
                 tx_cache = Cache(max_size=50000),
                 pending_cache=Cache(max_size=100000),
                 block_cache = Cache(max_size=10000),
                 cache_update=None,
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

        def empty(a=None):
            return None

        self.ws = None
        self.trace_rpc_url = trace_rpc_url
        self.rpc_url = rpc_url
        self.socket_url = socket_url
        self.postgresql_pool_max_size = postgresql_pool_max_size
        self.postgresql_dsn = postgresql_dsn

        self.cache_update=cache_update
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
        self.block_preload = Cache(max_size=50000)
        self.tx_cache = tx_cache
        self.pending_cache = pending_cache
        self.block_cache = block_cache
        self.last_inserted_block = [0, 0]
        self.last_block_height = None
        self.add_tx_future = dict()
        self.tx_batch_active = False
        self.rpc_batch_limit = rpc_batch_limit
        self.rpc_threads_limit = rpc_threads_limit
        self.await_tx_list = list()
        self.await_tx_list_check=list()
        self._watchdog = False
        self.websocket = False
        self._db_pool = False

        self.log.info("Ethereum node connector started")

        asyncio.ensure_future(self.start(), loop=self.loop)



    async def start(self):
        try:
            self.last_block_height = get_last_block_height(self)
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
        await asyncio.sleep(0.1)
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
                if self.tx_sub: await self.unsubscribe_blocks()
                if self.block_sub: await self.unsubscribe_transactions()
                while True:
                    msg = await self.ws.receive()
                    if msg.type == aiohttp.WSMsgType.TEXT:
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
                    elif msg.type in (aiohttp.WSMsgType.ERROR,
                                    aiohttp.WSMsgType.CLOSE,
                                    aiohttp.WSMsgType.CLOSED):
                        self.log.error("websocket error 1 %s" % msg.type.name)
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
                await session.close()
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



    async def _new_transaction(self, tx_hash, tx = None,block_height = -1,block_time = None):
            binary_tx_hash=unhexlify(tx_hash[2:])
            if tx_hash in self.tx_in_process:
                return
            self.tx_in_process.add(tx_hash)
            try:
                tx_cache = self.tx_cache.get(binary_tx_hash)
                if tx_cache:
                    tx_height,last_timestamp =tx_cache
                    if tx_height==block_height:
                        # if transaction in block
                        if tx_hash in self.await_tx_list:
                            self.await_tx_list.remove(tx_hash)
                            self.await_tx_list_check.append(tx_hash)
                            if not self.await_tx_list:
                                self.block_txs_request.set_result(True)
                    return
                else:
                    tx_cache = self.pending_cache.get(binary_tx_hash)
                    if tx_cache:
                        if block_height==-1:
                            last_seen_timestamp = int(time.time())
                            upd_tx_cache=(-1, last_seen_timestamp)
                            self.pending_cache.set(binary_tx_hash,upd_tx_cache)
                            if self.pending_tx_update_handler:
                                await self.pending_tx_update_handler(binary_tx_hash, last_seen_timestamp)
                            return
                    else:
                        if tx is None:
                            try:
                                tx = await self.rpc.eth_getTransactionByHash(tx_hash)
                                if not(tx_hash == tx["hash"]): raise Exception
                            except:
                                if tx:
                                    raise
                                else:
                                    self.log.debug("None tx data %s" % (tx_hash))
                                    return

                        # call external handler
                        if self.tx_handler:
                            handler_result=await self.tx_handler(tx,block_height, block_time)
                            if handler_result != 0 and handler_result != 1:
                                raise Exception('tx handler error')
                    if block_height==-1:
                        tx_cache = (block_height, int(time.time()))
                        self.pending_cache.set(binary_tx_hash, tx_cache)
                    else:
                        tx_cache=(block_height,block_time)
                        self.tx_cache.set(binary_tx_hash, tx_cache)
                        self.pending_cache.pop(binary_tx_hash)
                        if tx_hash in self.await_tx_list:
                            # check if this tx requested by new_block
                            self.await_tx_list.remove(tx_hash)
                            self.await_tx_list_check.append(tx_hash)
                            if not self.await_tx_list:
                                self.block_txs_request.set_result(True)
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("new transaction error %s " % err)
                if tx_hash in self.await_tx_list:
                    self.await_tx_list = []
                    self.await_tx_list_check = []
                    self.block_txs_request.cancel()
            finally:
                self.tx_in_process.remove(tx_hash)


    async def get_last_block(self):
        try:
            if not self.active:
                return
            data = await self.rpc.eth_blockNumber()
            last_block_node = int(data,16)
            if not self.last_block_height or last_block_node > self.last_block_height + 1000:
                try:
                    if self.block_sub:
                        await self.unsubscribe_blocks()
                    if self.tx_sub:
                        await self.unsubscribe_transactions()
                except:
                    self.log.error(str(traceback.format_exc()))
                    self.log.error("ws unsubscribe error")

            else:
                try:
                    if not self.block_sub:
                        await self.subscribe_blocks()
                    if not self.tx_sub:
                        await self.subscribe_transactions()
                except:
                    self.log.error(str(traceback.format_exc()))
                    self.log.error("ws subscribe error")

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
                    height = start_height + 10000
                    data = await self.rpc.eth_blockNumber()
                    last_block_node = int(data, 16)
                    if last_block_node > start_height + 10000:
                        while True:
                            if not self.active:
                                 raise asyncio.CancelledError
                            if start_height + 15000 < height:
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
            await asyncio.sleep(0.5)


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
        binary_previousblock_hash = unhexlify(block["parentHash"][2:]) if "parentHash" in block else None
        bt = time.time()
        block_height = int(block["number"], 16)
        block_time=int(block['timestamp'],16)
        self.log.info('parent hash %s' %block["parentHash"])
        next_block_height=block_height+1
        try:
            block_exist = self.block_cache.get(binary_block_hash)
            if block_exist is not None:
                self.log.info("block already exist in db %s" % block["hash"])
                return
            if binary_previousblock_hash is not None:
                parent_exist = self.block_cache.get(binary_previousblock_hash)
            else:
                self.log.debug("parent block hash doesnt exist in db %s" % block["parentHash"])
                parent_exist = None
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
                    if parent_exist is None:
                        q = time.time()
                        orphan_block_height=self.last_block_height
                        # if self.tx_cache.len() < 5000 or self.block_cache.len()<50:
                        #     if self.cache_update:
                        #         self.tx_cache,self.block_cache=await self.cache_update()
                        orphan_binary_block_hash =block_hash_by_height(self,orphan_block_height)
                        if self.orphan_handler:
                            await self.orphan_handler(orphan_block_height, orphan_binary_block_hash)
                        self.log.info("remove orphan %s" % self.last_block_height)
                        remove_orphan(self,orphan_block_height,orphan_binary_block_hash)
                        if block_height > self.sync: self.sync = block_height
                        self.sync_requested = True
                        self.last_block_height-=1
                        next_block_height = self.last_block_height + 1
                        self.log.info('Orphan handler time [%s]' % round(time.time() - q, 4))
                        return
            if block_height > self.sync:
                self.sync = block_height
                self.sync_requested = False
            self.await_tx_list_check = []
            if 'transactions' in block:
                transactions = block['transactions']
            else:
                q = time.time()
                block = await self.get_block_by_height(block_height)
                if not block:
                    raise Exception('cant get block by height')
                transactions = block['transactions']
                self.log.debug('Get block txs  [%s]' % round(time.time() - q, 4))
            q = time.time()
            tx_list, transactions = await self.handle_block_txs(block_height, block_time, transactions)
            block['transactions']=transactions
            if transactions:
                self.log.info('block_txs_handler time [%s]' % round(time.time() - q, 4))
            if len(tx_list) != len(self.await_tx_list_check):
                self.log.error('tx list [%s] await tx list [%s] blockheight [%s]' % (len(tx_list), len(self.await_tx_list_check), block_height))
                raise Exception('missed block transactions')
            if self.block_received_handler:
                q = time.time()
                await self.block_received_handler(block)
                self.log.info('block_received_handler time [%s]' % round(time.time() - q, 4))
            self.last_inserted_block = [binary_block_hash, block_height]
            self.block_cache.set(binary_block_hash, block_height)
            self.last_block_height=block_height
            # after block added handler
            if self.block_handler:
                q = time.time()
                await self.block_handler(block)
                self.log.info('block_handler time [%s]' % round(time.time() - q, 4))
        except Exception as err:
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            self.log.error("await_tx_list %s" %self.await_tx_list)
            self.sync_requested = False
        finally:
            self.active_block.set_result(True)
            if self.sync_requested:
                if not block_exist:
                    self.log.debug("requested %s" % next_block_height)
                    self.loop.create_task(self.request_block_by_height(next_block_height))
            self.log.warning("%s block processing time [%s]" % (block_height, round(time.time() - bt, 4)))


    async def handle_block_txs(self, block_height,block_time,transactions):
        self.log.info("%s transactions received" % len(transactions))
        if transactions:
            self.block_txs_request = asyncio.Future()
            tx_list = [tx["hash"] for tx in transactions]
            self.await_tx_list = list(tx_list)
            for tx in transactions:
                self.loop.create_task(self._new_transaction(tx["hash"], tx=tx, block_height=block_height,block_time=block_time))
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
            trace_tx = {}
            if self.trace_rpc:
                q = time.time()
                block_trace = await self.trace_rpc.trace_block(hex(block_height))
                if not (block_trace[0]['blockHash'] == block_hash):
                    raise Exception ('block trace hash %s block hash %s' %(block_trace[0]['blockHash'],block_hash))
                trace = {}

                for tx in block_trace:
                    if not tx['transactionHash'] in trace:
                        trace[tx['transactionHash']] = list()
                    tx_trace=tx
                    del tx_trace['blockHash']
                    del tx_trace['blockNumber']
                    del tx_trace['transactionPosition']
                    trace[tx['transactionHash']].append(tx_trace)
                for key in trace:
                    trace_tx[key] = trace[key]
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
                if receipt[tx['hash']]['status'] == '0x0':
                    tx['status'] = 0
                else:
                    tx['status'] = 1
                tx['logs'] = receipt[tx['hash']]['logs']
                tx['gasUsed'] = receipt[tx['hash']]['gasUsed']
                if tx['hash'] in trace_tx:
                    if 'result' in tx:
                        tx['result'] = trace_tx[tx['hash']][0]['result']
                    else:
                        tx['result']=None
                    tx['trace'] = trace_tx[tx['hash']]
                    if 'error' in trace_tx[tx['hash']][0]:
                        tx['status'] = 0
        return transactions


    async def stop(self):
        self.active = False
        self.log.warning("Stopping connector")
        self.log.warning("Kill watchdog")
        if self._watchdog:
            self._watchdog.cancel()
        self.log.warning("Unsubscribe blocks")
        if self.block_sub:
           await self.unsubscribe_blocks()
           self.log.warning("Unsubscribe transactions")
        if self.tx_sub:
            await self.unsubscribe_transactions()
        self.log.warning("New block processing restricted")
        if not self.active_block.done():
            self.log.warning("Waiting active block task")
            await self.active_block
        await self.session.close()
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
            try:
                self.log.info("watchdog started")
                while True:
                    await asyncio.sleep(60)
                    count=self.pending_cache.len()
                    height = get_last_block_height(self)
                    self.log.info("unconfirmed tx %s last block %s" %(count, height))
                    self.log.info('tx cache len %s' % (self.tx_cache.len()))
                    self.log.info('pending cache len %s' % (self.pending_cache.len()))
                    expired_hash_list=get_expired_tx(self)
                    self.log.info('expired tx len %s' % (len(expired_hash_list)))
                    if expired_hash_list:
                        if self.expired_tx_handler:
                            await self.expired_tx_handler(expired_hash_list)
                        if expired_hash_list:
                            [self.pending_cache.pop(tx_hash) for tx_hash in expired_hash_list]
                    await self.get_last_block()
            except asyncio.CancelledError:
                self.log.debug("watchdog terminated")
                break
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("watchdog error %s " % err)
