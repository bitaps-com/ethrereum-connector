import asyncio
from .utils import *
import traceback

async def health_check(app):
    try:
        return await app.rpc.eth_syncing()
    except Exception:
        app.log.error("Health check failed")
        raise

async def get_last_block(app):
    try:
        return await app.rpc.eth_blockNumber()
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get last_block failed")
        raise

async def get_transaction(app, tx_hash):
    try:
        return await app.rpc.eth_getTransactionByHash(tx_hash)
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get transaction %s failed" % tx_hash)
        raise

async def get_block_uncles(app, block_hash, index):
    try:
        return await app.rpc.eth_getUncleByBlockHashAndIndex(block_hash, hex(index))
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get uncle %s failed" % block_hash)
        raise

async def get_block_by_height(app, block_height):
    try:
        block = await app.rpc.eth_getBlockByNumber(hex(block_height), True)
        if block is None:
            await asyncio.sleep(1)
        else:
            if not(block["number"] == hex(block_height)): raise Exception
            await get_block_trace_and_receipt(app,block_height, block["hash"], block["transactions"])
            uncles_data = []
            if block["uncles"]:
                for index in range(len(block["uncles"])):
                    u_data = await get_block_uncles(app,block["hash"], index)
                    uncles_data.append(u_data)
            block['uncles_data'] = uncles_data
            block['details'] = True
            return block
    except Exception:
        app.log.error(str(traceback.format_exc()))
        app.log.error("Get block by height %s failed" % block_height)
        raise

async def get_block_trace_and_receipt(app, block_height, block_hash, transactions):
    if transactions:
        trace_tx = {}
        if app.trace:
            block_trace = await app.rpc.trace_block(hex(block_height))
            if not (block_trace[0]['blockHash'] == block_hash):
                raise Exception ('block trace hash %s block hash %s' %(block_trace[0]['blockHash'],block_hash))
            for tx in block_trace:
                if 'author' in tx['action']:continue
                if not tx['transactionHash'] in trace_tx:
                    trace_tx[tx['transactionHash']] = list()
                trace_tx[tx['transactionHash']].append(tx)
        receipt = {}
        if CLIENTS[app.client]["getBlockReceipts_method"]:
            func_name = CLIENTS[app.client]["getBlockReceipts_method"]
            func = getattr(app.rpc, func_name)
            block_receipt = await func(hex(block_height))
            if not (block_receipt[0]['blockHash'] == block_hash):
                raise Exception('block receipt hash %s block hash %s' % (block_receipt[0]['blockHash'], block_hash))
        else:
            block_receipt=[]
            for tx in transactions:
                tx_receipt = await app.rpc.eth_getTransactionReceipt(tx["hash"])
                block_receipt.append(tx_receipt)
        for tx in block_receipt:
            if not tx['transactionHash'] in receipt:
                receipt[tx['transactionHash']] = {}
            if 'status' in tx:
                receipt[tx['transactionHash']]['status'] = tx['status']
            else:
                receipt[tx['transactionHash']]['status'] = '0x1'
            receipt[tx['transactionHash']]['logs'] = tx['logs']
            receipt[tx['transactionHash']]['gasUsed']=tx['gasUsed']
            receipt[tx['transactionHash']]['effectiveGasPrice']=tx['effectiveGasPrice']
        for tx in transactions:
            if receipt[tx['hash']]['status'] == '0x0':
                tx['status'] = 0
            else:
                tx['status'] = 1
            tx['logs'] = receipt[tx['hash']]['logs']
            tx['gasUsed'] = receipt[tx['hash']]['gasUsed']
            tx['effectiveGasPrice'] = receipt[tx['hash']]['effectiveGasPrice']
            if tx['hash'] in trace_tx:
                if 'result' in tx:
                    tx['result'] = trace_tx[tx['hash']][0]['result']
                else:
                    tx['result']=None
                tx['trace'] = trace_tx[tx['hash']]
                if 'error' in trace_tx[tx['hash']][0]:
                    tx['status'] = 0




