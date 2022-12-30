import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import asyncio
import logging
import colorlog
import sys
import configparser
import signal
import traceback
import ethereumconnector


class App:
    def __init__(self, loop, logger, config):
        self.loop = loop
        self.log = logger
        self.connector = False
        self.client = config["NODE"]["client"]
        self.rpc_url = config["NODE"]["rpc_url"]
        self.socket_url = config["NODE"]["ws_url"]
        self.postgresql_dsn = config["POSTGRESQL"]["dsn"].replace(',',' ')
        self.log.info("Connector tester started")
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        asyncio.ensure_future(self.start(), loop=self.loop)


    async def start(self):
        try:
            self.connector = ethereumconnector.Connector(
                loop, self.log,
                self.rpc_url,
                self.socket_url,
                self.client,
                trace=True,
                connector_db=True,  # use connector database
                postgresql_dsn=self.postgresql_dsn,
                start_block=0,  # start block height sync, if not specified - use current block
                tx_handler=self.new_transaction_handler,
                block_handler=self.new_block_handler,
                orphan_handler=self.orphan_block_handler,
                preload=True)
            await self.connector.connected
        except Exception as err:
            self.log.error("Start failed")
            self.loop.create_task(self.terminate_coroutine())



    async def orphan_block_handler(self, orphan_block_height, orphan_bin_block_hash, **kwargs):
        pass


    async def new_block_handler(self, block, **kwargs):
        pass


    async def new_transaction_handler(self, tx, **kwargs):
        return 1

    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())


    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        if self.connector.active:
            await self.connector.stop()
        self.loop.stop()
        await asyncio.sleep(1)
        self.loop.close()


def init(loop):
    config_file = "test.cnf"
    config = configparser.ConfigParser()
    config.read(config_file)
    logger = colorlog.getLogger('test')
    log_level = logging.WARNING
    try:
        if config['LOG']["log_level"] == "debug": log_level = logging.DEBUG
        if config['LOG']["log_level"] == "info":log_level = logging.INFO
        if config['LOG']["log_level"] == "warning": log_level = logging.WARNING
        if config['LOG']["log_level"] == "error": log_level = logging.ERROR
    except:
        pass
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info("Start")
    app = App(loop, logger, config)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = init(loop)
    loop.run_forever()
    pending = asyncio.all_tasks()
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.wait(pending))
    loop.close()