import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import asyncio
import logging
import colorlog
import sys
import configparser
import parityconnector
import signal
import traceback


class App:
    def __init__(self, loop, logger, config):
        self.loop = loop
        self.log = logger
        self.connector = False
        self.console = False
        self.rpc_url = config["NODE_URL"]["url"]
        self.trace_rpc_url = config["NODE_URL"]["url"]
        self.socket_url = config["NODE_SOCKET"]["url"]
        self.postgresql_dsn =config["POSTGRESQL"]["dsn"].replace(',',' ')
        self.log.info("Connector tester started")
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        asyncio.ensure_future(self.start(), loop=self.loop)


    async def start(self):
        # init database
        try:
            self.connector = parityconnector.Connector(
                loop, self.log,
                self.rpc_url,self.trace_rpc_url,
                self.socket_url,
                self.postgresql_dsn,
                tx_handler=self.new_transaction_handler,
                orphan_handler=self.orphan_block_handler,
                block_handler=self.new_block_handler,preload=True, start_block=0)
            await self.connector.connected
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))


    async def orphan_block_handler(self, orphan_height, orphan_hash):
        pass


    async def new_block_handler(self, block):
        pass


    async def new_transaction_handler(self, data,block_height, block_time):
        return 1

    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())


    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.info('Stop request received')
        if self.connector:
            await self.connector.stop()
        self.loop.stop()
        await asyncio.sleep(1)
        self.loop.close()

def init(loop, argv):
    config_file = "tester.cnf"
    log_file = "tester.log"
    log_level = logging.WARNING
    logger = colorlog.getLogger('test')
    config = configparser.ConfigParser()
    config.read(config_file)
    if log_level == logging.WARNING and "LOG" in config.sections():
        if "log_level" in config['LOG']:
            if config['LOG']["log_level"] == "info":
                log_level = logging.INFO
            elif config['LOG']["log_level"] == "debug":
                log_level = logging.DEBUG
    logger.setLevel(log_level)
    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Start")
    loop = asyncio.get_event_loop()

    app = App(loop, logger, config)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = init(loop, sys.argv[1:])
    loop.run_forever()
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
    loop.close()