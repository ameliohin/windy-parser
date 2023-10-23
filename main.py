
import asyncio
import functools
import signal
from time import time

from config import SLEEPTIME
from downloader import Downloader
from grib import GribReader
from logger import logger
from wgf import WGFWRiter


async def main():

    downloader = Downloader()
    grib_reader = GribReader()
    wgf_writer = WGFWRiter()

    while True:
        start = time()

        # DOWNLOAD if needed
        logger.info("Checking for new epoch and getting the data...")
        filelist, epoch = await downloader.check_and_download()

        if filelist:
            # GET DATA
            logger.info("Extracting the data...")
            data = await grib_reader.get_data(filelist)

            # WRITE DATA
            logger.info("Writing wgf4 data...")
            await wgf_writer.write_wgf(data, epoch)
        else:
            logger.info("No new data...")

        # SLEEP
        elapsed = time() - start
        if elapsed < SLEEPTIME:
            logger.info("sleeping...")
            await asyncio.sleep(SLEEPTIME - elapsed)

def exit(signame, loop, task):

    logger.info("Received SIGTERM. Shutting down.")
    tasks = asyncio.Task.all_tasks()
    for task in tasks:
        task.cancel()


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    for signame in ('SIGTERM',):
        loop.add_signal_handler(getattr(signal, signame), functools.partial(exit, signame, loop, task))

    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt as e:
        tasks = asyncio.Task.all_tasks()
        for task in tasks:
            task.cancel()
    except asyncio.CancelledError as e:
        # From the SIGTERM handler.
        pass
    finally:
        loop.close()