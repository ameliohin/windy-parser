import aiofiles
import asyncio
import numpy as np
import struct
import os
from datetime import datetime, timedelta
from logger import logger

from config import OUT_FOLDER, WGF_NAME


class WGFFile():

    source_filename = None
    filedata = None
    out_filename = None
    epoch = None
    

    def get_folder_name(self):
        epoch_begin = datetime.strptime(self.epoch, "%Y%m%d%H")
        time_shift = timedelta(hours=self.filedata['time_shift'])
        file_time = epoch_begin + time_shift
        ts_str = str(int(round(file_time.timestamp())))
        
        folder_name = file_time.strftime('%d.%m.%Y_%H:%M_') + ts_str
        return folder_name
        
    def __init__(self, source_filename: str, filedata, epoch) -> None:
        self.source_filename = source_filename
        self.filedata = filedata
        self.filedata['data'].set_fill_value(-100500.0)
        self.epoch = epoch
        

    def prepare_header(self):
        return struct.pack('7if',
                           self.filedata['lat_min'],
                           self.filedata['lat_max'],
                           self.filedata['lon_min'],
                           self.filedata['lon_max'],
                           self.filedata['lat_step'],
                           self.filedata['lon_step'],
                           self.filedata['multiplier'],
                           -100500.0
        )
        
    async def write_wgf_file(self):
        header = self.prepare_header()

        # We need to convert np.float64 to float
        data_64 = np.ma.getdata(self.filedata['data'])
        data_32 = data_64.astype(np.float32)
        databytes = data_32.tobytes()

        folder_name = os.path.join(OUT_FOLDER, self.get_folder_name())

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        filename = os.path.join(folder_name, WGF_NAME)
        
        async with aiofiles.open(filename, mode='wb') as f:
            await f.write(header)
            await f.write(databytes)
            await f.flush()


class WGFWRiter():

    out_folder = OUT_FOLDER

    def __init__(self, out_folder: str = None) -> None:
        if out_folder:
            self.out_folder = out_folder

    async def write_wgf_file(self, filename, filedata, epoch):
        wgf = WGFFile(filename, filedata, epoch)
        await wgf.write_wgf_file()

    async def write_wgf(self, data, epoch):
        await asyncio.gather(*[self.write_wgf_file(filename, 
                                                   filedata,
                                                   epoch) for filename, filedata in data.items()])
