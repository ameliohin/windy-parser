import asyncio
import bz2
import pygrib
import numpy as np
import os
import shutil
from config import MULTIPLIER, TMP_FOLDER


class GribFile:
    data = None
    lat_max = None
    lat_min = None
    lon_max = None
    lon_min = None
    lat_step = None
    lon_step = None
    multiplier = MULTIPLIER
    filename = None

    def set_latlon_params(self, lats, lons):

        lat_dim, lon_dim = lats.shape
        
        self.lat_min = int(np.round(lats[0,0]*self.multiplier))
        self.lat_max = int(np.round(lats[lat_dim-1, lon_dim-1]*self.multiplier))

        self.lon_min = int(np.round(lons[0,0]*self.multiplier))
        self.lon_max = int(np.round(lons[lat_dim-1, lon_dim-1]*self.multiplier))

        self.lat_step = int((self.lat_max - self.lat_min)/(lat_dim - 1))
        self.lon_step = int((self.lon_max - self.lon_min)/(lon_dim - 1))

    def __init__(self, filename: str, multiplier=None) -> None:
        self.filename = filename
        if multiplier:
            self.multiplier = multiplier
        grbs = pygrib.open(filename)
        grb = grbs.select(name='Total Precipitation')[0]

        self.set_latlon_params(*grb.latlons())
        self.data = grb.values.flatten()

    def prepare_data(self, prev_data=None):
        if prev_data is not None:
            data = np.subtract(self.data, prev_data)
        else:
            data = self.data
        return {
            'data': data,
            'original_data': self.data,
            'lat_max': self.lat_max,
            'lat_min': self.lat_min,
            'lon_max': self.lon_max,
            'lon_min': self.lon_min,
            'lat_step': self.lat_step,
            'lon_step': self.lon_step,
            'multiplier': self.multiplier
        }


class GribReader():
    tmp_folder = TMP_FOLDER
    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder)

    def __init__(self, tmp_folder: str = None) -> None:
        if tmp_folder:
            self.tmp_folder = tmp_folder

    def prepare_grb(self, filename):
        filename = os.path.join(TMP_FOLDER, filename)
        gribfile = filename[:-4]
        # we're using such approach because of lack of information 
        # about how can pygrib work with bytes data
        with bz2.BZ2File(filename) as fr, open(gribfile, 'wb') as fw:
            shutil.copyfileobj(fr, fw)
        grb = GribFile(gribfile)
        return grb
    
    async def get_file_data(self, filename, time_shift):
        loop = asyncio.get_running_loop()
        grb = await loop.run_in_executor(None, self.prepare_grb, filename)
        
        return {
            'filename': filename,
            'time_shift': time_shift,
            'grb': grb
        }
        
    async def get_data(self, filelist):

        grbs = await asyncio.gather(*[self.get_file_data(file['filename'],
                                                         file['time_shift']) for file in filelist])
        
        sorted_grbs = sorted(grbs, key=lambda k: k['filename'])

        prev_data = None
        data = {}
        for grb_item in sorted_grbs:
            filename = grb_item['filename']
            grb = grb_item['grb']

            data[filename] = grb.prepare_data(prev_data)
            data[filename]['time_shift'] = grb_item['time_shift']

            prev_data = data[filename]['original_data']

        return data


if __name__ == '__main__':
    file_parser = GribFile('sample_files/icon-d2_germany_regular-lat-lon_single-level_2023101912_001_2d_tot_prec.grib2')
    print(file_parser.prepare_data())
