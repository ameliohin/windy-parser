import aiohttp
import aiofiles
import asyncio
import os
from bs4 import BeautifulSoup as bs
from config import WEATHER_DATA_FOLDER_URL, WEATHER_DATA_MODEL_PREFIX,\
                   TMP_FOLDER


class Downloader():

    folder_url = WEATHER_DATA_FOLDER_URL
    model_prefix = WEATHER_DATA_MODEL_PREFIX
    tmp_folder = TMP_FOLDER

    epoches = {}
    last_epoch = None
    need_download = False

    def __init__(self,
                 folder_url: str = None,
                 model_prefix: str = None,
                 model_name: str = None,
                 tmp_folder: str = None) -> None:
        if folder_url:
            self.folder_url = folder_url
        if model_prefix:
            self.model_prefix = model_prefix
        if model_name:
            self.model_name = model_name
        if tmp_folder:
            self.tmp_folder = tmp_folder

    async def check_for_new_epoch(self):
        async with aiohttp.ClientSession() as session:
            r = await session.get(self.folder_url, ssl=False)
            soup = bs(await r.text(), 'html.parser')

            filelist = []
            epoch = None

            for a in soup.find_all("a"):
                filename = a.attrs['href']

                if filename.startswith(self.model_prefix):
                    filedata = filename[len(self.model_prefix):].split('.')
                    fileparams = filedata[0].split('_')[:2]
                    if not epoch:
                        epoch = fileparams[0]
                    else:
                        if epoch != fileparams[0]:
                            raise Exception('More than one epoch in set!')
                    filelist.append({
                        'filename': filename,
                        'time_shift': int(fileparams[1])
                    })

            if epoch not in self.epoches.keys():
                self.epoches[epoch] = {
                    'filelist': filelist,
                    'downloaded': False
                }
                self.last_epoch = epoch
                self.need_download = True
            else:
                # Maybe we need initiate some additional imports
                # in case last epoch was not fully scanned and imported.
                # Not implemented for now.
                pass

            print(self.epoches)

    def need_download(self):
        if self.need_download:
            return self.last_epoch

    async def download_file(self, session: aiohttp.ClientSession, filename) -> None:
        r = await session.get(self.folder_url + filename)
        async with aiofiles.open(os.path.join(TMP_FOLDER, filename), 'wb') as f:
            async for data in r.content.iter_any():
                await f.write(data)

    async def download(self, epoch=None):
        if not epoch:
            epoch = self.last_epoch
        if not self.epoches[epoch]['downloaded']:
            filelist = self.epoches[epoch]['filelist']

            async with aiohttp.ClientSession() as session:
                await asyncio.gather(*[self.download_file(session, file['filename']) for file in filelist])

            if epoch == self.last_epoch:
                self.need_download = False
            self.epoches[epoch]['downloaded'] = True

            return filelist

    async def check_and_download(self):
        await self.check_for_new_epoch()
        filelist = await self.download()
        return filelist, self.last_epoch


async def main():
    d = Downloader()
    await d.check_and_download()

        
if __name__ == '__main__':
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())